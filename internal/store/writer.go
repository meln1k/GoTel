package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/meln1k/gotel/internal/model"
)

var (
	ErrClosed         = errors.New("store is closed")
	ErrOverloaded     = errors.New("outstanding telemetry budget exhausted")
	ErrRecordTooLarge = errors.New("telemetry exceeds admission or single-record batch limits")
	ErrInvalidRecord  = errors.New("invalid telemetry record")
)

// The ring retains its head during transactions and retries. Dequeueing never
// frees admission capacity; only commit or a reported terminal discard does.
type queuedRecord struct {
	span        *model.SpanRecord
	log         *model.LogRecord
	seq         uint64
	bytes, work int
	admitted    time.Time
}

type PersistenceStats struct {
	Ready                  bool      `json:"ready"`
	OutstandingRecords     int       `json:"outstandingRecords"`
	OutstandingBytes       int       `json:"outstandingBytes"`
	PendingRecords         int       `json:"pendingRecords"`
	PendingBytes           int       `json:"pendingBytes"`
	InFlightRecords        int       `json:"inFlightRecords"`
	InFlightBytes          int       `json:"inFlightBytes"`
	OldestOutstandingAgeMs int64     `json:"oldestOutstandingAgeMs"`
	HighWaterRecords       int       `json:"highWaterRecords"`
	HighWaterBytes         int       `json:"highWaterBytes"`
	MaxBatchRecords        int       `json:"maxBatchRecords"`
	MaxBatchBytes          int       `json:"maxBatchBytes"`
	MaxBatchWork           int       `json:"maxBatchWork"`
	LastBatchRecords       int       `json:"lastBatchRecords"`
	LastBatchBytes         int       `json:"lastBatchBytes"`
	LastBatchDurationMs    float64   `json:"lastBatchDurationMs"`
	LastSummaryDurationMs  float64   `json:"lastSummaryDurationMs"`
	TotalSummaryDurationMs float64   `json:"totalSummaryDurationMs"`
	SummaryRowsScanned     uint64    `json:"summaryRowsScanned"`
	CommittedBatches       uint64    `json:"committedBatches"`
	CommittedRecords       uint64    `json:"committedRecords"`
	Failures               uint64    `json:"failures"`
	Retries                uint64    `json:"retries"`
	DiscardedRecords       uint64    `json:"discardedRecords"`
	DiscardedBytes         uint64    `json:"discardedBytes"`
	AdmissionRejections    uint64    `json:"admissionRejections"`
	PersistenceFailing     bool      `json:"persistenceFailing"`
	LastPersistenceError   string    `json:"lastPersistenceError"`
	LastFailure            time.Time `json:"lastFailure"`
	LastCommit             time.Time `json:"lastCommit"`
}

func (s *Store) Stats() PersistenceStats {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	stats := s.stats
	stats.OutstandingRecords, stats.OutstandingBytes = s.count, s.bytes
	stats.InFlightRecords, stats.InFlightBytes = s.inFlightRecords, s.inFlightBytes
	stats.PendingRecords, stats.PendingBytes = s.count-s.inFlightRecords, s.bytes-s.inFlightBytes
	if s.count > 0 {
		stats.OldestOutstandingAgeMs = time.Since(s.queue[s.head].admitted).Milliseconds()
	}
	stats.Ready = !s.closed && !stats.PersistenceFailing && stats.DiscardedRecords == 0 &&
		s.count < s.config.MaxOutstandingRecords && s.bytes < s.config.MaxOutstandingBytes &&
		(stats.OldestOutstandingAgeMs < int64(2*s.config.WriteTimeoutSeconds)*1000)
	return stats
}

func (s *Store) StopAdmission() {
	s.pendingMu.Lock()
	s.closed = true
	s.notifyLocked()
	s.pendingMu.Unlock()
}

func (s *Store) IngestSpans(ctx context.Context, spans []model.SpanRecord) (int, error) {
	return s.admit(ctx, spans, nil)
}

func (s *Store) IngestLogs(ctx context.Context, logs []model.LogRecord) (int, error) {
	return s.admit(ctx, nil, logs)
}

func (s *Store) admit(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n, bytes := len(spans)+len(logs), 0
	var invalid error
	for i := range spans {
		cost, work := spanCost(spans[i])
		if math.IsNaN(spans[i].DurationMs) || math.IsInf(spans[i].DurationMs, 0) {
			invalid = ErrInvalidRecord
		}
		if cost > s.config.MaxBatchBytes || work > s.config.MaxBatchWork {
			invalid = ErrRecordTooLarge
		}
		if !sqliteAttributesFit(spans[i].Attributes, spans[i].Resource, 4) {
			invalid = ErrRecordTooLarge
		}
		bytes += cost
	}
	for i := range logs {
		cost, work := logCost(logs[i])
		if cost > s.config.MaxBatchBytes || work > s.config.MaxBatchWork {
			invalid = ErrRecordTooLarge
		}
		if !sqliteAttributesFit(logs[i].Attributes, logs[i].Resource, 3) {
			invalid = ErrRecordTooLarge
		}
		bytes += cost
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if invalid == nil && (n > len(s.queue) || bytes > s.config.MaxOutstandingBytes) {
		invalid = ErrRecordTooLarge
	}
	if invalid != nil {
		s.stats.AdmissionRejections++
		return 0, invalid
	}
	if n > len(s.queue)-s.count || bytes > s.config.MaxOutstandingBytes-s.bytes {
		s.stats.AdmissionRejections++
		return 0, ErrOverloaded
	}
	// Reserve under the same lock BEFORE cloning maps/events/pointers. Admission
	// is all-or-nothing, and concurrent callers cannot reuse this reservation.
	s.bytes += bytes
	s.stats.HighWaterBytes = max(s.stats.HighWaterBytes, s.bytes)
	spans, logs = cloneSpanRecords(spans), cloneLogRecords(logs)
	now := time.Now()
	for i := range spans {
		cost, work := spanCost(spans[i])
		record := spans[i] // Do not retain an entire cloned request through one slice element.
		s.enqueueLocked(queuedRecord{span: &record, bytes: cost, work: work, admitted: now})
	}
	for i := range logs {
		cost, work := logCost(logs[i])
		record := logs[i]
		s.enqueueLocked(queuedRecord{log: &record, bytes: cost, work: work, admitted: now})
	}
	return n, nil
}

func (s *Store) enqueueLocked(record queuedRecord) {
	s.nextSeq++
	record.seq = s.nextSeq
	s.queue[(s.head+s.count)%len(s.queue)] = record
	s.count++
	s.stats.HighWaterRecords = max(s.stats.HighWaterRecords, s.count)
}

// Accounting, not RSS: fixed overhead plus 8x variable strings (JSON escaping,
// driver parameters and retained payload) and 128 bytes per map entry. Work
// counts related attribute/event entries as well as the record's SQL operations.
func mapCost(m map[string]string) (bytes, work int) {
	bytes = 64
	for k, v := range m {
		bytes += 128 + 8*(len(k)+len(v))
		work++
	}
	return
}

func pointerCost(v *string) int {
	if v == nil {
		return 0
	}
	return 32 + 8*len(*v)
}

func spanCost(v model.SpanRecord) (bytes, work int) {
	bytes = 512 + 8*(len(v.TraceID)+len(v.SpanID)+len(v.ServiceName)+len(v.OperationName)+len(v.Status)) +
		pointerCost(v.ParentSpanID) + pointerCost(v.ScopeName) + pointerCost(v.Kind)
	work = 3
	for _, m := range []map[string]string{v.Attributes, v.Resource} {
		b, w := mapCost(m)
		bytes += b
		work += w
	}
	for _, e := range v.Events {
		b, w := mapCost(e.Attributes)
		bytes += 128 + 8*len(e.Name) + b
		work += 1 + w
	}
	return
}

func logCost(v model.LogRecord) (bytes, work int) {
	bytes = 512 + 8*(len(v.ServiceName)+len(v.SeverityText)+len(v.Body)) +
		pointerCost(v.TraceID) + pointerCost(v.SpanID) + pointerCost(v.ScopeName)
	work = 1
	for _, m := range []map[string]string{v.Attributes, v.Resource} {
		b, w := mapCost(m)
		bytes += b
		work += w
	}
	return
}

func (s *Store) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }
func (s *Store) wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// Flush snapshots admission order at entry. Later admissions do not extend its
// barrier. A failure returns an error, not a promise that retry has stopped.
func (s *Store) Flush(ctx context.Context) error {
	s.pendingMu.Lock()
	target := s.nextSeq
	s.pendingMu.Unlock()
	s.wake()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.pendingMu.Lock()
		var err error
		if s.firstDiscardSeq != 0 && s.firstDiscardSeq <= target {
			err = s.discardError
		}
		complete := s.completedSeq >= target
		if !complete && s.stats.PersistenceFailing {
			err = errors.New(s.stats.LastPersistenceError)
		}
		changed := s.changed
		s.pendingMu.Unlock()
		if err != nil || complete {
			return err
		}
		select {
		case <-changed:
		case <-s.doneCh:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Store) Close() error {
	return s.CloseContext(context.Background())
}

// CloseContext stops admission and bounds the caller's wait, including driver
// close/checkpoint. If a driver fails to honor cancellation, cleanup continues
// behind the writer; no second writer or concurrent database close is started.
func (s *Store) CloseContext(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.config.ShutdownTimeoutSeconds)*time.Second)
	defer cancel()
	s.closeOnce.Do(func() {
		s.StopAdmission()
		close(s.stopCh)
		go func() {
			stopCancel := context.AfterFunc(ctx, s.cancelWriter)
			defer stopCancel()
			defer s.cancelWriter()
			<-s.doneCh
			s.shutdownErr = errors.Join(s.shutdownErr, s.db.Close())
			close(s.closeDone)
		}()
	})
	select {
	case <-s.closeDone:
		return s.shutdownErr
	case <-ctx.Done():
		s.cancelWriter()
		err := s.drainError(ctx.Err())
		slog.Error("telemetry shutdown incomplete", "error", err)
		return err
	}
}

func (s *Store) drainError(cause error) error {
	stats := s.Stats()
	return fmt.Errorf("telemetry drain: %d records, %d estimated bytes remain; last persistence error=%q: %w",
		stats.OutstandingRecords, stats.OutstandingBytes, stats.LastPersistenceError, cause)
}

func (s *Store) runWriter(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(s.doneCh)
	for {
		stopping := false
		select {
		case <-ticker.C:
		case <-s.wakeCh:
		case <-s.stopCh:
			stopping = true
		}
		s.pendingMu.Lock()
		target := s.nextSeq
		s.pendingMu.Unlock()
		err := s.drainTo(target)
		if stopping || s.writerCtx.Err() != nil {
			if err != nil {
				s.shutdownErr = s.drainError(err)
			}
			s.pendingMu.Lock()
			s.shutdownErr = errors.Join(s.shutdownErr, s.discardError)
			s.pendingMu.Unlock()
			if s.shutdownErr != nil {
				slog.Error("telemetry shutdown incomplete", "error", s.shutdownErr)
			}
			return
		}
	}
}

const retryInitial = 100 * time.Millisecond
const retryMaximum = 2 * time.Second
const attemptsBeforeReduction = 5

func persistenceClass(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "database operation canceled or timed out"
	}
	var sqliteError sqlite3.Error
	if errors.As(err, &sqliteError) {
		switch sqliteError.Code {
		case sqlite3.ErrNomem:
			return "database memory allocation failed"
		case sqlite3.ErrConstraint, sqlite3.ErrMismatch, sqlite3.ErrTooBig, sqlite3.ErrRange:
			return "permanent database record error"
		case sqlite3.ErrInterrupt:
			return "database operation canceled or timed out"
		}
	}
	return "database persistence unavailable"
}

func (s *Store) drainTo(target uint64) error {
	for {
		if err := s.writerCtx.Err(); err != nil {
			return err
		}
		s.pendingMu.Lock()
		n, bytes, work := 0, 0, 0
		for n < s.count && n < s.config.MaxBatchRecords {
			r := s.queue[(s.head+n)%len(s.queue)]
			if r.seq > target || bytes+r.bytes > s.config.MaxBatchBytes || work+r.work > s.config.MaxBatchWork {
				break
			}
			n++
			bytes += r.bytes
			work += r.work
		}
		s.pendingMu.Unlock()
		if n == 0 {
			return nil
		}
		attempts, delay := 0, retryInitial
		for {
			spans, logs, bytes, work := s.batch(n)
			started := time.Now()
			ctx, cancel := context.WithTimeout(s.writerCtx, time.Duration(s.config.WriteTimeoutSeconds)*time.Second)
			s.writeMu.Lock()
			var err error
			if err = ctx.Err(); err == nil {
				if s.persist != nil {
					err = s.persist(ctx, spans, logs)
				} else {
					err = s.writeBatch(ctx, spans, logs)
				}
			}
			s.writeMu.Unlock()
			if err != nil && ctx.Err() != nil {
				err = ctx.Err()
			}
			cancel()
			s.pendingMu.Lock()
			s.stats.LastBatchDurationMs = float64(time.Since(started)) / float64(time.Millisecond)
			if err == nil {
				s.stats.CommittedBatches++
				s.stats.CommittedRecords += uint64(n)
				s.stats.LastCommit = time.Now()
				s.stats.PersistenceFailing = false
				s.releaseLocked(n)
				s.pendingMu.Unlock()
				break
			}
			class := persistenceClass(err)
			s.stats.Failures++
			s.stats.PersistenceFailing = true
			s.stats.LastPersistenceError, s.stats.LastFailure = class, time.Now()
			s.notifyLocked()
			// Log classifications only; driver errors can contain attribute values.
			if time.Since(s.lastFailureLog) >= 5*time.Second {
				s.lastFailureLog = time.Now()
				slog.Warn("telemetry persistence failed", "error", class, "records", n, "estimated_bytes", bytes, "work", work)
			}
			attempts++
			permanent := class == "permanent database record error"
			memory := class == "database memory allocation failed"
			if n == 1 && (permanent || (memory && attempts >= attemptsBeforeReduction)) && s.writerCtx.Err() == nil {
				if s.firstDiscardSeq == 0 {
					s.firstDiscardSeq = s.queue[s.head].seq
					s.discardError = fmt.Errorf("admitted telemetry discarded: %s (see persistence counters)", class)
				}
				s.stats.DiscardedRecords++
				s.stats.DiscardedBytes += uint64(bytes)
				if time.Since(s.lastDiscardLog) >= 5*time.Second {
					s.lastDiscardLog = time.Now()
					slog.Error("telemetry record discarded", "error", class, "estimated_bytes", bytes, "discarded_records", s.stats.DiscardedRecords)
				}
				s.releaseLocked(1)
				s.pendingMu.Unlock()
				break
			}
			if n > 1 && (permanent || memory || attempts >= attemptsBeforeReduction) {
				n = max(1, n/2)
				attempts = 0
			}
			s.stats.Retries++
			s.pendingMu.Unlock()
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-s.writerCtx.Done():
				timer.Stop()
				return s.writerCtx.Err()
			}
			delay = min(retryMaximum, delay*2)
		}
	}
}

func (s *Store) batch(n int) (spans []model.SpanRecord, logs []model.LogRecord, bytes, work int) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for i := 0; i < n; i++ {
		r := s.queue[(s.head+i)%len(s.queue)]
		if r.span != nil {
			spans = append(spans, *r.span)
		} else {
			logs = append(logs, *r.log)
		}
		bytes += r.bytes
		work += r.work
	}
	s.inFlightRecords, s.inFlightBytes = n, bytes
	s.stats.LastBatchRecords, s.stats.LastBatchBytes = n, bytes
	s.stats.MaxBatchRecords = max(s.stats.MaxBatchRecords, n)
	s.stats.MaxBatchBytes = max(s.stats.MaxBatchBytes, bytes)
	s.stats.MaxBatchWork = max(s.stats.MaxBatchWork, work)
	return
}

func (s *Store) releaseLocked(n int) {
	for i := 0; i < n; i++ {
		r := &s.queue[s.head]
		s.bytes -= r.bytes
		s.completedSeq = r.seq
		*r = queuedRecord{}
		s.head = (s.head + 1) % len(s.queue)
		s.count--
	}
	s.inFlightRecords, s.inFlightBytes = 0, 0
	s.notifyLocked()
}

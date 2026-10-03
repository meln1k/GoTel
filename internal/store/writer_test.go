package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
)

func boundedStore(t *testing.T, configure func(*config.Config)) *Store {
	t.Helper()
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "bounded.sqlite")
	if configure != nil {
		configure(&cfg)
	}
	s, err := openWithBatchInterval(cfg, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := s.CloseContext(ctx)
		if err != nil && s.Stats().DiscardedRecords == 0 && s.Stats().OutstandingRecords == 0 {
			t.Error(err)
		}
	})
	return s
}

func awaitStats(t *testing.T, s *Store, predicate func(PersistenceStats) bool) PersistenceStats {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		stats := s.Stats()
		if predicate(stats) {
			return stats
		}
		select {
		case <-deadline:
			t.Fatalf("persistence did not reach expected state: %+v", stats)
		case <-time.After(time.Millisecond):
		}
	}
}

type writeAttempt struct {
	ids    []string
	result chan error
}

func takeAttempt(t *testing.T, attempts <-chan writeAttempt) writeAttempt {
	t.Helper()
	select {
	case a := <-attempts:
		return a
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not attempt persistence")
	}
	return writeAttempt{}
}

func TestOutstandingBudgetIncludesInFlightAndStableRetries(t *testing.T) {
	s := boundedStore(t, func(c *config.Config) { c.MaxOutstandingRecords, c.MaxBatchRecords = 4, 2 })
	attempts := make(chan writeAttempt, 10)
	s.persist = func(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) error {
		a := writeAttempt{result: make(chan error)}
		for _, v := range spans {
			a.ids = append(a.ids, v.SpanID)
		}
		for _, v := range logs {
			a.ids = append(a.ids, v.Body)
		}
		select {
		case attempts <- a:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-a.result:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return s.writeBatch(ctx, spans, logs)
	}
	if _, err := s.IngestSpans(context.Background(), []model.SpanRecord{{TraceID: "t", SpanID: "a"}, {TraceID: "t", SpanID: "b"}}); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- s.Flush(context.Background()) }()
	a := takeAttempt(t, attempts)
	initial := s.Stats()
	if initial.OutstandingRecords != 2 || initial.InFlightRecords != 2 || initial.OutstandingBytes != initial.InFlightBytes {
		t.Fatalf("in-flight budget missing: %+v", initial)
	}
	if _, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "c"}, {Body: "d"}}); err != nil {
		t.Fatal(err)
	}
	full := s.Stats()
	for failures := 0; failures < 3; failures++ {
		if !reflect.DeepEqual(a.ids, []string{"a", "b"}) {
			t.Fatalf("retry absorbed arrivals: %v", a.ids)
		}
		for producer := 0; producer < 100; producer++ {
			if n, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: fmt.Sprint(producer)}}); n != 0 || !errors.Is(err, ErrOverloaded) {
				t.Fatalf("overload admitted: %d %v", n, err)
			}
		}
		if stats := s.Stats(); stats.OutstandingRecords != 4 || stats.OutstandingBytes != full.OutstandingBytes {
			t.Fatalf("failed batch lost budget: %+v", stats)
		}
		a.result <- errors.New("temporary failure includes PRIVATE payload")
		a = takeAttempt(t, attempts)
	}
	if err := <-flushed; err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("Flush must report sanitized failure: %v", err)
	}
	if s.Stats().Ready {
		t.Fatal("failed persistence reported ready")
	}
	a.result <- nil
	// Request the later work without blocking this gate-driving test on recovery.
	s.wake()
	a = takeAttempt(t, attempts)
	if !reflect.DeepEqual(a.ids, []string{"c", "d"}) {
		t.Fatalf("mixed FIFO work lost: %v", a.ids)
	}
	a.result <- nil
	stats := awaitStats(t, s, func(v PersistenceStats) bool { return v.CommittedRecords == 4 })
	if stats.OutstandingBytes != 0 || stats.OutstandingRecords != 0 || stats.Failures != 3 || stats.Retries != 3 || stats.AdmissionRejections != 300 || !stats.Ready || stats.LastPersistenceError == "" {
		t.Fatalf("bad recovery accounting: %+v", stats)
	}
	assertTableCount(t, s, "spans", 2)
	assertTableCount(t, s, "logs", 2)
	// Admission must resume after successful commit releases the reservation.
	if _, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "resumed"}}); err != nil {
		t.Fatal(err)
	}
	go func() { flushed <- s.Flush(context.Background()) }()
	a = takeAttempt(t, attempts)
	a.result <- nil
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	assertTableCount(t, s, "logs", 3)
}

func TestBatchLimitsAreSeparateRealTransactions(t *testing.T) {
	for _, limit := range []string{"records", "bytes", "work"} {
		t.Run(limit, func(t *testing.T) {
			s := boundedStore(t, func(c *config.Config) {
				c.MaxBatchRecords = 2
				if limit == "bytes" {
					c.MaxBatchRecords = 10
					c.MaxBatchBytes = 1500
				}
				if limit == "work" {
					c.MaxBatchRecords = 10
					c.MaxBatchWork = 5
				}
			})
			var seen []int
			s.persist = func(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) error {
				// Every preceding callback must have committed before this one.
				var count int
				if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM logs").Scan(&count); err != nil {
					return err
				}
				want := 0
				for _, n := range seen {
					want += n
				}
				if count != want {
					return fmt.Errorf("previous transaction not visible: count=%d records=%d", count, want)
				}
				seen = append(seen, len(logs))
				return s.writeBatch(ctx, spans, logs)
			}
			// Each log is 936 estimated bytes, 3 work units. Two fit the record
			// limit, but neither the 1500-byte nor the 5-work limit.
			logs := []model.LogRecord{{Body: "a", Attributes: map[string]string{"k": "v"}, Resource: map[string]string{"r": "s"}},
				{Body: "b", Attributes: map[string]string{"k": "v"}, Resource: map[string]string{"r": "s"}}}
			if limit == "records" {
				// Two transactions: first two records, then the third.
				logs = append(logs, model.LogRecord{Body: "c"})
			}
			if _, err := s.IngestLogs(context.Background(), logs); err != nil {
				t.Fatal(err)
			}
			mustFlush(t, s)
			want := []int{1, 1}
			if limit == "records" {
				want = []int{2, 1}
			}
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("transactions=%v want %v", seen, want)
			}
			assertTableCount(t, s, "logs", len(logs))
		})
	}
}

func TestEstimatedBytesAndOversizedRecords(t *testing.T) {
	s := boundedStore(t, func(c *config.Config) {
		c.MaxOutstandingRecords, c.MaxBatchRecords = 10, 10
		c.MaxOutstandingBytes, c.MaxBatchBytes = 1296, 648
	})
	// Empty maps cost 64 each; fixed log overhead 512 + one byte x 8 = 648.
	if n, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "a"}, {Body: "b"}}); n != 2 || err != nil {
		t.Fatalf("byte boundary: %d %v", n, err)
	}
	if _, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "c"}}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("byte budget not enforced: %v", err)
	}
	if _, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "aa"}}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("single oversized record: %v", err)
	}
	if _, err := s.IngestSpans(context.Background(), []model.SpanRecord{{DurationMs: math.NaN()}}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("invalid record: %v", err)
	}
	if stats := s.Stats(); stats.OutstandingBytes != 1296 || stats.OutstandingRecords != 2 {
		t.Fatalf("invalid admission retained copies: %+v", stats)
	}
	mustFlush(t, s)
	if stats := s.Stats(); stats.MaxBatchRecords != 1 || stats.CommittedBatches != 2 {
		t.Fatalf("byte bound ignored: %+v", stats)
	}
	if _, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "c"}}); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	assertTableCount(t, s, "logs", 3)
	// Variable attributes and events must consume both budgets independently.
	v := model.SpanRecord{Attributes: map[string]string{"attr": "value"}, Resource: map[string]string{"resource": "payload"},
		Events: []model.EventRecord{{Name: "event", Attributes: map[string]string{"event-key": "event-value"}}}}
	bytes, work := spanCost(v)
	if bytes != 640+128+8*9+128+8*15+128+8*5+64+128+8*20 || work != 7 {
		t.Fatalf("variable estimate: bytes=%d work=%d", bytes, work)
	}
}

func TestFlushBarrierDoesNotWaitForLaterAdmission(t *testing.T) {
	s := boundedStore(t, func(c *config.Config) { c.MaxBatchRecords = 1 })
	entered := make(chan chan struct{}, 2)
	s.persist = func(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) error {
		gate := make(chan struct{})
		entered <- gate
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
		return s.writeBatch(ctx, spans, logs)
	}
	_, _ = s.IngestLogs(context.Background(), []model.LogRecord{{Body: "before"}})
	first := make(chan error, 1)
	go func() { first <- s.Flush(context.Background()) }()
	gate := <-entered
	_, _ = s.IngestLogs(context.Background(), []model.LogRecord{{Body: "after"}})
	second := make(chan error, 1)
	go func() { second <- s.Flush(context.Background()) }()
	close(gate)
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("barrier extended to later admission")
	}
	gate = <-entered
	select {
	case err := <-second:
		t.Fatalf("second Flush returned before commit: %v", err)
	default:
	}
	close(gate)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	assertTableCount(t, s, "logs", 2)
}

func TestPermanentDatabaseErrorIsolatesRecordAndRollsBack(t *testing.T) {
	s := boundedStore(t, nil)
	// A real constraint error occurs after a valid row in the original
	// batch. Rollback must prevent duplicating that valid log on isolation/retry.
	_, err := s.db.Exec(`DROP TABLE logs;
		CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, trace_id VARCHAR, span_id VARCHAR,
		service_name VARCHAR NOT NULL, scope_name VARCHAR, severity_text VARCHAR NOT NULL, timestamp_ms BIGINT NOT NULL,
		body VARCHAR NOT NULL CHECK(body <> 'bad-private-value'), attributes_json VARCHAR NOT NULL, resource_json VARCHAR NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.IngestLogs(context.Background(), []model.LogRecord{{Body: "first"}, {Body: "bad-private-value"}, {Body: "last"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(context.Background()); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatalf("constraint error was hidden or leaked: %v", err)
	}
	stats := awaitStats(t, s, func(v PersistenceStats) bool { return v.OutstandingRecords == 0 })
	if stats.DiscardedRecords != 1 || stats.DiscardedBytes == 0 || stats.CommittedRecords != 2 || stats.Ready {
		t.Fatalf("terminal disposition: %+v", stats)
	}
	rows, err := s.db.Query(`SELECT body FROM logs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || !reflect.DeepEqual(bodies, []string{"first", "last"}) {
		t.Fatalf("rollback/isolation output: %v %v", bodies, err)
	}
	if err := s.Flush(context.Background()); err == nil {
		t.Fatal("discarded admission falsely reported committed")
	}
	if err := s.Close(); err == nil {
		t.Fatal("shutdown hid terminal loss")
	}
}

func TestMemoryErrorReducesBatchAndReportsSingleRecordFailure(t *testing.T) {
	for _, irreducible := range []bool{false, true} {
		t.Run(fmt.Sprint(irreducible), func(t *testing.T) {
			s := boundedStore(t, nil)
			attempts := 0
			s.persist = func(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) error {
				attempts++
				if len(logs) > 1 || irreducible && logs[0].Body == "bad" {
					return fmt.Errorf("wrapped: %w", sqlite3.Error{Code: sqlite3.ErrNomem})
				}
				return s.writeBatch(ctx, spans, logs)
			}
			logs := []model.LogRecord{{Body: "bad"}, {Body: "good"}}
			if irreducible {
				logs = logs[:1]
			}
			_, _ = s.IngestLogs(context.Background(), logs)
			if err := s.Flush(context.Background()); err == nil {
				t.Fatal("memory failure unreported")
			}
			stats := awaitStats(t, s, func(v PersistenceStats) bool { return v.OutstandingRecords == 0 })
			if irreducible {
				if stats.DiscardedRecords != 1 || attempts != attemptsBeforeReduction {
					t.Fatalf("unbounded single-record retry: attempts=%d stats=%+v", attempts, stats)
				}
			} else if stats.CommittedRecords != 2 || stats.DiscardedRecords != 0 || attempts != 3 {
				t.Fatalf("batch did not reduce: attempts=%d stats=%+v", attempts, stats)
			}
		})
	}
}

func TestShutdownDeadlineCancelsRealDatabaseWork(t *testing.T) {
	s := boundedStore(t, nil)
	entered := make(chan struct{})
	s.persist = func(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) error {
		close(entered)
		var value float64
		query := `WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM r WHERE i<999999)
			SELECT sum(CAST(a.i AS DOUBLE)*b.i) FROM r a, r b`
		return s.db.QueryRowContext(ctx, query).Scan(&value)
	}
	_, _ = s.IngestLogs(context.Background(), []model.LogRecord{{Body: "not committed"}})
	s.wake()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.CloseContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "1 records") || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("shutdown not bounded/reporting: %v duration=%v", err, time.Since(start))
	}
	if _, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "late"}}); !errors.Is(err, ErrClosed) {
		t.Fatal("shutdown admitted more work")
	}
	select {
	case <-s.closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("database did not honor cancellation")
	}
}

func TestCloseContextAppliesConfiguredDeadline(t *testing.T) {
	s := boundedStore(t, func(c *config.Config) { c.ShutdownTimeoutSeconds = 1 })
	entered := make(chan struct{})
	s.persist = func(ctx context.Context, _ []model.SpanRecord, _ []model.LogRecord) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	_, _ = s.IngestLogs(context.Background(), []model.LogRecord{{Body: "retained"}})
	s.wake()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- s.CloseContext(context.Background()) }()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "1 records") {
			t.Fatalf("configured shutdown deadline/report ignored: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background close ignored the configured shutdown deadline")
	}
}

func TestShutdownInterruptsRetryBackoff(t *testing.T) {
	s := boundedStore(t, nil)
	s.persist = func(context.Context, []model.SpanRecord, []model.LogRecord) error {
		return errors.New("temporary failure")
	}
	_, _ = s.IngestLogs(context.Background(), []model.LogRecord{{Body: "retained"}})
	if err := s.Flush(context.Background()); err == nil {
		t.Fatal("failure not returned")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.CloseContext(ctx); err == nil || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("backoff prevented bounded close: %v", err)
	}
	select {
	case <-s.closeDone:
	case <-time.After(time.Second):
		t.Fatal("retry backoff did not interrupt")
	}
}

func TestBoundedTransactionsPreserveUpdatesAndLateRoot(t *testing.T) {
	s := boundedStore(t, func(c *config.Config) { c.MaxBatchRecords = 1 })
	root := "root"
	child := model.SpanRecord{TraceID: "late", SpanID: "child", ParentSpanID: &root, ServiceName: "worker", OperationName: "child", Status: "error", StartTimeMs: 20,
		Resource: map[string]string{"region": "west", "collision": "resource"}, Attributes: map[string]string{"collision": "span", "old": "remove"},
		Events: []model.EventRecord{{Name: "original", Attributes: map[string]string{"event": "kept"}}}}
	_, err := s.IngestSpans(context.Background(), []model.SpanRecord{child})
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	_, err = s.IngestSpans(context.Background(), []model.SpanRecord{{TraceID: "late", SpanID: "root", ServiceName: "api", OperationName: "late root", Status: "ok", StartTimeMs: 10, EndTimeMs: 40, DurationMs: 30}})
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	child.EndTimeMs, child.DurationMs, child.Status = 30, 10, "ok"
	child.Attributes = map[string]string{"collision": "updated", "new": "value"}
	_, err = s.IngestSpans(context.Background(), []model.SpanRecord{child})
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	trace, err := s.GetTrace(context.Background(), "late")
	if err != nil || trace == nil {
		t.Fatalf("trace: %v %v", trace, err)
	}
	if trace.SpanCount != 2 || trace.ErrorCount != 0 || trace.IsRunning || trace.DurationMs != 30 || trace.ServiceName != "api" || trace.RootOperationName != "late root" {
		t.Fatalf("late/update trace facts: %+v", trace.TraceSummary)
	}
	span := trace.Spans[1]
	if span.SpanID != "child" || span.Depth != 1 || span.Tags["collision"] != "updated" || span.Tags["region"] != "west" || span.Tags["new"] != "value" || len(span.Events) != 1 || span.Events[0].Attributes["event"] != "kept" {
		t.Fatalf("related rows/events changed: %+v", span)
	}
	var stale int
	if err := s.db.QueryRow(`SELECT count(*) FROM span_attributes WHERE trace_id='late' AND key='old'`).Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("stale attribute update: %d %v", stale, err)
	}
	summaries, err := s.ListTraceSummaries(context.Background(), TraceFilter{}, 10)
	if err != nil || len(summaries) != 1 || !reflect.DeepEqual(summaries[0], trace.TraceSummary) {
		t.Fatalf("summary no longer matches detail: %+v %v", summaries, err)
	}
	if s.Stats().CommittedBatches != 3 {
		t.Fatal("fixture did not exercise separate transactions")
	}
}

func TestReadinessUsesBacklogNotIdleCommitAge(t *testing.T) {
	s := boundedStore(t, nil)
	s.pendingMu.Lock()
	s.stats.LastCommit = time.Now().Add(-24 * time.Hour)
	s.pendingMu.Unlock()
	if !s.Stats().Ready {
		t.Fatal("idle process is unhealthy")
	}
	_, err := s.IngestLogs(context.Background(), []model.LogRecord{{Body: "old backlog"}})
	if err != nil {
		t.Fatal(err)
	}
	s.pendingMu.Lock()
	s.queue[s.head].admitted = time.Now().Add(-time.Duration(2*s.config.WriteTimeoutSeconds+1) * time.Second)
	s.pendingMu.Unlock()
	if s.Stats().Ready {
		t.Fatal("stalled backlog is ready")
	}
	mustFlush(t, s)
	if !s.Stats().Ready {
		t.Fatal("drained process did not recover readiness")
	}
}

func TestPersistenceClassificationUsesTypeNotPayload(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{errors.Join(context.DeadlineExceeded, sqlite3.Error{Code: sqlite3.ErrNomem}), "database operation canceled or timed out"},
		{fmt.Errorf("wrapped: %w", sqlite3.Error{Code: sqlite3.ErrConstraint}), "permanent database record error"},
		{sqlite3.Error{Code: sqlite3.ErrNomem}, "database memory allocation failed"},
		{sqlite3.Error{Code: sqlite3.ErrBusy, ExtendedCode: sqlite3.ErrBusySnapshot}, "database persistence unavailable"},
		{sqlite3.Error{Code: sqlite3.ErrLocked}, "database persistence unavailable"},
		{sqlite3.Error{Code: sqlite3.ErrInterrupt}, "database operation canceled or timed out"},
		{errors.New("Constraint Error: PRIVATE"), "database persistence unavailable"},
	} {
		if got := persistenceClass(test.err); got != test.want {
			t.Fatalf("classification=%q want %q", got, test.want)
		}
	}
}

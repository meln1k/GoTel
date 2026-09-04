package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
)

type Store struct {
	db      *sql.DB
	config  config.Config
	writeMu sync.Mutex

	pendingMu    sync.Mutex
	pendingSpans []model.SpanRecord
	pendingLogs  []model.LogRecord
	closed       bool
	flushCh      chan chan error
	stopCh       chan struct{}
	doneCh       chan struct{}
	shutdownErr  error
	closeOnce    sync.Once

	committedBatches uint64
}

const writeBatchInterval = 500 * time.Millisecond

var errStoreClosed = errors.New("store is closed")

func Open(cfg config.Config) (*Store, error) {
	return openWithBatchInterval(cfg, writeBatchInterval)
}

func openWithBatchInterval(cfg config.Config, interval time.Duration) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DatabasePath), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	store := &Store{
		db: db, config: cfg,
		flushCh: make(chan chan error), stopCh: make(chan struct{}), doneCh: make(chan struct{}),
	}
	if err := store.initialize(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	go store.runWriter(interval)
	return store, nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.pendingMu.Lock()
		s.closed = true
		s.pendingMu.Unlock()
		close(s.stopCh)
		<-s.doneCh
		s.shutdownErr = errors.Join(s.shutdownErr, s.db.Close())
	})
	return s.shutdownErr
}

// Flush waits until all telemetry accepted before the call has been written.
func (s *Store) Flush(ctx context.Context) error {
	result := make(chan error, 1)
	select {
	case s.flushCh <- result:
	case <-s.doneCh:
		return errStoreClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-s.doneCh:
		return errStoreClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) runWriter(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(s.doneCh)
	for {
		select {
		case <-ticker.C:
			_ = s.flushPending()
		case result := <-s.flushCh:
			result <- s.flushPending()
		case <-s.stopCh:
			s.shutdownErr = s.flushPending()
			return
		}
	}
}

func (s *Store) flushPending() error {
	s.pendingMu.Lock()
	spans, logs := s.pendingSpans, s.pendingLogs
	s.pendingSpans, s.pendingLogs = nil, nil
	s.pendingMu.Unlock()
	if len(spans) == 0 && len(logs) == 0 {
		return nil
	}
	s.writeMu.Lock()
	err := s.writeBatch(context.Background(), spans, logs)
	s.writeMu.Unlock()
	if err == nil {
		s.pendingMu.Lock()
		s.committedBatches++
		s.pendingMu.Unlock()
		return nil
	}
	s.pendingMu.Lock()
	requeuedSpans := make([]model.SpanRecord, 0, len(spans)+len(s.pendingSpans))
	requeuedSpans = append(requeuedSpans, spans...)
	s.pendingSpans = append(requeuedSpans, s.pendingSpans...)
	requeuedLogs := make([]model.LogRecord, 0, len(logs)+len(s.pendingLogs))
	requeuedLogs = append(requeuedLogs, logs...)
	s.pendingLogs = append(requeuedLogs, s.pendingLogs...)
	s.pendingMu.Unlock()
	return err
}

func (s *Store) initialize(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS spans (
			trace_id VARCHAR NOT NULL,
			span_id VARCHAR NOT NULL,
			parent_span_id VARCHAR,
			service_name VARCHAR NOT NULL,
			scope_name VARCHAR,
			operation_name VARCHAR NOT NULL,
			kind VARCHAR,
			start_time_ms BIGINT NOT NULL,
			end_time_ms BIGINT NOT NULL,
			duration_ms DOUBLE NOT NULL,
			status VARCHAR NOT NULL,
			attributes_json VARCHAR NOT NULL,
			resource_json VARCHAR NOT NULL,
			events_json VARCHAR NOT NULL,
			PRIMARY KEY (trace_id, span_id)
		)`,
		`CREATE SEQUENCE IF NOT EXISTS logs_id_seq START 1`,
		`CREATE TABLE IF NOT EXISTS logs (
			id BIGINT PRIMARY KEY DEFAULT nextval('logs_id_seq'),
			trace_id VARCHAR,
			span_id VARCHAR,
			service_name VARCHAR NOT NULL,
			scope_name VARCHAR,
			severity_text VARCHAR NOT NULL,
			timestamp_ms BIGINT NOT NULL,
			body VARCHAR NOT NULL,
			attributes_json VARCHAR NOT NULL,
			resource_json VARCHAR NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS trace_summaries (
			trace_id VARCHAR PRIMARY KEY,
			service_name VARCHAR NOT NULL,
			root_operation_name VARCHAR NOT NULL,
			started_at_ms BIGINT NOT NULL,
			ended_at_ms BIGINT NOT NULL,
			active_span_count BIGINT NOT NULL DEFAULT 0,
			duration_ms DOUBLE NOT NULL,
			span_count BIGINT NOT NULL,
			error_count BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS span_attributes (
			trace_id VARCHAR NOT NULL,
			span_id VARCHAR NOT NULL,
			key VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			PRIMARY KEY (trace_id, span_id, key)
		)`,
		`CREATE TABLE IF NOT EXISTS log_attributes (
			log_id BIGINT NOT NULL,
			key VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			PRIMARY KEY (log_id, key)
		)`,
		`CREATE TABLE IF NOT EXISTS gotel_maintenance (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)`,
		`INSERT INTO gotel_maintenance (key, value) VALUES ('schema_version', '1') ON CONFLICT (key) DO NOTHING`,
		`CREATE INDEX IF NOT EXISTS spans_start_idx ON spans(start_time_ms)`,
		`CREATE INDEX IF NOT EXISTS spans_service_idx ON spans(service_name)`,
		`CREATE INDEX IF NOT EXISTS spans_operation_idx ON spans(operation_name)`,
		`CREATE INDEX IF NOT EXISTS spans_span_id_idx ON spans(span_id)`,
		`CREATE INDEX IF NOT EXISTS logs_time_idx ON logs(timestamp_ms)`,
		`CREATE INDEX IF NOT EXISTS logs_trace_idx ON logs(trace_id)`,
		`CREATE INDEX IF NOT EXISTS logs_span_idx ON logs(span_id)`,
		`CREATE INDEX IF NOT EXISTS summaries_start_idx ON trace_summaries(started_at_ms)`,
		`CREATE INDEX IF NOT EXISTS span_attributes_lookup_idx ON span_attributes(key, value)`,
		`CREATE INDEX IF NOT EXISTS log_attributes_lookup_idx ON log_attributes(key, value)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize store: %w", err)
		}
	}
	return nil
}

func (s *Store) IngestSpans(ctx context.Context, spans []model.SpanRecord) (int, error) {
	if len(spans) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	spans = cloneSpanRecords(spans)
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.closed {
		return 0, errStoreClosed
	}
	s.pendingSpans = append(s.pendingSpans, spans...)
	return len(spans), nil
}

func (s *Store) IngestLogs(ctx context.Context, logs []model.LogRecord) (int, error) {
	if len(logs) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	logs = cloneLogRecords(logs)
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.closed {
		return 0, errStoreClosed
	}
	s.pendingLogs = append(s.pendingLogs, logs...)
	return len(logs), nil
}

func (s *Store) writeBatch(ctx context.Context, spans []model.SpanRecord, logs []model.LogRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	touched := make(map[string]struct{})
	for _, span := range spans {
		attributesJSON, err := marshalObject(span.Attributes)
		if err != nil {
			return err
		}
		resourceJSON, err := marshalObject(span.Resource)
		if err != nil {
			return err
		}
		eventsJSON, err := json.Marshal(span.Events)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO spans (
			trace_id, span_id, parent_span_id, service_name, scope_name, operation_name, kind,
			start_time_ms, end_time_ms, duration_ms, status, attributes_json, resource_json, events_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (trace_id, span_id) DO UPDATE SET
			parent_span_id=excluded.parent_span_id, service_name=excluded.service_name,
			scope_name=excluded.scope_name, operation_name=excluded.operation_name, kind=excluded.kind,
			start_time_ms=excluded.start_time_ms, end_time_ms=excluded.end_time_ms,
			duration_ms=excluded.duration_ms, status=excluded.status,
			attributes_json=excluded.attributes_json, resource_json=excluded.resource_json,
			events_json=excluded.events_json`,
			span.TraceID, span.SpanID, span.ParentSpanID, span.ServiceName, span.ScopeName, span.OperationName, span.Kind,
			span.StartTimeMs, span.EndTimeMs, span.DurationMs, span.Status, attributesJSON, resourceJSON, string(eventsJSON))
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM span_attributes WHERE trace_id=? AND span_id=?`, span.TraceID, span.SpanID); err != nil {
			return err
		}
		for key, value := range merged(span.Resource, span.Attributes) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO span_attributes (trace_id, span_id, key, value) VALUES (?, ?, ?, ?)`, span.TraceID, span.SpanID, key, value); err != nil {
				return err
			}
		}
		touched[span.TraceID] = struct{}{}
	}
	for traceID := range touched {
		if err := refreshSummary(ctx, tx, traceID); err != nil {
			return err
		}
	}
	for _, record := range logs {
		attributesJSON, err := marshalObject(record.Attributes)
		if err != nil {
			return err
		}
		resourceJSON, err := marshalObject(record.Resource)
		if err != nil {
			return err
		}
		var id int64
		err = tx.QueryRowContext(ctx, `INSERT INTO logs (
			trace_id, span_id, service_name, scope_name, severity_text, timestamp_ms, body, attributes_json, resource_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, record.TraceID, record.SpanID, record.ServiceName,
			record.ScopeName, record.SeverityText, record.TimestampMs, record.Body, attributesJSON, resourceJSON).Scan(&id)
		if err != nil {
			return err
		}
		for key, value := range merged(record.Resource, record.Attributes) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO log_attributes (log_id, key, value) VALUES (?, ?, ?)`, id, key, value); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func refreshSummary(ctx context.Context, tx *sql.Tx, traceID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT parent_span_id, service_name, operation_name, start_time_ms, end_time_ms, status
		FROM spans WHERE trace_id=? ORDER BY start_time_ms ASC, span_id ASC`, traceID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var facts traceFacts
	for rows.Next() {
		var parent sql.NullString
		var service, operation, status string
		var start, end int64
		if err := rows.Scan(&parent, &service, &operation, &start, &end, &status); err != nil {
			return err
		}
		facts.add(parent, service, operation, start, end, status)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if facts.spanCount == 0 {
		_, err := tx.ExecContext(ctx, `DELETE FROM trace_summaries WHERE trace_id=?`, traceID)
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO trace_summaries (
		trace_id, service_name, root_operation_name, started_at_ms, ended_at_ms,
		active_span_count, duration_ms, span_count, error_count
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (trace_id) DO UPDATE SET service_name=excluded.service_name,
		root_operation_name=excluded.root_operation_name, started_at_ms=excluded.started_at_ms,
		ended_at_ms=excluded.ended_at_ms, active_span_count=excluded.active_span_count,
		duration_ms=excluded.duration_ms, span_count=excluded.span_count, error_count=excluded.error_count`,
		traceID, facts.rootService, facts.rootOperation, facts.started, facts.ended, facts.activeSpanCount,
		facts.durationAt(facts.ended), facts.spanCount, facts.errorCount)
	return err
}

type traceFacts struct {
	rootService, rootOperation string
	started, ended             int64
	activeSpanCount, spanCount int
	errorCount                 int
	hasExplicitRoot            bool
}

func (facts *traceFacts) add(parent sql.NullString, service, operation string, start, end int64, status string) {
	if facts.spanCount == 0 {
		facts.rootService, facts.rootOperation = service, operation
		facts.started, facts.ended = start, end
	} else {
		facts.started = min(facts.started, start)
		facts.ended = max(facts.ended, end)
	}
	if !facts.hasExplicitRoot && (!parent.Valid || parent.String == "") {
		facts.rootService, facts.rootOperation = service, operation
		facts.hasExplicitRoot = true
	}
	if end <= 0 || end < start {
		facts.activeSpanCount++
	}
	if status == "error" {
		facts.errorCount++
	}
	facts.spanCount++
}

func (facts traceFacts) durationAt(now int64) float64 {
	ended := facts.ended
	if facts.activeSpanCount > 0 {
		ended = now
	}
	return float64(max(int64(0), ended-facts.started))
}

func (facts traceFacts) summary(traceID string, now int64) model.TraceSummary {
	return model.TraceSummary{
		TraceID: traceID, ServiceName: facts.rootService, RootOperationName: facts.rootOperation,
		StartedAt: model.ISOTime(facts.started), IsRunning: facts.activeSpanCount > 0, DurationMs: facts.durationAt(now),
		SpanCount: facts.spanCount, ErrorCount: facts.errorCount, Warnings: []string{},
	}
}

type dbSpan struct {
	traceID, spanID, serviceName, operationName, status string
	parentSpanID, scopeName, kind                       sql.NullString
	startTimeMs, endTimeMs                              int64
	durationMs                                          float64
	attributesJSON, resourceJSON, eventsJSON            string
}

const spanColumns = `trace_id, span_id, parent_span_id, service_name, scope_name, operation_name, kind,
	start_time_ms, end_time_ms, duration_ms, status, attributes_json, resource_json, events_json`

func (span *dbSpan) scanTargets() []any {
	return []any{&span.traceID, &span.spanID, &span.parentSpanID, &span.serviceName, &span.scopeName,
		&span.operationName, &span.kind, &span.startTimeMs, &span.endTimeMs, &span.durationMs, &span.status,
		&span.attributesJSON, &span.resourceJSON, &span.eventsJSON}
}

func scanSpan(scanner interface{ Scan(...any) error }) (dbSpan, error) {
	var span dbSpan
	err := scanner.Scan(span.scanTargets()...)
	return span, err
}

func hydrateSpan(span dbSpan, now int64) model.TraceSpan {
	attributes := decodeObject(span.attributesJSON)
	resource := decodeObject(span.resourceJSON)
	events := make([]model.EventRecord, 0)
	_ = json.Unmarshal([]byte(span.eventsJSON), &events)
	hydratedEvents := make([]model.TraceSpanEvent, 0, len(events))
	for _, event := range events {
		hydratedEvents = append(hydratedEvents, model.TraceSpanEvent{Name: event.Name, Timestamp: model.ISOTime(event.Timestamp), Attributes: nonNilMap(event.Attributes)})
	}
	running := span.endTimeMs <= 0 || span.endTimeMs < span.startTimeMs
	duration := span.durationMs
	if running {
		duration = float64(max(int64(0), now-span.startTimeMs))
	}
	return model.TraceSpan{
		SpanID: span.spanID, ParentSpanID: nullString(span.parentSpanID), ServiceName: span.serviceName,
		ScopeName: nullString(span.scopeName), Kind: nullString(span.kind), OperationName: span.operationName,
		StartTime: model.ISOTime(span.startTimeMs), IsRunning: running, DurationMs: duration, Status: span.status,
		Depth: 0, Tags: merged(resource, attributes), Warnings: []string{}, Events: hydratedEvents,
	}
}

func (s *Store) GetTrace(ctx context.Context, traceID string) (*model.Trace, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+spanColumns+` FROM spans WHERE trace_id=? ORDER BY start_time_ms ASC, span_id ASC`, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	dbSpans := make([]dbSpan, 0)
	for rows.Next() {
		span, err := scanSpan(rows)
		if err != nil {
			return nil, err
		}
		dbSpans = append(dbSpans, span)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(dbSpans) == 0 {
		return nil, nil
	}
	return buildTrace(traceID, dbSpans, time.Now().UnixMilli()), nil
}

func (s *Store) loadTraces(ctx context.Context, traceIDs []string) (map[string]*model.Trace, error) {
	result := make(map[string]*model.Trace, len(traceIDs))
	if len(traceIDs) == 0 {
		return result, nil
	}
	args := make([]any, len(traceIDs))
	for i, traceID := range traceIDs {
		args[i] = traceID
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(traceIDs)), ",")
	rows, err := s.db.QueryContext(ctx, `SELECT `+spanColumns+` FROM spans WHERE trace_id IN (`+placeholders+`)
		ORDER BY trace_id ASC, start_time_ms ASC, span_id ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grouped := make(map[string][]dbSpan, len(traceIDs))
	for rows.Next() {
		span, err := scanSpan(rows)
		if err != nil {
			return nil, err
		}
		grouped[span.traceID] = append(grouped[span.traceID], span)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	for traceID, spans := range grouped {
		result[traceID] = buildTrace(traceID, spans, now)
	}
	return result, nil
}

type treeSpan struct {
	span      model.TraceSpan
	start     int64
	synthetic bool
}

func buildTrace(traceID string, dbSpans []dbSpan, now int64) *model.Trace {
	nodes := make(map[string]*treeSpan, len(dbSpans))
	children := make(map[string][]string)
	roots := make([]string, 0)
	missing := make(map[string][]dbSpan)
	missingOrder := make([]string, 0)
	var facts traceFacts
	for _, raw := range dbSpans {
		facts.add(raw.parentSpanID, raw.serviceName, raw.operationName, raw.startTimeMs, raw.endTimeMs, raw.status)
		nodes[raw.spanID] = &treeSpan{span: hydrateSpan(raw, now), start: raw.startTimeMs}
	}
	for _, raw := range dbSpans {
		if !raw.parentSpanID.Valid || raw.parentSpanID.String == "" {
			roots = append(roots, raw.spanID)
			continue
		}
		parent := raw.parentSpanID.String
		if _, ok := nodes[parent]; ok {
			children[parent] = append(children[parent], raw.spanID)
		} else {
			if _, exists := missing[parent]; !exists {
				missingOrder = append(missingOrder, parent)
			}
			missing[parent] = append(missing[parent], raw)
			children[parent] = append(children[parent], raw.spanID)
		}
	}
	warnings := make([]string, 0, len(missing))
	for _, parentID := range missingOrder {
		orphaned := missing[parentID]
		start, end := orphaned[0].startTimeMs, orphaned[0].endTimeMs
		running := false
		for _, child := range orphaned {
			if child.startTimeMs < start {
				start = child.startTimeMs
			}
			if child.endTimeMs > end {
				end = child.endTimeMs
			}
			if child.endTimeMs <= 0 || child.endTimeMs < child.startTimeMs {
				running = true
			}
		}
		warning := fmt.Sprintf("missing span ID (%d children)", len(orphaned))
		if len(orphaned) == 1 {
			warning = "missing span ID (1 child)"
		}
		duration := float64(max(int64(0), end-start))
		if running {
			duration = float64(max(int64(0), now-start))
		}
		service := orphaned[0].serviceName
		nodes[parentID] = &treeSpan{start: start, synthetic: true, span: model.TraceSpan{
			SpanID: parentID, ServiceName: service, OperationName: fmt.Sprintf("[missing parent %s]", prefix(parentID, 8)),
			StartTime: model.ISOTime(start), IsRunning: running, DurationMs: duration, Status: "error",
			Tags: map[string]string{}, Warnings: []string{warning}, Events: []model.TraceSpanEvent{},
		}}
		roots = append(roots, parentID)
		warnings = append(warnings, warning)
	}
	sortIDs := func(ids []string) {
		sort.SliceStable(ids, func(i, j int) bool {
			left, right := nodes[ids[i]], nodes[ids[j]]
			if left.start != right.start {
				return left.start < right.start
			}
			if left.synthetic != right.synthetic {
				return !left.synthetic
			}
			return ids[i] < ids[j]
		})
	}
	sortIDs(roots)
	for parent := range children {
		sortIDs(children[parent])
	}
	ordered := make([]model.TraceSpan, 0, len(nodes))
	visited := make(map[string]bool, len(nodes))
	var walk func(string, int)
	walk = func(id string, depth int) {
		if visited[id] {
			return
		}
		visited[id] = true
		node := nodes[id]
		if node == nil {
			return
		}
		span := node.span
		span.Depth = depth
		ordered = append(ordered, span)
		for _, child := range children[id] {
			walk(child, depth+1)
		}
	}
	for _, root := range roots {
		walk(root, 0)
	}
	for _, raw := range dbSpans {
		walk(raw.spanID, 0)
	}
	summary := facts.summary(traceID, now)
	if !facts.hasExplicitRoot {
		summary.ServiceName = ordered[0].ServiceName
		summary.RootOperationName = ordered[0].OperationName
	}
	summary.SpanCount = len(ordered)
	summary.ErrorCount += len(ordered) - facts.spanCount
	summary.Warnings = warnings
	return &model.Trace{TraceSummary: summary, Spans: ordered}
}

func (s *Store) Cleanup(ctx context.Context, now time.Time) error {
	if err := s.Flush(ctx); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cutoff := now.Add(-time.Duration(s.config.RetentionHours) * time.Hour).UnixMilli()
	oversized, err := s.exceedsSizeLimit(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	traceIDs, err := selectStrings(ctx, tx, `SELECT trace_id FROM trace_summaries
		WHERE active_span_count=0 AND ended_at_ms>0 AND ended_at_ms<? ORDER BY ended_at_ms ASC LIMIT ?`, cutoff, s.config.RetentionTraceBatch)
	if err != nil {
		return err
	}
	if oversized {
		more, queryErr := selectStrings(ctx, tx, `SELECT trace_id FROM trace_summaries
			WHERE active_span_count=0 ORDER BY started_at_ms ASC LIMIT ?`, s.config.RetentionTraceBatch)
		if queryErr != nil {
			return queryErr
		}
		traceIDs = unique(append(traceIDs, more...))
	}
	changed := len(traceIDs) > 0
	for _, traceID := range traceIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM log_attributes WHERE log_id IN (SELECT id FROM logs WHERE trace_id=?)`, traceID); err != nil {
			return err
		}
		for _, statement := range []string{
			`DELETE FROM logs WHERE trace_id=?`, `DELETE FROM span_attributes WHERE trace_id=?`,
			`DELETE FROM spans WHERE trace_id=?`, `DELETE FROM trace_summaries WHERE trace_id=?`,
		} {
			if _, err := tx.ExecContext(ctx, statement, traceID); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM log_attributes WHERE log_id IN (
		SELECT id FROM logs WHERE timestamp_ms<? ORDER BY timestamp_ms ASC LIMIT ?)`, cutoff, s.config.RetentionLogBatch); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM logs WHERE id IN (
		SELECT id FROM logs WHERE timestamp_ms<? ORDER BY timestamp_ms ASC LIMIT ?)`, cutoff, s.config.RetentionLogBatch)
	if err != nil {
		return err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr == nil && affected > 0 {
		changed = true
	}
	if oversized {
		if _, err := tx.ExecContext(ctx, `DELETE FROM log_attributes WHERE log_id IN (
			SELECT id FROM logs ORDER BY timestamp_ms ASC LIMIT ?)`, s.config.RetentionLogBatch); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `DELETE FROM logs WHERE id IN (
			SELECT id FROM logs ORDER BY timestamp_ms ASC LIMIT ?)`, s.config.RetentionLogBatch)
		if err != nil {
			return err
		}
		if affected, affectedErr := result.RowsAffected(); affectedErr == nil && affected > 0 {
			changed = true
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if changed {
		_, err = s.db.ExecContext(ctx, `CHECKPOINT`)
		return err
	}
	return nil
}

func (s *Store) exceedsSizeLimit(ctx context.Context) (bool, error) {
	limit := int64(s.config.MaxDBSizeMB) * 1024 * 1024
	physicalSize := int64(0)
	for _, path := range []string{s.config.DatabasePath, s.config.DatabasePath + ".wal"} {
		if info, err := os.Stat(path); err == nil {
			physicalSize += info.Size()
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if physicalSize <= limit {
		return false, nil
	}
	if _, err := s.db.ExecContext(ctx, `CHECKPOINT`); err != nil {
		return false, err
	}
	var liveBytes int64
	if err := s.db.QueryRowContext(ctx, `SELECT coalesce(sum(used_blocks * block_size), 0) FROM pragma_database_size()`).Scan(&liveBytes); err != nil {
		return false, err
	}
	return liveBytes > limit, nil
}

func (s *Store) RunMaintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.config.RetentionIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_ = s.Cleanup(ctx, now)
		}
	}
}

func marshalObject(value map[string]string) (string, error) {
	encoded, err := json.Marshal(nonNilMap(value))
	return string(encoded), err
}

func decodeObject(value string) map[string]string {
	result := make(map[string]string)
	_ = json.Unmarshal([]byte(value), &result)
	return result
}

func merged(first, second map[string]string) map[string]string {
	result := make(map[string]string, len(first)+len(second))
	for key, value := range first {
		result[key] = value
	}
	for key, value := range second {
		result[key] = value
	}
	return result
}

func nonNilMap(value map[string]string) map[string]string {
	if value == nil {
		return map[string]string{}
	}
	return value
}

func cloneSpanRecords(records []model.SpanRecord) []model.SpanRecord {
	cloned := make([]model.SpanRecord, len(records))
	for i, record := range records {
		cloned[i] = record
		cloned[i].ParentSpanID = cloneStringPointer(record.ParentSpanID)
		cloned[i].ScopeName = cloneStringPointer(record.ScopeName)
		cloned[i].Kind = cloneStringPointer(record.Kind)
		cloned[i].Attributes = cloneStringMap(record.Attributes)
		cloned[i].Resource = cloneStringMap(record.Resource)
		if record.Events != nil {
			cloned[i].Events = make([]model.EventRecord, len(record.Events))
			for eventIndex, event := range record.Events {
				cloned[i].Events[eventIndex] = event
				cloned[i].Events[eventIndex].Attributes = cloneStringMap(event.Attributes)
			}
		}
	}
	return cloned
}

func cloneLogRecords(records []model.LogRecord) []model.LogRecord {
	cloned := make([]model.LogRecord, len(records))
	for i, record := range records {
		cloned[i] = record
		cloned[i].TraceID = cloneStringPointer(record.TraceID)
		cloned[i].SpanID = cloneStringPointer(record.SpanID)
		cloned[i].ScopeName = cloneStringPointer(record.ScopeName)
		cloned[i].Attributes = cloneStringMap(record.Attributes)
		cloned[i].Resource = cloneStringMap(record.Resource)
	}
	return cloned
}

func cloneStringMap(value map[string]string) map[string]string {
	if value == nil {
		return nil
	}
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func nullString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func prefix(value string, size int) string {
	if len(value) <= size {
		return value
	}
	return value[:size]
}

func selectStrings(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func unique(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func IsNotFound(err error) bool { return errors.Is(err, sql.ErrNoRows) }

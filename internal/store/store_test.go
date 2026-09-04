package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
)

func TestStoreIngestAndQuery(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "test.duckdb")
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	now := time.Now().Add(-time.Minute).UnixMilli()
	rootID, scope, kind := "root", "scope", "server"
	spans := []model.SpanRecord{
		{TraceID: "trace-1", SpanID: rootID, ServiceName: "api", ScopeName: &scope, Kind: &kind, OperationName: "GET /items", StartTimeMs: now, EndTimeMs: now + 100, DurationMs: 100, Status: "ok", Resource: map[string]string{"region": "west"}, Attributes: map[string]string{"http.method": "GET"}, Events: []model.EventRecord{}},
		{TraceID: "trace-1", SpanID: "child", ParentSpanID: &rootID, ServiceName: "db", OperationName: "select items", StartTimeMs: now + 10, EndTimeMs: now + 90, DurationMs: 80, Status: "error", Resource: map[string]string{}, Attributes: map[string]string{"db.system": "duckdb"}, Events: []model.EventRecord{}},
	}
	if inserted, err := telemetryStore.IngestSpans(context.Background(), spans); err != nil || inserted != 2 {
		t.Fatalf("ingest spans: inserted=%d err=%v", inserted, err)
	}
	mustFlush(t, telemetryStore)
	trace, err := telemetryStore.GetTrace(context.Background(), "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if trace == nil || trace.SpanCount != 2 || trace.ErrorCount != 1 || trace.Spans[1].Depth != 1 || trace.Spans[0].Tags["region"] != "west" {
		t.Fatalf("unexpected trace: %#v", trace)
	}

	logs := []model.LogRecord{{TraceID: stringPointer("trace-1"), SpanID: stringPointer("child"), ServiceName: "api", SeverityText: "Warn", Body: "database timeout", TimestampMs: now + 50, Attributes: map[string]string{"debug.session": "s1"}, Resource: map[string]string{"region": "west"}}}
	if inserted, err := telemetryStore.IngestLogs(context.Background(), logs); err != nil || inserted != 1 {
		t.Fatalf("ingest logs: inserted=%d err=%v", inserted, err)
	}
	mustFlush(t, telemetryStore)
	matchedLogs, err := telemetryStore.SearchLogs(context.Background(), LogFilter{Severity: "WARN", Body: "data time", Attributes: map[string]string{"debug.session": "s1"}, SinceMs: now - 1}, 10)
	if err != nil || len(matchedLogs) != 1 || matchedLogs[0].Attributes["region"] != "west" {
		t.Fatalf("search logs: %#v, %v", matchedLogs, err)
	}

	summaries, err := telemetryStore.ListTraceSummaries(context.Background(), TraceFilter{Operation: "select", Attributes: map[string]string{"db.system": "duckdb"}, SinceMs: now - 1}, 10)
	if err != nil || len(summaries) != 1 || summaries[0].TraceID != "trace-1" {
		t.Fatalf("search traces: %#v, %v", summaries, err)
	}
	spansFound, err := telemetryStore.SearchSpans(context.Background(), SpanFilter{ParentOperation: "GET /items", SinceMs: now - 1}, 10)
	if err != nil || len(spansFound) != 1 || spansFound[0].Span.SpanID != "child" {
		t.Fatalf("search spans: %#v, %v", spansFound, err)
	}
	spansFound, err = telemetryStore.SearchSpans(context.Background(), SpanFilter{
		Attributes: map[string]string{"db.system": "duckdb"}, AttributeContains: map[string]string{"db.system": "DUCK"}, SinceMs: now - 1,
	}, 10)
	if err != nil || len(spansFound) != 1 || spansFound[0].Span.SpanID != "child" {
		t.Fatalf("search spans with exact and contains filters on one key: %#v, %v", spansFound, err)
	}
	matchedLogs, err = telemetryStore.SearchLogs(context.Background(), LogFilter{
		Attributes: map[string]string{"debug.session": "s1"}, AttributeContains: map[string]string{"debug.session": "S"}, SinceMs: now - 1,
	}, 10)
	if err != nil || len(matchedLogs) != 1 {
		t.Fatalf("search logs with exact and contains filters on one key: %#v, %v", matchedLogs, err)
	}
}

func TestMissingParentCreatesSyntheticSpan(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "test.duckdb")
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()
	now := time.Now().UnixMilli()
	missing := "missing-id"
	_, err = telemetryStore.IngestSpans(context.Background(), []model.SpanRecord{{
		TraceID: "trace-orphan", SpanID: "child", ParentSpanID: &missing, ServiceName: "api", OperationName: "child",
		StartTimeMs: now, EndTimeMs: now + 5, DurationMs: 5, Status: "ok", Attributes: map[string]string{}, Resource: map[string]string{}, Events: []model.EventRecord{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, telemetryStore)
	trace, err := telemetryStore.GetTrace(context.Background(), "trace-orphan")
	if err != nil {
		t.Fatal(err)
	}
	if trace == nil || trace.SpanCount != 2 || trace.ErrorCount != 1 || len(trace.Warnings) != 1 || trace.Spans[0].SpanID != missing {
		t.Fatalf("unexpected trace: %#v", trace)
	}
	summaries, err := telemetryStore.ListTraceSummaries(context.Background(), TraceFilter{SinceMs: now - 1}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].ServiceName != "api" || summaries[0].RootOperationName != "child" ||
		summaries[0].SpanCount != 1 || summaries[0].ErrorCount != 0 || len(summaries[0].Warnings) != 0 {
		t.Fatalf("synthetic detail data leaked into persisted summary: %#v", summaries)
	}
}

func TestTraceSummaryUsesOneCoherentRoot(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "test.duckdb")
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	now := time.Now().Add(-time.Minute).UnixMilli()
	spans := []model.SpanRecord{
		{TraceID: "trace-roots", SpanID: "first", ServiceName: "z-service", OperationName: "a-operation", StartTimeMs: now, EndTimeMs: now + 10, DurationMs: 10, Status: "ok"},
		{TraceID: "trace-roots", SpanID: "second", ServiceName: "a-service", OperationName: "z-operation", StartTimeMs: now + 1, EndTimeMs: now + 20, DurationMs: 19, Status: "error"},
	}
	if _, err := telemetryStore.IngestSpans(context.Background(), spans); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, telemetryStore)
	detail, err := telemetryStore.GetTrace(context.Background(), "trace-roots")
	if err != nil {
		t.Fatal(err)
	}
	summaries, err := telemetryStore.ListTraceSummaries(context.Background(), TraceFilter{SinceMs: now - 1}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if detail == nil || len(summaries) != 1 {
		t.Fatalf("missing trace data: detail=%#v summaries=%#v", detail, summaries)
	}
	summary := summaries[0]
	if summary.ServiceName != "z-service" || summary.RootOperationName != "a-operation" ||
		detail.ServiceName != summary.ServiceName || detail.RootOperationName != summary.RootOperationName ||
		detail.StartedAt != summary.StartedAt || detail.IsRunning != summary.IsRunning ||
		detail.DurationMs != summary.DurationMs || detail.SpanCount != summary.SpanCount || detail.ErrorCount != summary.ErrorCount {
		t.Fatalf("list and detail summaries disagree: list=%#v detail=%#v", summary, detail.TraceSummary)
	}
}

func TestTraceWithoutExplicitRootUsesEarliestSpan(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "test.duckdb")
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	now := time.Now().Add(-time.Minute).UnixMilli()
	firstParent, secondParent := "second", "first"
	spans := []model.SpanRecord{
		{TraceID: "trace-cycle", SpanID: "first", ParentSpanID: &firstParent, ServiceName: "cycle-a", OperationName: "first operation", StartTimeMs: now, EndTimeMs: now + 10, DurationMs: 10, Status: "ok"},
		{TraceID: "trace-cycle", SpanID: "second", ParentSpanID: &secondParent, ServiceName: "cycle-b", OperationName: "second operation", StartTimeMs: now + 1, EndTimeMs: now + 9, DurationMs: 8, Status: "ok"},
	}
	if _, err := telemetryStore.IngestSpans(context.Background(), spans); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, telemetryStore)
	detail, err := telemetryStore.GetTrace(context.Background(), "trace-cycle")
	if err != nil {
		t.Fatal(err)
	}
	summaries, err := telemetryStore.ListTraceSummaries(context.Background(), TraceFilter{SinceMs: now - 1}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if detail == nil || len(summaries) != 1 || detail.ServiceName != "cycle-a" || detail.RootOperationName != "first operation" ||
		summaries[0].ServiceName != detail.ServiceName || summaries[0].RootOperationName != detail.RootOperationName {
		t.Fatalf("unexpected root fallback: detail=%#v summaries=%#v", detail, summaries)
	}
}

func TestIngestBatchesSpansAndLogsInOneWrite(t *testing.T) {
	if writeBatchInterval != 500*time.Millisecond {
		t.Fatalf("write interval=%s, want 500ms", writeBatchInterval)
	}
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "batch.duckdb")
	telemetryStore, err := openWithBatchInterval(cfg, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	now := time.Now().UnixMilli()
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("batch-%d", i)
		if _, err := telemetryStore.IngestSpans(context.Background(), []model.SpanRecord{{
			TraceID: id, SpanID: id, ServiceName: "batch", OperationName: "write", StartTimeMs: now,
			EndTimeMs: now + 1, DurationMs: 1, Status: "ok", Attributes: map[string]string{"index": fmt.Sprint(i)},
		}}); err != nil {
			t.Fatal(err)
		}
		if _, err := telemetryStore.IngestLogs(context.Background(), []model.LogRecord{{
			ServiceName: "batch", SeverityText: "INFO", Body: id, TimestampMs: now,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	assertTableCount(t, telemetryStore, "spans", 0)
	assertTableCount(t, telemetryStore, "logs", 0)
	mustFlush(t, telemetryStore)
	assertTableCount(t, telemetryStore, "spans", 25)
	assertTableCount(t, telemetryStore, "logs", 25)
	telemetryStore.pendingMu.Lock()
	batches := telemetryStore.committedBatches
	telemetryStore.pendingMu.Unlock()
	if batches != 1 {
		t.Fatalf("committed batches=%d, want 1", batches)
	}
}

func TestWriterFlushesOnInterval(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "interval.duckdb")
	telemetryStore, err := openWithBatchInterval(cfg, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()
	if _, err := telemetryStore.IngestLogs(context.Background(), []model.LogRecord{{
		ServiceName: "interval", SeverityText: "INFO", Body: "tick", TimestampMs: time.Now().UnixMilli(),
	}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		var count int
		if err := telemetryStore.db.QueryRow(`SELECT count(*) FROM logs`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not commit on its interval")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestIngestOwnsQueuedRecords(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "ownership.duckdb")
	telemetryStore, err := openWithBatchInterval(cfg, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	now := time.Now().UnixMilli()
	scope, kind := "original-scope", "server"
	span := model.SpanRecord{
		TraceID: "original-trace", SpanID: "original-span", ServiceName: "ownership", ScopeName: &scope, Kind: &kind,
		OperationName: "original-operation", StartTimeMs: now, EndTimeMs: now + 1, DurationMs: 1, Status: "ok",
		Attributes: map[string]string{"span-key": "original"}, Resource: map[string]string{"resource-key": "original"},
		Events: []model.EventRecord{{Name: "original-event", Timestamp: now, Attributes: map[string]string{"event-key": "original"}}},
	}
	traceID, spanID, logScope := span.TraceID, span.SpanID, "original-log-scope"
	record := model.LogRecord{
		TraceID: &traceID, SpanID: &spanID, ScopeName: &logScope, ServiceName: "ownership", SeverityText: "INFO",
		Body: "original-body", TimestampMs: now, Attributes: map[string]string{"log-key": "original"}, Resource: map[string]string{"resource-key": "original"},
	}
	if _, err := telemetryStore.IngestSpans(context.Background(), []model.SpanRecord{span}); err != nil {
		t.Fatal(err)
	}
	if _, err := telemetryStore.IngestLogs(context.Background(), []model.LogRecord{record}); err != nil {
		t.Fatal(err)
	}

	scope, kind = "mutated-scope", "client"
	span.Attributes["span-key"], span.Resource["resource-key"] = "mutated", "mutated"
	span.Events[0].Name, span.Events[0].Attributes["event-key"] = "mutated-event", "mutated"
	traceID, spanID, logScope = "mutated-trace", "mutated-span", "mutated-log-scope"
	record.Attributes["log-key"], record.Resource["resource-key"] = "mutated", "mutated"
	mustFlush(t, telemetryStore)

	trace, err := telemetryStore.GetTrace(context.Background(), "original-trace")
	if err != nil || trace == nil || len(trace.Spans) != 1 {
		t.Fatalf("queued trace missing: trace=%#v err=%v", trace, err)
	}
	storedSpan := trace.Spans[0]
	if storedSpan.ScopeName == nil || *storedSpan.ScopeName != "original-scope" || storedSpan.Kind == nil || *storedSpan.Kind != "server" ||
		storedSpan.Tags["span-key"] != "original" || storedSpan.Tags["resource-key"] != "original" ||
		len(storedSpan.Events) != 1 || storedSpan.Events[0].Name != "original-event" || storedSpan.Events[0].Attributes["event-key"] != "original" {
		t.Fatalf("queued span was mutated through caller-owned data: %#v", storedSpan)
	}
	logs, err := telemetryStore.SearchLogs(context.Background(), LogFilter{TraceID: "original-trace", SinceMs: now - 1}, 10)
	if err != nil || len(logs) != 1 || logs[0].SpanID == nil || *logs[0].SpanID != "original-span" ||
		logs[0].ScopeName == nil || *logs[0].ScopeName != "original-log-scope" ||
		logs[0].Attributes["log-key"] != "original" || logs[0].Attributes["resource-key"] != "original" {
		t.Fatalf("queued log was mutated through caller-owned data: logs=%#v err=%v", logs, err)
	}
}

func TestCloseFlushesAndRejectsFurtherIngest(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "close.duckdb")
	telemetryStore, err := openWithBatchInterval(cfg, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := telemetryStore.IngestSpans(context.Background(), []model.SpanRecord{{
		TraceID: "close", SpanID: "close", ServiceName: "close", OperationName: "flush",
		StartTimeMs: now, EndTimeMs: now + 1, DurationMs: 1, Status: "ok",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := telemetryStore.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := telemetryStore.IngestLogs(context.Background(), []model.LogRecord{{ServiceName: "closed"}}); !errors.Is(err, errStoreClosed) {
		t.Fatalf("ingest after close error=%v, want %v", err, errStoreClosed)
	}

	reopened, err := openWithBatchInterval(cfg, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	trace, err := reopened.GetTrace(context.Background(), "close")
	if err != nil || trace == nil {
		t.Fatalf("close did not flush trace: trace=%#v err=%v", trace, err)
	}
}

func TestConcurrentIngestFlushAndCleanup(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "concurrent.duckdb")
	telemetryStore, err := openWithBatchInterval(cfg, 2*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	const workers, recordsPerWorker = 8, 50
	start := make(chan struct{})
	errCh := make(chan error, workers+2)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for record := 0; record < recordsPerWorker; record++ {
				if _, err := telemetryStore.IngestLogs(context.Background(), []model.LogRecord{{
					ServiceName: "concurrent", SeverityText: "INFO", Body: fmt.Sprintf("%d-%d", worker, record), TimestampMs: time.Now().UnixMilli(),
				}}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		for i := 0; i < 20; i++ {
			if err := telemetryStore.Flush(context.Background()); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for i := 0; i < 10; i++ {
			if err := telemetryStore.Cleanup(context.Background(), time.Now()); err != nil {
				errCh <- err
				return
			}
		}
	}()
	close(start)
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	mustFlush(t, telemetryStore)
	assertTableCount(t, telemetryStore, "logs", workers*recordsPerWorker)
}

func mustFlush(t *testing.T, telemetryStore *Store) {
	t.Helper()
	if err := telemetryStore.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func assertTableCount(t *testing.T, telemetryStore *Store, table string, want int) {
	t.Helper()
	var count int
	if err := telemetryStore.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s count=%d, want %d", table, count, want)
	}
}

func stringPointer(value string) *string { return &value }

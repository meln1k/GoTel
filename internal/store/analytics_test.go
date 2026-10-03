package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"path/filepath"
	"testing"
	"time"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
)

func TestAIAnalyticsAndFacets(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "analytics.sqlite")
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()
	now := time.Now().Add(-time.Minute).UnixMilli()
	rootID := "ai-root"
	_, err = telemetryStore.IngestSpans(context.Background(), []model.SpanRecord{
		{
			TraceID: "trace-ai", SpanID: rootID, ServiceName: "assistant", OperationName: "ai.streamText",
			StartTimeMs: now, EndTimeMs: now + 20_000, DurationMs: 20_000, Status: "ok", Resource: map[string]string{"deployment": "local"},
			Attributes: map[string]string{
				"ai.telemetry.functionId": "chat", "ai.model.provider": "openai", "ai.model.id": "gpt-test",
				"ai.telemetry.metadata.sessionId": "session-1", "ai.prompt.messages": `[{"role":"user","content":"hello"}]`,
				"ai.response.text": "hello back", "ai.response.finishReason": "stop", "ai.usage.inputTokens": "10",
				"ai.usage.outputTokens": "5", "ai.usage.totalTokens": "15", "ai.response.msToFirstChunk": "25",
			}, Events: []model.EventRecord{},
		},
		{
			TraceID: "trace-ai", SpanID: "tool", ParentSpanID: &rootID, ServiceName: "assistant", OperationName: "ai.toolCall",
			StartTimeMs: now + 1_000, EndTimeMs: now + 2_000, DurationMs: 1_000, Status: "ok",
			Attributes: map[string]string{"ai.toolCall.name": "lookup"}, Resource: map[string]string{}, Events: []model.EventRecord{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, telemetryStore)

	calls, err := telemetryStore.SearchAICalls(context.Background(), AIFilter{Provider: "openai", Text: "hello", SinceMs: now - 1}, 10)
	if err != nil || len(calls) != 1 {
		t.Fatalf("search calls: %#v, %v", calls, err)
	}
	if calls[0].Operation != "streamText" || calls[0].ToolCallCount != 1 || calls[0].Usage.InputTokens == nil || *calls[0].Usage.InputTokens != 10 {
		t.Fatalf("unexpected call: %#v", calls[0])
	}
	detail, err := telemetryStore.GetAICall(context.Background(), rootID)
	if err != nil {
		t.Fatal(err)
	}
	if detail == nil || len(detail.ToolCalls) != 1 || detail.ToolCalls[0].Name != "lookup" || detail.PromptMessages == nil || detail.Timing.MsToFirstChunk == nil {
		t.Fatalf("unexpected detail: %#v", detail)
	}
	stats, err := telemetryStore.AIStats(context.Background(), "provider", "total_input_tokens", AIFilter{Provider: "openai", SinceMs: now - 1}, 10)
	if err != nil || len(stats) != 1 || stats[0].Group != "openai" || stats[0].Value != 10 {
		t.Fatalf("unexpected stats: %#v, %v", stats, err)
	}
	facets, err := telemetryStore.ListFacets(context.Background(), "traces", "attribute_values", "ai.model.provider", "", now-1, 10)
	if err != nil || len(facets) != 1 || facets[0].Value != "openai" {
		t.Fatalf("unexpected facets: %#v, %v", facets, err)
	}
}

func TestCleanupPreservesRunningTraces(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "retention.sqlite")
	cfg.RetentionHours = 1
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	_, err = telemetryStore.IngestSpans(context.Background(), []model.SpanRecord{
		{TraceID: "completed", SpanID: "completed", ServiceName: "api", OperationName: "done", StartTimeMs: old, EndTimeMs: old + 10, DurationMs: 10, Status: "ok", Attributes: map[string]string{}, Resource: map[string]string{}, Events: []model.EventRecord{}},
		{TraceID: "running", SpanID: "running", ServiceName: "api", OperationName: "active", StartTimeMs: old, EndTimeMs: 0, DurationMs: 0, Status: "ok", Attributes: map[string]string{}, Resource: map[string]string{}, Events: []model.EventRecord{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = telemetryStore.IngestLogs(context.Background(), []model.LogRecord{{ServiceName: "api", SeverityText: "INFO", Body: "old", TimestampMs: old, Attributes: map[string]string{}, Resource: map[string]string{}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := telemetryStore.Cleanup(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	completed, err := telemetryStore.GetTrace(context.Background(), "completed")
	if err != nil || completed != nil {
		t.Fatalf("completed trace was not removed: %#v, %v", completed, err)
	}
	running, err := telemetryStore.GetTrace(context.Background(), "running")
	if err != nil || running == nil || !running.IsRunning {
		t.Fatalf("running trace was removed: %#v, %v", running, err)
	}
	logs, err := telemetryStore.SearchLogs(context.Background(), LogFilter{SinceMs: 0}, 10)
	if err != nil || len(logs) != 0 {
		t.Fatalf("expired logs remain: %#v, %v", logs, err)
	}
}

func TestCleanupUsesLiveSize(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "size.sqlite")
	// This retention fixture deliberately stores multi-megabyte payloads.
	cfg.MaxBatchBytes = 32 << 20
	cfg.MaxDBSizeMB = 1
	cfg.RetentionHours = 24
	cfg.RetentionLogBatch = 1
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()

	randomBody := func() string {
		value := make([]byte, 1200*1024)
		if _, err := rand.Read(value); err != nil {
			t.Fatal(err)
		}
		return base64.StdEncoding.EncodeToString(value)
	}
	now := time.Now().UnixMilli()
	_, err = telemetryStore.IngestLogs(context.Background(), []model.LogRecord{
		{ServiceName: "api", SeverityText: "INFO", Body: randomBody(), TimestampMs: now - 1, Attributes: map[string]string{}, Resource: map[string]string{}},
		{ServiceName: "api", SeverityText: "INFO", Body: randomBody(), TimestampMs: now, Attributes: map[string]string{}, Resource: map[string]string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, telemetryStore)
	over, err := telemetryStore.exceedsSizeLimit(context.Background())
	if err != nil || !over {
		t.Fatalf("database should exceed the live-data cap: over=%v err=%v", over, err)
	}
	if err := telemetryStore.Cleanup(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	logs, err := telemetryStore.SearchLogs(context.Background(), LogFilter{SinceMs: 0}, 10)
	if err != nil || len(logs) != 1 || logs[0].Timestamp != model.ISOTime(now) {
		t.Fatalf("size cleanup did not evict exactly the oldest batch: count=%d err=%v", len(logs), err)
	}
}

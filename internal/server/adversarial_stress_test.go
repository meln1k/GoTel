//go:build stress

package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
	"github.com/meln1k/gotel/internal/store"
)

const (
	stressTraceCount = 192
	stressWorkers    = 8
	stressQueryCases = 22
)

// TestAdversarialHTTPAndStoreStress combines protocol, concurrency, retention,
// query-contract, pagination, aggregation, graph-shape, and persistence checks.
// It is deterministic so any failure can be reproduced with:
//
//	go test -tags=stress ./internal/server -run TestAdversarialHTTPAndStoreStress -count=1
func TestAdversarialHTTPAndStoreStress(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "adversarial.duckdb")
	cfg.RetentionHours = 24
	cfg.RetentionTraceBatch = 16
	cfg.RetentionLogBatch = 32
	cfg.MaxDBSizeMB = 1024

	telemetryStore, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(New(cfg, telemetryStore, Identity{DatabasePath: cfg.DatabasePath}).Handler())
	client := &http.Client{Timeout: 15 * time.Second}
	t.Cleanup(func() {
		if httpServer != nil {
			httpServer.Close()
		}
		if telemetryStore != nil {
			_ = telemetryStore.Close()
		}
	})

	base := time.Now().Add(-2 * time.Minute).UnixMilli()
	seedAdversarialGraphs(t, client, httpServer.URL, base)
	seedExpiredTelemetry(t, client, httpServer.URL)

	start := make(chan struct{})
	errors := make(chan error, 4096)
	report := func(err error) {
		if err != nil {
			errors <- err
		}
	}
	var coverage [stressQueryCases]atomic.Int64
	var wait sync.WaitGroup

	for worker := 0; worker < stressWorkers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for index := worker; index < stressTraceCount; index += stressWorkers {
				spans := stressSpans(index, base)
				logs := stressLogs(index, base)
				if index%2 == 0 {
					payload, encodeErr := jsonTracePayload(spans)
					report(encodeErr)
					if encodeErr == nil {
						report(postTelemetry(client, httpServer.URL+"/v1/traces", "application/json", payload, "insertedSpans", len(spans)))
					}
					payload, encodeErr = protobufLogPayload(logs)
					report(encodeErr)
					if encodeErr == nil {
						report(postTelemetry(client, httpServer.URL+"/v1/logs", "application/x-protobuf", payload, "insertedLogs", len(logs)))
					}
				} else {
					payload, encodeErr := protobufTracePayload(spans)
					report(encodeErr)
					if encodeErr == nil {
						report(postTelemetry(client, httpServer.URL+"/v1/traces", "application/x-protobuf", payload, "insertedSpans", len(spans)))
					}
					payload, encodeErr = jsonLogPayload(logs)
					report(encodeErr)
					if encodeErr == nil {
						report(postTelemetry(client, httpServer.URL+"/v1/logs", "application/json", payload, "insertedLogs", len(logs)))
					}
				}
			}
		}()
	}

	for worker := 0; worker < 6; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			random := rand.New(rand.NewSource(int64(7000 + worker)))
			for iteration := 0; iteration < stressQueryCases*4; iteration++ {
				caseIndex := (iteration + worker) % stressQueryCases
				coverage[caseIndex].Add(1)
				path, textResponse, allowNotFound := stressQuery(caseIndex, random.Intn(stressTraceCount))
				report(runStressQuery(client, httpServer.URL+path, textResponse, allowNotFound))
			}
		}()
	}

	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		for iteration := 0; iteration < 64; iteration++ {
			report(telemetryStore.Cleanup(context.Background(), time.Now()))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	close(start)
	wait.Wait()
	close(errors)
	stressErrors := make([]string, 0)
	for stressErr := range errors {
		if len(stressErrors) < 20 {
			stressErrors = append(stressErrors, stressErr.Error())
		}
	}
	if len(stressErrors) != 0 {
		t.Fatalf("concurrent stress failures:\n%s", strings.Join(stressErrors, "\n"))
	}
	for caseIndex := range coverage {
		if coverage[caseIndex].Load() == 0 {
			t.Fatalf("query case %d was not exercised", caseIndex)
		}
	}

	updated := stressSpans(0, base)[0]
	updated.OperationName = "updated root operation"
	updated.Attributes = map[string]string{"replacement": "true"}
	payload, err := jsonTracePayload([]model.SpanRecord{updated})
	if err != nil {
		t.Fatal(err)
	}
	if err := postTelemetry(client, httpServer.URL+"/v1/traces", "application/json", payload, "insertedSpans", 1); err != nil {
		t.Fatal(err)
	}
	if err := telemetryStore.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	runMalformedRequestMatrix(t, client, httpServer.URL)
	assertStressInvariants(t, client, httpServer.URL, telemetryStore)

	httpServer.Close()
	httpServer = nil
	if err := telemetryStore.Close(); err != nil {
		t.Fatal(err)
	}
	telemetryStore = nil

	reopened, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertPersistenceInvariants(t, reopened, base)
}

func stressSpans(index int, base int64) []model.SpanRecord {
	traceID := stressTraceID(index)
	rootID, workerID := stressSpanID(index, 1), stressSpanID(index, 2)
	aiID, toolID := stressSpanID(index, 3), stressSpanID(index, 4)
	service := fmt.Sprintf("stress-%d", index%4)
	start := base + int64(index*20)
	longValue := "edge ' % _ \\ unicode-λ-東京"
	if index%31 == 0 {
		longValue += strings.Repeat("x", 8192)
	}
	status := func(divisor int) string {
		if index%divisor == 0 {
			return "error"
		}
		return "ok"
	}
	resource := map[string]string{"stress.region": fmt.Sprintf("region-%d", index%3)}
	return []model.SpanRecord{
		{TraceID: traceID, SpanID: rootID, ServiceName: service, OperationName: fmt.Sprintf("root operation %d", index%7), StartTimeMs: start, EndTimeMs: start + 12, DurationMs: 12, Status: status(11), Resource: resource,
			Attributes: map[string]string{"stress.bucket": fmt.Sprintf("bucket-%d", index%5), "stress.edge": longValue}, Events: []model.EventRecord{{Name: "root-event", Timestamp: start + 1, Attributes: map[string]string{"event.kind": "start"}}}},
		{TraceID: traceID, SpanID: workerID, ParentSpanID: &rootID, ServiceName: service, OperationName: "worker child", StartTimeMs: start + 2, EndTimeMs: start + 10, DurationMs: 8, Status: status(13), Resource: resource,
			Attributes: map[string]string{"stress.bucket": fmt.Sprintf("bucket-%d", index%5), "search.text": fmt.Sprintf("needle-%d %s", index, longValue)}},
		{TraceID: traceID, SpanID: aiID, ParentSpanID: &rootID, ServiceName: service, OperationName: "ai.streamText", StartTimeMs: start + 3, EndTimeMs: start + 9, DurationMs: 6, Status: status(17), Resource: resource,
			Attributes: map[string]string{"ai.telemetry.functionId": fmt.Sprintf("function-%d", index%6), "ai.model.provider": fmt.Sprintf("provider-%d", index%3), "ai.model.id": fmt.Sprintf("model-%d", index%8), "ai.telemetry.metadata.sessionId": fmt.Sprintf("session-%d", index%9), "ai.prompt.messages": `[{"role":"user","content":"needle λ"}]`, "ai.response.text": "response 東京", "ai.response.finishReason": "stop", "ai.usage.inputTokens": strconv.Itoa(index%20 + 1), "ai.usage.outputTokens": strconv.Itoa(index%10 + 1)}},
		{TraceID: traceID, SpanID: toolID, ParentSpanID: &aiID, ServiceName: service, OperationName: "ai.toolCall", StartTimeMs: start + 4, EndTimeMs: start + 8, DurationMs: 4, Status: status(19), Resource: resource,
			Attributes: map[string]string{"ai.toolCall.name": fmt.Sprintf("tool-%d", index%5)}},
	}
}

func stressLogs(index int, base int64) []model.LogRecord {
	traceID, rootID, aiID := stressTraceID(index), stressSpanID(index, 1), stressSpanID(index, 3)
	service := fmt.Sprintf("stress-%d", index%4)
	start := base + int64(index*20)
	severity := []string{"INFO", "Warn", "ERROR"}[index%3]
	resource := map[string]string{"stress.region": fmt.Sprintf("region-%d", index%3)}
	return []model.LogRecord{
		{TraceID: &traceID, SpanID: &rootID, ServiceName: service, SeverityText: severity, Body: fmt.Sprintf("root log needle-%d λ %% _", index), TimestampMs: start + 13, Resource: resource, Attributes: map[string]string{"stress.bucket": fmt.Sprintf("bucket-%d", index%5)}},
		{TraceID: &traceID, SpanID: &aiID, ServiceName: service, SeverityText: "DEBUG", Body: "AI detail log 東京", TimestampMs: start + 14, Resource: resource, Attributes: map[string]string{"log.edge": "quote ' and slash \\"}},
	}
}

func stressTraceID(index int) string { return fmt.Sprintf("%032x", index+1000) }
func stressSpanID(index, offset int) string {
	return fmt.Sprintf("%016x", index*10+offset+1000)
}

func seedAdversarialGraphs(t *testing.T, client *http.Client, baseURL string, base int64) {
	t.Helper()
	multi := []model.SpanRecord{
		{TraceID: fmt.Sprintf("%032x", 1), SpanID: fmt.Sprintf("%016x", 1), ServiceName: "z-service", OperationName: "a-operation", StartTimeMs: base - 40, EndTimeMs: base - 30, Status: "ok"},
		{TraceID: fmt.Sprintf("%032x", 1), SpanID: fmt.Sprintf("%016x", 2), ServiceName: "a-service", OperationName: "z-operation", StartTimeMs: base - 39, EndTimeMs: base - 20, Status: "error"},
	}
	missingID := fmt.Sprintf("%016x", 99)
	orphan := []model.SpanRecord{{TraceID: fmt.Sprintf("%032x", 2), SpanID: fmt.Sprintf("%016x", 3), ParentSpanID: &missingID, ServiceName: "edge", OperationName: "orphan child", StartTimeMs: base - 30, EndTimeMs: base - 20, Status: "ok"}}
	firstID, secondID := fmt.Sprintf("%016x", 4), fmt.Sprintf("%016x", 5)
	cycle := []model.SpanRecord{
		{TraceID: fmt.Sprintf("%032x", 3), SpanID: firstID, ParentSpanID: &secondID, ServiceName: "cycle-a", OperationName: "cycle first", StartTimeMs: base - 20, EndTimeMs: base - 10, Status: "ok"},
		{TraceID: fmt.Sprintf("%032x", 3), SpanID: secondID, ParentSpanID: &firstID, ServiceName: "cycle-b", OperationName: "cycle second", StartTimeMs: base - 19, EndTimeMs: base - 11, Status: "ok"},
	}
	running := []model.SpanRecord{{TraceID: fmt.Sprintf("%032x", 4), SpanID: fmt.Sprintf("%016x", 6), ServiceName: "edge", OperationName: "running", StartTimeMs: base - 10, EndTimeMs: 0, Status: "ok"}}
	for _, spans := range [][]model.SpanRecord{multi, orphan, cycle, running} {
		payload, err := jsonTracePayload(spans)
		if err != nil {
			t.Fatal(err)
		}
		if err := postTelemetry(client, baseURL+"/v1/traces", "application/json", payload, "insertedSpans", len(spans)); err != nil {
			t.Fatal(err)
		}
	}
}

func seedExpiredTelemetry(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	old := time.Now().Add(-48 * time.Hour).UnixMilli()
	runningID := fmt.Sprintf("%032x", 10)
	spans := []model.SpanRecord{
		{TraceID: fmt.Sprintf("%032x", 9), SpanID: fmt.Sprintf("%016x", 9), ServiceName: "expired", OperationName: "completed", StartTimeMs: old, EndTimeMs: old + 10, Status: "ok"},
		{TraceID: runningID, SpanID: fmt.Sprintf("%016x", 10), ServiceName: "expired", OperationName: "still running", StartTimeMs: old, EndTimeMs: 0, Status: "ok"},
	}
	payload, err := jsonTracePayload(spans)
	if err != nil {
		t.Fatal(err)
	}
	if err := postTelemetry(client, baseURL+"/v1/traces", "application/json", payload, "insertedSpans", len(spans)); err != nil {
		t.Fatal(err)
	}
	logRecord := model.LogRecord{ServiceName: "expired", SeverityText: "INFO", Body: "remove me", TimestampMs: old}
	payload, err = jsonLogPayload([]model.LogRecord{logRecord})
	if err != nil {
		t.Fatal(err)
	}
	if err := postTelemetry(client, baseURL+"/v1/logs", "application/json", payload, "insertedLogs", 1); err != nil {
		t.Fatal(err)
	}
}

func stressQuery(caseIndex, index int) (path string, textResponse, allowNotFound bool) {
	traceID := stressTraceID(index)
	rootID, aiID := stressSpanID(index, 1), stressSpanID(index, 3)
	values := url.Values{}
	switch caseIndex {
	case 0:
		return "/", true, false
	case 1:
		return "/api/health", false, false
	case 2:
		return "/api/services", false, false
	case 3:
		values.Set("limit", "17junk")
		values.Set("lookback", "1d")
		return "/api/traces?" + values.Encode(), false, false
	case 4:
		values.Set("operation", "WORKER")
		values.Set("attr.stress.bucket", fmt.Sprintf("bucket-%d", index%5))
		values.Set("aiText", "needle λ")
		values.Set("lookback", "1d")
		return "/api/traces/search?" + values.Encode(), false, false
	case 5:
		values.Set("groupBy", "attr.stress.bucket")
		values.Set("agg", "p95_duration")
		values.Set("lookback", "1d")
		return "/api/traces/stats?" + values.Encode(), false, false
	case 6:
		return "/api/traces/" + traceID, false, true
	case 7:
		return "/api/traces/" + traceID + "/logs?lookback=1d&limit=9", false, true
	case 8:
		return "/api/traces/" + traceID + "/spans", false, true
	case 9:
		return "/api/spans/" + rootID, false, true
	case 10:
		return "/api/spans/" + rootID + "/logs?lookback=1d&limit=9", false, true
	case 11:
		values.Set("parentOperation", "root operation")
		values.Set("attrContains.search.text", "NEEDLE")
		values.Set("lookback", "1d")
		values.Set("limit", "31")
		return "/api/spans/search?" + values.Encode(), false, false
	case 12:
		return "/api/logs?lookback=1d&limit=23", false, false
	case 13:
		values.Set("body", "東京")
		values.Set("attrContains.log.edge", "QUOTE")
		values.Set("lookback", "1d")
		return "/api/logs/search?" + values.Encode(), false, false
	case 14:
		values.Set("groupBy", "attr.stress.bucket")
		values.Set("agg", "count")
		values.Set("lookback", "1d")
		return "/api/logs/stats?" + values.Encode(), false, false
	case 15:
		return "/api/docs", false, false
	case 16:
		return "/api/docs/debug", true, false
	case 17:
		return "/api/facets?type=traces&field=attribute_values&key=stress.bucket&lookback=1d&limit=20", false, false
	case 18:
		return "/api/ai/calls?text=needle+%CE%BB&lookback=1d&limit=37", false, false
	case 19:
		return "/api/ai/calls/" + aiID, false, true
	case 20:
		return "/api/ai/stats?groupBy=provider&agg=total_input_tokens&lookback=1d&limit=20", false, false
	default:
		return "/openapi.json", false, false
	}
}

func runStressQuery(client *http.Client, requestURL string, textResponse, allowNotFound bool) error {
	response, err := client.Get(requestURL)
	if err != nil {
		return fmt.Errorf("GET %s: %w", requestURL, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("GET %s body: %w", requestURL, err)
	}
	if response.StatusCode == http.StatusNotFound && allowNotFound {
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %d: %s", requestURL, response.StatusCode, body)
	}
	if textResponse {
		if len(body) == 0 || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/plain") {
			return fmt.Errorf("GET %s returned invalid text response", requestURL)
		}
		return nil
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return fmt.Errorf("GET %s returned invalid JSON: %w", requestURL, err)
	}
	return nil
}

func runMalformedRequestMatrix(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	tests := []struct {
		method, path, contentType string
		body                      []byte
		status                    int
	}{
		{http.MethodPost, "/v1/traces", "application/json", []byte(`{"resourceSpans":[`), http.StatusInternalServerError},
		{http.MethodPost, "/v1/logs", "application/x-protobuf", []byte{0xff, 0xff}, http.StatusInternalServerError},
		{http.MethodPost, "/v1/traces", "application/json", []byte(`{"resourceSpans":[]}`), http.StatusOK},
		{http.MethodGet, "/api/traces/stats?groupBy=service&agg=drop_table", "", nil, http.StatusBadRequest},
		{http.MethodGet, "/api/logs/stats?groupBy=service", "", nil, http.StatusBadRequest},
		{http.MethodGet, "/api/ai/stats?groupBy=invalid&agg=count", "", nil, http.StatusBadRequest},
		{http.MethodGet, "/api/facets?type=spans&field=service", "", nil, http.StatusBadRequest},
		{http.MethodGet, "/api/docs/unknown", "", nil, http.StatusNotFound},
		{http.MethodPost, "/api/services", "application/json", []byte(`{}`), http.StatusNotFound},
		{http.MethodGet, "/api/traces?cursor=not-base64&limit=999999999999999999999999999999&lookback=999999999999999999999d", "", nil, http.StatusOK},
		{http.MethodGet, "/api/logs?cursor=eyJraW5kIjoid3JvbmcifQ&lookback=1d", "", nil, http.StatusOK},
		{http.MethodGet, "/api/spans/search?attr.x%27%20OR%201%3D1--=value&lookback=1d", "", nil, http.StatusOK},
		{http.MethodGet, "/api/traces/stats?groupBy=attr.x%27%20OR%201%3D1--&agg=count&lookback=1d", "", nil, http.StatusOK},
		{http.MethodGet, "/definitely-absent", "", nil, http.StatusNotFound},
	}
	for _, test := range tests {
		request, err := http.NewRequest(test.method, baseURL+test.path, bytes.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		if test.contentType != "" {
			request.Header.Set("Content-Type", test.contentType)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", test.method, test.path, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != test.status {
			t.Errorf("%s %s returned %d, want %d: %s", test.method, test.path, response.StatusCode, test.status, body)
		}
		if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
			t.Errorf("%s %s returned content type %q", test.method, test.path, response.Header.Get("Content-Type"))
		}
		var decoded any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("%s %s returned invalid JSON: %v", test.method, test.path, err)
		}
	}
}

func assertStressInvariants(t *testing.T, client *http.Client, baseURL string, telemetryStore *store.Store) {
	t.Helper()
	expectedTraceCount := stressTraceCount + 4
	traces := fetchAllTraces(t, client, baseURL)
	if len(traces) != expectedTraceCount {
		t.Fatalf("paginated trace count=%d, want %d", len(traces), expectedTraceCount)
	}
	seen := make(map[string]bool, len(traces))
	for index, trace := range traces {
		if seen[trace.TraceID] {
			t.Fatalf("duplicate trace across cursor pages: %s", trace.TraceID)
		}
		seen[trace.TraceID] = true
		if index > 0 && trace.StartedAt > traces[index-1].StartedAt {
			t.Fatalf("trace order increased at %d: %s then %s", index, traces[index-1].StartedAt, trace.StartedAt)
		}
	}

	logs := fetchAllLogs(t, client, baseURL)
	if len(logs) != stressTraceCount*2 {
		t.Fatalf("paginated log count=%d, want %d", len(logs), stressTraceCount*2)
	}
	logIDs := make(map[string]bool, len(logs))
	for index, log := range logs {
		if logIDs[log.ID] {
			t.Fatalf("duplicate log across cursor pages: %s", log.ID)
		}
		logIDs[log.ID] = true
		if index > 0 && log.Timestamp > logs[index-1].Timestamp {
			t.Fatalf("log order increased at %d: %s then %s", index, logs[index-1].Timestamp, log.Timestamp)
		}
	}

	assertStatTotal(t, client, baseURL+"/api/traces/stats?groupBy=service&agg=count&lookback=1d&limit=100", expectedTraceCount)
	assertStatTotal(t, client, baseURL+"/api/logs/stats?groupBy=service&agg=count&lookback=1d&limit=100", stressTraceCount*2)
	assertFacetTotal(t, client, baseURL+"/api/facets?type=traces&field=service&lookback=1d&limit=100", expectedTraceCount)
	assertFacetTotal(t, client, baseURL+"/api/facets?type=logs&field=service&lookback=1d&limit=100", stressTraceCount*2)

	var spans struct {
		Data []model.SpanWithContext `json:"data"`
	}
	getStressJSON(t, client, baseURL+"/api/spans/search?operation=worker&attrContains.search.text=NEEDLE&lookback=1d&limit=500", &spans)
	if len(spans.Data) != stressTraceCount {
		t.Fatalf("span search count=%d, want %d", len(spans.Data), stressTraceCount)
	}
	var calls struct {
		Data []model.AICallSummary `json:"data"`
	}
	getStressJSON(t, client, baseURL+"/api/ai/calls?text=needle+%CE%BB&lookback=1d&limit=500", &calls)
	if len(calls.Data) != stressTraceCount {
		t.Fatalf("AI call count=%d, want %d", len(calls.Data), stressTraceCount)
	}
	for _, call := range calls.Data {
		if call.ToolCallCount != 1 {
			t.Fatalf("AI call %s has tool count %d", call.SpanID, call.ToolCallCount)
		}
	}

	var sample struct {
		Data model.Trace `json:"data"`
	}
	getStressJSON(t, client, baseURL+"/api/traces/"+stressTraceID(0), &sample)
	if sample.Data.RootOperationName != "updated root operation" || sample.Data.SpanCount != 4 || len(sample.Data.Spans) != 4 {
		t.Fatalf("upsert or trace graph invariant failed: %#v", sample.Data)
	}
	if sample.Data.Spans[3].Depth != 2 {
		t.Fatalf("tool span depth=%d, want 2", sample.Data.Spans[3].Depth)
	}

	var orphan struct {
		Data model.Trace `json:"data"`
	}
	getStressJSON(t, client, baseURL+"/api/traces/"+fmt.Sprintf("%032x", 2), &orphan)
	if orphan.Data.SpanCount != 2 || orphan.Data.ErrorCount != 1 || len(orphan.Data.Warnings) != 1 {
		t.Fatalf("orphan detail invariant failed: %#v", orphan.Data.TraceSummary)
	}
	if summary := findTrace(t, traces, fmt.Sprintf("%032x", 2)); summary.SpanCount != 1 || summary.ErrorCount != 0 || len(summary.Warnings) != 0 {
		t.Fatalf("synthetic data leaked into list summary: %#v", summary)
	}
	multi := findTrace(t, traces, fmt.Sprintf("%032x", 1))
	if multi.ServiceName != "z-service" || multi.RootOperationName != "a-operation" {
		t.Fatalf("multi-root summary is incoherent: %#v", multi)
	}

	completed, err := telemetryStore.GetTrace(context.Background(), fmt.Sprintf("%032x", 9))
	if err != nil || completed != nil {
		t.Fatalf("expired completed trace survived cleanup: trace=%#v err=%v", completed, err)
	}
	running, err := telemetryStore.GetTrace(context.Background(), fmt.Sprintf("%032x", 10))
	if err != nil || running == nil || !running.IsRunning {
		t.Fatalf("expired running trace did not survive cleanup: trace=%#v err=%v", running, err)
	}
	if running.DurationMs < float64((47 * time.Hour).Milliseconds()) {
		// Use a loose lower bound because the expired fixture and current dataset have different reference times.
		t.Fatalf("running duration was not rehydrated: %f", running.DurationMs)
	}
}

func assertPersistenceInvariants(t *testing.T, telemetryStore *store.Store, base int64) {
	t.Helper()
	traces, err := telemetryStore.ListTraceSummaries(context.Background(), store.TraceFilter{SinceMs: base - 100}, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != stressTraceCount+4 {
		t.Fatalf("reopened trace count=%d, want %d", len(traces), stressTraceCount+4)
	}
	logs, err := telemetryStore.SearchLogs(context.Background(), store.LogFilter{SinceMs: base - 100}, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != stressTraceCount*2 {
		t.Fatalf("reopened log count=%d, want %d", len(logs), stressTraceCount*2)
	}
	trace, err := telemetryStore.GetTrace(context.Background(), stressTraceID(0))
	if err != nil || trace == nil || trace.RootOperationName != "updated root operation" || trace.SpanCount != 4 {
		t.Fatalf("upsert did not persist: trace=%#v err=%v", trace, err)
	}
	completed, err := telemetryStore.GetTrace(context.Background(), fmt.Sprintf("%032x", 9))
	if err != nil || completed != nil {
		t.Fatalf("expired trace reappeared after reopen: trace=%#v err=%v", completed, err)
	}
	running, err := telemetryStore.GetTrace(context.Background(), fmt.Sprintf("%032x", 10))
	if err != nil || running == nil || !running.IsRunning {
		t.Fatalf("running trace was not persisted: trace=%#v err=%v", running, err)
	}
}

func fetchAllTraces(t *testing.T, client *http.Client, baseURL string) []model.TraceSummary {
	t.Helper()
	result := make([]model.TraceSummary, 0)
	cursor := ""
	for page := 0; page < 100; page++ {
		values := url.Values{"lookback": {"1d"}, "limit": {"13"}}
		if cursor != "" {
			values.Set("cursor", cursor)
		}
		var response struct {
			Data []model.TraceSummary `json:"data"`
			Meta model.ListMeta       `json:"meta"`
		}
		getStressJSON(t, client, baseURL+"/api/traces?"+values.Encode(), &response)
		result = append(result, response.Data...)
		if response.Meta.NextCursor == nil {
			return result
		}
		if *response.Meta.NextCursor == cursor {
			t.Fatal("trace cursor did not advance")
		}
		cursor = *response.Meta.NextCursor
	}
	t.Fatal("trace pagination did not terminate")
	return nil
}

func fetchAllLogs(t *testing.T, client *http.Client, baseURL string) []model.Log {
	t.Helper()
	result := make([]model.Log, 0)
	cursor := ""
	for page := 0; page < 100; page++ {
		values := url.Values{"lookback": {"1d"}, "limit": {"19"}}
		if cursor != "" {
			values.Set("cursor", cursor)
		}
		var response struct {
			Data []model.Log    `json:"data"`
			Meta model.ListMeta `json:"meta"`
		}
		getStressJSON(t, client, baseURL+"/api/logs?"+values.Encode(), &response)
		result = append(result, response.Data...)
		if response.Meta.NextCursor == nil {
			return result
		}
		if *response.Meta.NextCursor == cursor {
			t.Fatal("log cursor did not advance")
		}
		cursor = *response.Meta.NextCursor
	}
	t.Fatal("log pagination did not terminate")
	return nil
}

func assertStatTotal(t *testing.T, client *http.Client, requestURL string, expected int) {
	t.Helper()
	var response struct {
		Data []model.Stat `json:"data"`
	}
	getStressJSON(t, client, requestURL, &response)
	total := 0
	for _, item := range response.Data {
		total += item.Count
		if item.Value != float64(item.Count) {
			t.Fatalf("count stat value=%f count=%d", item.Value, item.Count)
		}
	}
	if total != expected {
		t.Fatalf("stat total=%d, want %d", total, expected)
	}
}

func assertFacetTotal(t *testing.T, client *http.Client, requestURL string, expected int) {
	t.Helper()
	var response struct {
		Data []model.Facet `json:"data"`
	}
	getStressJSON(t, client, requestURL, &response)
	total := 0
	for _, item := range response.Data {
		total += item.Count
	}
	if total != expected {
		t.Fatalf("facet total=%d, want %d", total, expected)
	}
}

func findTrace(t *testing.T, traces []model.TraceSummary, traceID string) model.TraceSummary {
	t.Helper()
	for _, trace := range traces {
		if trace.TraceID == traceID {
			return trace
		}
	}
	t.Fatalf("trace %s was not listed", traceID)
	return model.TraceSummary{}
}

func getStressJSON(t *testing.T, client *http.Client, requestURL string, target any) {
	t.Helper()
	response, err := client.Get(requestURL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		t.Fatalf("GET %s returned %d: %s", requestURL, response.StatusCode, body)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("GET %s: %v", requestURL, err)
	}
}

func postTelemetry(client *http.Client, requestURL, contentType string, payload []byte, countField string, expected int) error {
	request, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", contentType)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return fmt.Errorf("POST %s returned %d: %s", requestURL, response.StatusCode, body)
	}
	var result map[string]int
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return err
	}
	if result[countField] != expected {
		return fmt.Errorf("POST %s %s=%d, want %d", requestURL, countField, result[countField], expected)
	}
	return nil
}

func jsonTracePayload(spans []model.SpanRecord) ([]byte, error) {
	service := "unknown_service"
	resource := map[string]string{}
	if len(spans) != 0 {
		service = spans[0].ServiceName
		resource = spans[0].Resource
	}
	resourceValues := cloneStressMap(resource)
	resourceValues["service.name"] = service
	encodedSpans := make([]any, 0, len(spans))
	for _, span := range spans {
		value := map[string]any{
			"traceId": span.TraceID, "spanId": span.SpanID, "name": span.OperationName, "kind": 2,
			"startTimeUnixNano": strconv.FormatInt(span.StartTimeMs*1_000_000, 10),
			"endTimeUnixNano":   strconv.FormatInt(span.EndTimeMs*1_000_000, 10),
			"attributes":        jsonAttributes(span.Attributes),
		}
		if span.ParentSpanID != nil {
			value["parentSpanId"] = *span.ParentSpanID
		}
		if span.Status == "error" {
			value["status"] = map[string]any{"code": 2}
		}
		events := make([]any, 0, len(span.Events))
		for _, event := range span.Events {
			events = append(events, map[string]any{"name": event.Name, "timeUnixNano": strconv.FormatInt(event.Timestamp*1_000_000, 10), "attributes": jsonAttributes(event.Attributes)})
		}
		value["events"] = events
		encodedSpans = append(encodedSpans, value)
	}
	return json.Marshal(map[string]any{"resourceSpans": []any{map[string]any{
		"resource":   map[string]any{"attributes": jsonAttributes(resourceValues)},
		"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "stress-json"}, "spans": encodedSpans}},
	}}})
}

func jsonLogPayload(logs []model.LogRecord) ([]byte, error) {
	service := "unknown_service"
	resource := map[string]string{}
	if len(logs) != 0 {
		service = logs[0].ServiceName
		resource = logs[0].Resource
	}
	resourceValues := cloneStressMap(resource)
	resourceValues["service.name"] = service
	encodedLogs := make([]any, 0, len(logs))
	for _, log := range logs {
		value := map[string]any{
			"timeUnixNano": strconv.FormatInt(log.TimestampMs*1_000_000, 10), "severityText": log.SeverityText,
			"body": map[string]any{"stringValue": log.Body}, "attributes": jsonAttributes(log.Attributes),
		}
		if log.TraceID != nil {
			value["traceId"] = *log.TraceID
		}
		if log.SpanID != nil {
			value["spanId"] = *log.SpanID
		}
		encodedLogs = append(encodedLogs, value)
	}
	return json.Marshal(map[string]any{"resourceLogs": []any{map[string]any{
		"resource":  map[string]any{"attributes": jsonAttributes(resourceValues)},
		"scopeLogs": []any{map[string]any{"scope": map[string]any{"name": "stress-json"}, "logRecords": encodedLogs}},
	}}})
}

func protobufTracePayload(spans []model.SpanRecord) ([]byte, error) {
	service := "unknown_service"
	resource := map[string]string{}
	if len(spans) != 0 {
		service = spans[0].ServiceName
		resource = spans[0].Resource
	}
	resourceValues := cloneStressMap(resource)
	resourceValues["service.name"] = service
	encodedSpans := make([]*tracev1.Span, 0, len(spans))
	for _, span := range spans {
		traceID, err := hex.DecodeString(span.TraceID)
		if err != nil {
			return nil, err
		}
		spanID, err := hex.DecodeString(span.SpanID)
		if err != nil {
			return nil, err
		}
		value := &tracev1.Span{TraceId: traceID, SpanId: spanID, Name: span.OperationName, Kind: tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(span.StartTimeMs) * 1_000_000, EndTimeUnixNano: uint64(span.EndTimeMs) * 1_000_000,
			Attributes: protobufAttributes(span.Attributes)}
		if span.ParentSpanID != nil {
			value.ParentSpanId, err = hex.DecodeString(*span.ParentSpanID)
			if err != nil {
				return nil, err
			}
		}
		if span.Status == "error" {
			value.Status = &tracev1.Status{Code: tracev1.Status_STATUS_CODE_ERROR}
		}
		for _, event := range span.Events {
			value.Events = append(value.Events, &tracev1.Span_Event{Name: event.Name, TimeUnixNano: uint64(event.Timestamp) * 1_000_000, Attributes: protobufAttributes(event.Attributes)})
		}
		encodedSpans = append(encodedSpans, value)
	}
	request := &collectortracev1.ExportTraceServiceRequest{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource:   &resourcev1.Resource{Attributes: protobufAttributes(resourceValues)},
		ScopeSpans: []*tracev1.ScopeSpans{{Scope: &commonv1.InstrumentationScope{Name: "stress-protobuf"}, Spans: encodedSpans}},
	}}}
	return proto.Marshal(request)
}

func protobufLogPayload(logs []model.LogRecord) ([]byte, error) {
	service := "unknown_service"
	resource := map[string]string{}
	if len(logs) != 0 {
		service = logs[0].ServiceName
		resource = logs[0].Resource
	}
	resourceValues := cloneStressMap(resource)
	resourceValues["service.name"] = service
	encodedLogs := make([]*logsv1.LogRecord, 0, len(logs))
	for _, log := range logs {
		value := &logsv1.LogRecord{TimeUnixNano: uint64(log.TimestampMs) * 1_000_000, SeverityText: log.SeverityText,
			Body: stringValue(log.Body), Attributes: protobufAttributes(log.Attributes)}
		var err error
		if log.TraceID != nil {
			value.TraceId, err = hex.DecodeString(*log.TraceID)
			if err != nil {
				return nil, err
			}
		}
		if log.SpanID != nil {
			value.SpanId, err = hex.DecodeString(*log.SpanID)
			if err != nil {
				return nil, err
			}
		}
		encodedLogs = append(encodedLogs, value)
	}
	request := &collectorlogsv1.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource:  &resourcev1.Resource{Attributes: protobufAttributes(resourceValues)},
		ScopeLogs: []*logsv1.ScopeLogs{{Scope: &commonv1.InstrumentationScope{Name: "stress-protobuf"}, LogRecords: encodedLogs}},
	}}}
	return proto.Marshal(request)
}

func jsonAttributes(values map[string]string) []any {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]any, 0, len(keys))
	for _, key := range keys {
		result = append(result, map[string]any{"key": key, "value": map[string]any{"stringValue": values[key]}})
	}
	return result
}

func protobufAttributes(values map[string]string) []*commonv1.KeyValue {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*commonv1.KeyValue, 0, len(keys))
	for _, key := range keys {
		result = append(result, &commonv1.KeyValue{Key: key, Value: stringValue(values[key])})
	}
	return result
}

func stringValue(value string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: value}}
}

func cloneStressMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meln1k/gotel/internal/api"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/store"
)

func TestHTTPIngestAndQuery(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "http.duckdb")
	telemetryStore, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer telemetryStore.Close()
	handler := New(cfg, telemetryStore, Identity{PID: 42, URL: cfg.BaseURL, Workdir: "/tmp/test", StartedAt: time.Now().UTC().Format(time.RFC3339Nano), InstanceID: "instance", DatabasePath: cfg.DatabasePath}).Handler()

	now := time.Now().Add(-time.Minute).UnixNano()
	payload := map[string]any{
		"resourceSpans": []any{
			map[string]any{
				"resource": map[string]any{"attributes": []any{
					map[string]any{"key": "service.name", "value": map[string]any{"stringValue": "api"}},
				}},
				"scopeSpans": []any{
					map[string]any{"spans": []any{
						map[string]any{
							"traceId": "trace-http", "spanId": "span-http", "name": "request",
							"startTimeUnixNano": now, "endTimeUnixNano": now + int64(time.Millisecond),
						},
					}},
				},
			},
		},
	}
	encoded, _ := json.Marshal(payload)
	request := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.String() != "{\"insertedSpans\":1}\n" {
		t.Fatalf("ingest response: %d %s", recorder.Code, recorder.Body.String())
	}
	if err := telemetryStore.Flush(request.Context()); err != nil {
		t.Fatal(err)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/traces?service=api&lookback=1h", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("query response: %d %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []struct {
			TraceID string `json:"traceId"`
		} `json:"data"`
		Meta struct {
			Lookback string `json:"lookback"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data) != 1 || response.Data[0].TraceID != "trace-http" || response.Meta.Lookback != "1h" {
		t.Fatalf("unexpected query response: %s", recorder.Body.String())
	}

	for _, path := range []string{"/openapi.json", "/api/docs", "/api/docs/debug"} {
		request = httptest.NewRequest(http.MethodGet, path, nil)
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, recorder.Code, recorder.Body.String())
		}
		if path == "/api/docs/debug" && recorder.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Fatalf("%s content type: %q", path, recorder.Header().Get("Content-Type"))
		}
	}
	for _, path := range []string{"/trace/trace-http", "/docs", "/ui", "/tui"} {
		request = httptest.NewRequest(http.MethodGet, path, nil)
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("UI route %s should be absent, got %d", path, recorder.Code)
		}
	}
}

func TestOpenAPIContract(t *testing.T) {
	spec := openAPISpec()
	info := spec["info"].(map[string]any)
	if spec["openapi"] != "3.1.0" || info["title"] != "Gotel Telemetry API" || info["version"] != "1.0.0" {
		t.Fatalf("unexpected API metadata: %#v", spec)
	}
	paths := spec["paths"].(map[string]any)
	if len(paths) != len(api.Endpoints()) {
		t.Fatalf("got %d documented paths, want %d", len(paths), len(api.Endpoints()))
	}
	for _, endpoint := range api.Endpoints() {
		path, ok := paths[endpoint.Path].(map[string]any)
		if !ok {
			t.Errorf("route %s is missing from OpenAPI", endpoint.Path)
			continue
		}
		operation, ok := path[strings.ToLower(endpoint.Method)].(map[string]any)
		if !ok || operation["operationId"] != string(endpoint.Operation) {
			t.Errorf("route %s is missing operation %s", endpoint.Pattern(), endpoint.Operation)
		}
	}
	if paths["/openapi.json"] == nil {
		t.Fatal("OpenAPI endpoint does not document itself")
	}
	stats := paths["/api/traces/stats"].(map[string]any)["get"].(map[string]any)
	if stats["operationId"] != "traceStats" {
		t.Fatalf("unexpected operation: %#v", stats)
	}
	parameters := stats["parameters"].([]map[string]any)
	if parameters[0]["name"] != "groupBy" || parameters[0]["required"] != true || parameters[1]["name"] != "agg" || parameters[1]["required"] != true {
		t.Fatalf("required parameters are missing: %#v", parameters)
	}
	trace := paths["/api/traces/{traceId}"].(map[string]any)["get"].(map[string]any)
	traceResponses := trace["responses"].(map[string]any)
	if traceResponses["200"] == nil || traceResponses["500"] == nil {
		t.Fatalf("trace response schemas are missing: %#v", traceResponses)
	}
	logSearch := paths["/api/logs/search"].(map[string]any)["get"].(map[string]any)
	dynamicParameters := make(map[string]string)
	for _, parameter := range logSearch["parameters"].([]map[string]any) {
		if prefix, ok := parameter["x-query-prefix"].(string); ok {
			dynamicParameters[parameter["name"].(string)] = prefix
		}
	}
	if dynamicParameters["attr.<key>"] != "attr." || dynamicParameters["attrContains.<key>"] != "attrContains." {
		t.Fatalf("dynamic attribute parameters are missing: %#v", dynamicParameters)
	}
	components := spec["components"].(map[string]any)["schemas"].(map[string]any)
	for _, name := range []string{"Trace", "TraceSpan", "Log", "AiCallSummary", "AiCallDetail", "ListMeta"} {
		if components[name] == nil {
			t.Errorf("missing component schema %q", name)
		}
	}
}

func TestRouterMatchesEndpointContract(t *testing.T) {
	server := New(config.Load(), nil, Identity{})
	mux, ok := server.Handler().(*http.ServeMux)
	if !ok {
		t.Fatalf("unexpected router type %T", server.Handler())
	}
	for _, endpoint := range api.Endpoints() {
		path := strings.NewReplacer("{traceId}", "trace", "{spanId}", "span", "{name}", "debug").Replace(endpoint.Path)
		request := httptest.NewRequest(endpoint.Method, path, nil)
		_, pattern := mux.Handler(request)
		if pattern != endpoint.Pattern() {
			t.Errorf("route %s matched %q", endpoint.Pattern(), pattern)
		}
	}
}

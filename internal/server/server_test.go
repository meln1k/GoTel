package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/meln1k/gotel/internal/api"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/store"
)

func TestHTTPIngestAndQuery(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "http.sqlite")
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
	if got := recorder.Header().Get("X-GoTel-Acknowledgment"); got != "accepted-not-persisted" {
		t.Fatalf("acknowledgment semantics not explicit: %q", got)
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
	for _, path := range []string{"/v1/traces", "/v1/logs"} {
		operation := paths[path].(map[string]any)["post"].(map[string]any)
		for _, code := range []string{"200", "400", "413", "503", "500"} {
			response := operation["responses"].(map[string]any)[code].(map[string]any)
			if response["content"].(map[string]any)["application/x-protobuf"] == nil {
				t.Fatalf("missing OTLP protobuf contract: %s %s", path, code)
			}
		}
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

func openHTTPTestStore(t *testing.T, cfg config.Config) *store.Store {
	t.Helper()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "http.sqlite")
	s, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func decodeOTLPStatus(t *testing.T, recorder *httptest.ResponseRecorder, protobuf bool) *statuspb.Status {
	t.Helper()
	status := &statuspb.Status{}
	var err error
	if protobuf {
		err = proto.Unmarshal(recorder.Body.Bytes(), status)
	} else {
		err = protojson.Unmarshal(recorder.Body.Bytes(), status)
	}
	if err != nil {
		t.Fatalf("invalid OTLP status: %v", err)
	}
	return status
}

func TestIngestBodyByteBoundary(t *testing.T) {
	cfg := config.Load()
	cfg.MaxRequestBytes = 2
	handler := New(cfg, openHTTPTestStore(t, cfg), Identity{}).Handler()
	for _, path := range []string{"/v1/traces", "/v1/logs"} {
		for _, unknownLength := range []bool{false, true} {
			for _, body := range []string{"{}", "{} "} {
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				if unknownLength {
					request.ContentLength = -1
					request.TransferEncoding = []string{"chunked"}
				}
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				want := http.StatusOK
				if len(body) > cfg.MaxRequestBytes {
					want = http.StatusRequestEntityTooLarge
				}
				if recorder.Code != want {
					t.Fatalf("%s chunked=%t body=%q: %d %s", path, unknownLength, body, recorder.Code, recorder.Body.String())
				}
			}
		}
	}
	// Exercise actual chunked framing over a network connection, not just request metadata.
	server := httptest.NewServer(handler)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/logs", io.NopCloser(strings.NewReader("{} ")))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("actual chunked request: %d", response.StatusCode)
	}
}

type blockedBody struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockedBody) Read([]byte) (int, error) {
	close(b.started)
	<-b.release
	return 0, io.EOF
}
func (*blockedBody) Close() error { return nil }

type countedBody struct{ reads atomic.Int32 }

func (b *countedBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (*countedBody) Close() error               { return nil }

func TestIngestSemaphoreSharedBeforeReading(t *testing.T) {
	for _, firstPath := range []string{"/v1/traces", "/v1/logs"} {
		t.Run(firstPath, func(t *testing.T) {
			cfg := config.Load()
			cfg.MaxConcurrentIngest = 1
			server := New(cfg, nil, Identity{})
			handler := server.Handler()
			body := &blockedBody{started: make(chan struct{}), release: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				defer close(done)
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, firstPath, body))
			}()
			defer func() { close(body.release); <-done }()
			select {
			case <-body.started:
			case <-time.After(time.Second):
				t.Fatal("first handler did not start reading")
			}
			for _, path := range []string{"/v1/traces", "/v1/logs"} {
				for _, protobuf := range []bool{false, true} {
					unread := &countedBody{}
					request := httptest.NewRequest(http.MethodPost, path, unread)
					if protobuf {
						request.Header.Set("Content-Type", "application/x-protobuf")
					}
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, request)
					if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") == "" || unread.reads.Load() != 0 {
						t.Fatalf("admission did not reject before reading: %d reads=%d", recorder.Code, unread.reads.Load())
					}
					if status := decodeOTLPStatus(t, recorder, protobuf); status.Code != 8 {
						t.Fatalf("overload status: %v", status)
					}
				}
			}
			if rejected := server.rejectedRequests.Load(); rejected != 4 {
				t.Fatalf("HTTP overload rejections not counted: %d", rejected)
			}
		})
	}
}

func TestIngestSlotHeldThroughAdmission(t *testing.T) {
	cfg := config.Load()
	cfg.MaxConcurrentIngest = 1
	s := New(cfg, nil, Identity{})
	admitting, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	recorder := httptest.NewRecorder()
	go func() {
		defer close(done)
		s.ingest(recorder, httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("{}")), "insertedSpans", func([]byte, bool) (int, error) {
			close(admitting)
			<-release
			return 7, nil // Acceptance does not wait for persistence.
		})
	}()
	select {
	case <-admitting:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("admission did not start")
	}
	other := httptest.NewRecorder()
	s.Handler().ServeHTTP(other, httptest.NewRequest(http.MethodPost, "/v1/logs", nil))
	close(release)
	<-done
	if other.Code != http.StatusServiceUnavailable {
		t.Fatalf("slot released before admission: %d", other.Code)
	}
	var result map[string]int
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil || result["insertedSpans"] != 7 || recorder.Header().Get("X-GoTel-Acknowledgment") != "accepted-not-persisted" {
		t.Fatalf("in-memory acceptance response: %s (%v)", recorder.Body.String(), err)
	}
}

func TestIngestErrorMappingAndSanitization(t *testing.T) {
	for _, test := range []struct {
		err        error
		httpStatus int
		code       int32
	}{
		{store.ErrOverloaded, 503, 8}, {store.ErrClosed, 503, 8},
		{store.ErrRecordTooLarge, 413, 8}, {store.ErrInvalidRecord, 400, 3},
		{errInvalidPayload, 400, 3}, {errors.New("secret DB payload"), 500, 13},
	} {
		for _, protobuf := range []bool{false, true} {
			request := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
			if protobuf {
				request.Header.Set("Content-Type", "application/protobuf")
			}
			recorder := httptest.NewRecorder()
			writeIngestError(recorder, request, fmt.Errorf("secret payload: %w", test.err))
			status := decodeOTLPStatus(t, recorder, protobuf)
			if recorder.Code != test.httpStatus || status.Code != test.code || strings.Contains(status.Message, "secret") {
				t.Fatalf("mapping %v: HTTP=%d status=%v", test.err, recorder.Code, status)
			}
			if test.httpStatus == 503 && recorder.Header().Get("Retry-After") == "" {
				t.Fatal("retry hint missing")
			}
		}
	}
}

func TestOTLPFormatsAndStoreOverload(t *testing.T) {
	cfg := config.Load()
	cfg.MaxOutstandingRecords, cfg.MaxBatchRecords = 1, 1
	s := openHTTPTestStore(t, cfg)
	handler := New(cfg, s, Identity{}).Handler()
	for _, protobuf := range []bool{false, true} {
		body := []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{},{}]}]}]}`)
		if protobuf {
			body, _ = proto.Marshal(&collectorlogsv1.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{{}, {}}}}}}})
		}
		request := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
		if protobuf {
			request.Header.Set("Content-Type", "application/x-protobuf")
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != 413 || decodeOTLPStatus(t, recorder, protobuf).Code != 8 {
			t.Fatalf("request cannot fit even an empty store: %d %s", recorder.Code, recorder.Body.String())
		}
	}
	for _, path := range []string{"/v1/traces", "/v1/logs"} {
		for _, protobuf := range []bool{false, true} {
			body := []byte(`{"private":"secret",`)
			if protobuf {
				body = []byte{0xff}
			}
			request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			if protobuf {
				request.Header.Set("Content-Type", "application/x-protobuf")
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != 400 || decodeOTLPStatus(t, recorder, protobuf).Code != 3 || strings.Contains(recorder.Body.String(), "secret") {
				t.Fatalf("malformed request: %d %s", recorder.Code, recorder.Body.String())
			}
		}
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(nil))
		request.Header.Set("Content-Type", "application/x-protobuf")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		var response proto.Message = &collectortracev1.ExportTraceServiceResponse{}
		if path == "/v1/logs" {
			response = &collectorlogsv1.ExportLogsServiceResponse{}
		}
		if err := proto.Unmarshal(recorder.Body.Bytes(), response); err != nil || recorder.Code != 200 || recorder.Header().Get("Content-Type") != "application/x-protobuf" {
			t.Fatalf("invalid protobuf success: %d %v", recorder.Code, err)
		}
	}
}

func TestHealthLivenessDistinctFromReadiness(t *testing.T) {
	cfg := config.Load()
	s := openHTTPTestStore(t, cfg)
	handler := New(cfg, s, Identity{PID: 42, InstanceID: "alive"}).Handler()
	for _, ready := range []bool{true, false} {
		if !ready {
			s.StopAdmission()
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/health", nil))
		var health struct {
			OK, Ready   bool
			PID         int
			Persistence struct{ Ready bool }
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &health); err != nil || recorder.Code != 200 || !health.OK || health.Ready != ready || health.Persistence.Ready != ready || health.PID != 42 {
			t.Fatalf("liveness/readiness: %s (%v)", recorder.Body.String(), err)
		}
	}
}

func TestRunCancellationStopsAdmission(t *testing.T) {
	cfg := config.Load()
	cfg.Host, cfg.Port = "127.0.0.1", 0
	s := openHTTPTestStore(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New(cfg, s, Identity{}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Ready {
		t.Fatal("canceled server left admission open")
	}
}

func TestRunCancellationPreventsLateAdmission(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Load()
	cfg.Host, cfg.Port = "127.0.0.1", listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	s := openHTTPTestStore(t, cfg)
	server := New(cfg, s, Identity{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	address := net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port))
	var conn net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(6 * time.Second))
	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"late"}}]}]}]}`
	fmt.Fprintf(conn, "POST /v1/logs HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n{", len(body))
	deadline = time.Now().Add(time.Second)
	for len(server.ingestSlots) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(server.ingestSlots) != 1 {
		t.Fatal("handler did not start reading incomplete body")
	}
	cancel()
	deadline = time.Now().Add(time.Second)
	for s.Stats().Ready && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Stats().Ready {
		t.Fatal("shutdown did not stop admission immediately")
	}
	if _, err := io.WriteString(conn, body[1:]); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("late request accepted: %d", response.StatusCode)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("HTTP shutdown did not finish within its budget")
	}
}

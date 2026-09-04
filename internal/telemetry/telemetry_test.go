package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/meln1k/gotel/internal/config"
)

func TestSetupExportsSpans(t *testing.T) {
	requests := make(chan *collectortracev1.ExportTraceServiceRequest, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var request collectortracev1.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- &request
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()

	cfg := config.Load()
	cfg.Enabled = true
	cfg.ServiceName = "gotel-test"
	cfg.TelemetryURL = collector.URL + "/v1/traces"
	shutdown, err := Setup(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "test-span")
	span.End()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-requests:
		if len(request.ResourceSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans) != 1 || request.ResourceSpans[0].ScopeSpans[0].Spans[0].Name != "test-span" {
			t.Fatalf("unexpected export: %#v", request)
		}
		attributes := request.ResourceSpans[0].Resource.Attributes
		foundService := false
		for _, item := range attributes {
			if item.Key == "service.name" && item.Value.GetStringValue() == "gotel-test" {
				foundService = true
			}
		}
		if !foundService {
			t.Fatalf("service.name missing from export: %#v", attributes)
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not receive an export")
	}
}

func TestHTTPHandlerAvoidsSelfIngestFeedback(t *testing.T) {
	cfg := config.Load()
	cfg.Enabled = true
	cfg.BaseURL = "http://127.0.0.1:27686"
	cfg.TelemetryURL = "http://localhost:27686/v1/traces"
	if shouldInstrumentHTTP(cfg, "POST /v1/traces") {
		t.Fatal("self-exporting trace ingestion must not be instrumented")
	}
	if !shouldInstrumentHTTP(cfg, "GET /api/health") {
		t.Fatal("non-ingest route was not instrumented")
	}
}

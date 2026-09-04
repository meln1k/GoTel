package telemetry

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/meln1k/gotel/internal/config"
)

type Shutdown func(context.Context) error

func Setup(ctx context.Context, cfg config.Config) (Shutdown, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	if strings.TrimSpace(cfg.TelemetryURL) == "" {
		return nil, fmt.Errorf("self-telemetry is enabled but no OTLP trace endpoint is configured")
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(cfg.TelemetryURL))
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", config.Version),
		)),
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(5*time.Second)),
	)
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return func(shutdownContext context.Context) error {
		err := provider.Shutdown(shutdownContext)
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		return err
	}, nil
}

func HTTPHandler(cfg config.Config, pattern string, next http.Handler) http.Handler {
	if !shouldInstrumentHTTP(cfg, pattern) {
		return next
	}
	return otelhttp.NewHandler(next, pattern)
}

func shouldInstrumentHTTP(cfg config.Config, pattern string) bool {
	return cfg.Enabled && !(isIngestPattern(pattern) && exportsToSelf(cfg))
}

func isIngestPattern(pattern string) bool {
	return pattern == "POST /v1/traces" || pattern == "POST /v1/logs"
}

func exportsToSelf(cfg config.Config) bool {
	collector, collectorErr := url.Parse(cfg.TelemetryURL)
	server, serverErr := url.Parse(cfg.BaseURL + "/v1/traces")
	if collectorErr != nil || serverErr != nil || strings.TrimRight(collector.Path, "/") != strings.TrimRight(server.Path, "/") {
		return false
	}
	if effectivePort(collector) != effectivePort(server) {
		return false
	}
	left, right := strings.ToLower(collector.Hostname()), strings.ToLower(server.Hostname())
	return left == right || (localHost(left) && localHost(right))
}

func effectivePort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if value.Scheme == "https" {
		return "443"
	}
	return "80"
}

func localHost(value string) bool {
	if value == "localhost" || value == "0.0.0.0" || value == "::" {
		return true
	}
	ip := net.ParseIP(value)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

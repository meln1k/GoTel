package config

import (
	"path/filepath"
	"testing"
)

func TestDatabasePath(t *testing.T) {
	t.Setenv("GOTEL_RUNTIME_DIR", t.TempDir())
	t.Setenv("GOTEL_OTEL_DB_PATH", "")
	cfg := Load()
	if err := cfg.ValidateIngestion(); err != nil || cfg.DatabasePath != filepath.Join(cfg.StateDir, "telemetry.sqlite") {
		t.Fatalf("path=%q err=%v", cfg.DatabasePath, err)
	}
	t.Setenv("GOTEL_OTEL_DB_PATH", "/tmp/custom.db")
	if got := Load().DatabasePath; got != "/tmp/custom.db" {
		t.Fatalf("custom path ignored: %q", got)
	}
}

func TestLoadURLPrecedenceAndOverrides(t *testing.T) {
	t.Setenv("GOTEL_RUNTIME_DIR", t.TempDir())
	t.Setenv("GOTEL_OTEL_BASE_URL", "http://collector.example:4318/custom")
	t.Setenv("GOTEL_OTEL_QUERY_URL", "http://ignored.example:9000")
	t.Setenv("GOTEL_OTEL_COLLECTOR_URL", "http://also-ignored.example:9001")
	t.Setenv("GOTEL_OTEL_HOST", "0.0.0.0")
	t.Setenv("GOTEL_OTEL_PORT", "8123")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	cfg := Load()
	if cfg.BaseURL != "http://collector.example:4318/custom" || cfg.QueryURL != cfg.BaseURL {
		t.Fatalf("unexpected base/query URLs: %q %q", cfg.BaseURL, cfg.QueryURL)
	}
	if cfg.ExporterURL != "http://collector.example:4318/custom/v1/traces" || cfg.LogsExporterURL != "http://collector.example:4318/custom/v1/logs" {
		t.Fatalf("unexpected exporter URLs: %q %q", cfg.ExporterURL, cfg.LogsExporterURL)
	}
	if cfg.Host != "0.0.0.0" || cfg.Port != 8123 {
		t.Fatalf("unexpected listener: %s:%d", cfg.Host, cfg.Port)
	}
}

func TestLoadRenamedDefaults(t *testing.T) {
	for _, name := range []string{
		"GOTEL_OTEL_BASE_URL", "GOTEL_OTEL_QUERY_URL", "GOTEL_OTEL_COLLECTOR_URL", "GOTEL_OTEL_HOST", "GOTEL_OTEL_PORT",
		"GOTEL_OTEL_SERVICE_NAME", "GOTEL_OTEL_EXPORTER_URL", "GOTEL_OTEL_LOGS_EXPORTER_URL", "GOTEL_OTEL_DB_PATH",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("GOTEL_RUNTIME_DIR", t.TempDir())
	cfg := Load()
	if cfg.BaseURL != "http://127.0.0.1:27686" || cfg.Host != "127.0.0.1" || cfg.Port != 27686 {
		t.Fatalf("unexpected default endpoint: %#v", cfg)
	}
	if cfg.ServiceName != "gotel-otel-tui" {
		t.Fatalf("unexpected default service: %q", cfg.ServiceName)
	}
}

func TestLoadURLWithoutPortDefaultsTo80(t *testing.T) {
	t.Setenv("GOTEL_OTEL_BASE_URL", "http://collector.example/")
	t.Setenv("GOTEL_OTEL_QUERY_URL", "")
	t.Setenv("GOTEL_OTEL_COLLECTOR_URL", "")
	t.Setenv("GOTEL_OTEL_PORT", "")
	cfg := Load()
	if cfg.Port != 80 || cfg.BaseURL != "http://collector.example/" || cfg.QueryURL != cfg.BaseURL {
		t.Fatalf("unexpected endpoint: %q port %d", cfg.BaseURL, cfg.Port)
	}
}

func TestLoadManagedUsesLocalEndpointRules(t *testing.T) {
	t.Setenv("GOTEL_RUNTIME_DIR", t.TempDir())
	t.Setenv("GOTEL_OTEL_BASE_URL", "")
	t.Setenv("GOTEL_OTEL_QUERY_URL", "")
	t.Setenv("GOTEL_OTEL_COLLECTOR_URL", "http://remote.example:4318")
	t.Setenv("GOTEL_OTEL_HOST", "")
	t.Setenv("GOTEL_OTEL_PORT", "")
	t.Setenv("GOTEL_OTEL_EXPORTER_URL", "http://ignored.example/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://telemetry.example/otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	cfg := LoadManaged()
	if cfg.BaseURL != "http://127.0.0.1:27686" || cfg.QueryURL != cfg.BaseURL || cfg.Port != 27686 {
		t.Fatalf("collector URL affected managed endpoint: %#v", cfg)
	}
	if cfg.ExporterURL != cfg.BaseURL+"/v1/traces" || cfg.LogsExporterURL != cfg.BaseURL+"/v1/logs" {
		t.Fatalf("managed exporters do not use managed endpoint: %#v", cfg)
	}
	if cfg.TelemetryURL != "http://telemetry.example/otlp/v1/traces" {
		t.Fatalf("managed endpoint overwrote self-telemetry endpoint: %q", cfg.TelemetryURL)
	}

	t.Setenv("GOTEL_OTEL_BASE_URL", "https://source.example")
	t.Setenv("GOTEL_OTEL_HOST", "::1")
	t.Setenv("GOTEL_OTEL_PORT", "8765")
	cfg = LoadManaged()
	if cfg.BaseURL != "http://[::1]:8765" || cfg.Host != "::1" || cfg.Port != 8765 {
		t.Fatalf("unexpected managed override: %#v", cfg)
	}
}

func TestSelfTelemetryEndpointPrecedence(t *testing.T) {
	t.Setenv("GOTEL_OTEL_BASE_URL", "http://127.0.0.1:8765")
	t.Setenv("GOTEL_OTEL_EXPORTER_URL", "http://renamed.example/fallback")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://standard.example/base")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces.example/custom")
	if got := Load().TelemetryURL; got != "http://traces.example/custom" {
		t.Fatalf("trace-specific endpoint lost precedence: %q", got)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	if got := Load().TelemetryURL; got != "http://standard.example/base/v1/traces" {
		t.Fatalf("standard base endpoint was not resolved: %q", got)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	if got := Load().TelemetryURL; got != "http://renamed.example/fallback" {
		t.Fatalf("renamed endpoint fallback was not used: %q", got)
	}
}

func TestIngestionDefaultsAndValidation(t *testing.T) {
	var zero Config
	for _, setting := range zero.ingestionSettings() {
		t.Setenv(setting.name, "")
	}
	cfg := Load()
	if err := cfg.ValidateIngestion(); err != nil {
		t.Fatal(err)
	}
	for _, setting := range cfg.ingestionSettings() {
		if *setting.value != setting.fallback {
			t.Errorf("%s = %d, want %d", setting.name, *setting.value, setting.fallback)
		}
	}
	if err := zero.ValidateIngestion(); err == nil {
		t.Fatal("direct zero-valued configuration must be rejected")
	}
	for _, setting := range cfg.ingestionSettings() {
		for _, raw := range []string{"0", "-1", "5seconds", "1.5", "no", "999999999999999999999999999999999"} {
			t.Run(setting.name+"/"+raw, func(t *testing.T) {
				t.Setenv(setting.name, raw)
				if err := Load().ValidateIngestion(); err == nil {
					t.Fatalf("accepted invalid %s=%q", setting.name, raw)
				}
			})
		}
		old := *setting.value
		*setting.value = setting.maximum + 1
		if err := cfg.ValidateIngestion(); err == nil {
			t.Errorf("accepted impractical %s", setting.name)
		}
		*setting.value = old
	}
	tooMany := cfg
	tooMany.MaxBatchRecords = cfg.MaxOutstandingRecords + 1
	if err := tooMany.ValidateIngestion(); err == nil {
		t.Fatal("batch records may not exceed outstanding records")
	}
	tooBig := cfg
	tooBig.MaxBatchBytes = cfg.MaxOutstandingBytes + 1
	if err := tooBig.ValidateIngestion(); err == nil {
		t.Fatal("batch bytes may not exceed outstanding bytes")
	}
}

func TestIngestionEnvironmentOverrides(t *testing.T) {
	var zero Config
	for _, setting := range zero.ingestionSettings() {
		t.Setenv(setting.name, " 42 ")
	}
	cfg := LoadManaged()
	if err := cfg.ValidateIngestion(); err != nil {
		t.Fatal(err)
	}
	for _, setting := range cfg.ingestionSettings() {
		if *setting.value != 42 {
			t.Errorf("%s = %d, want 42", setting.name, *setting.value)
		}
	}
}

package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const Version = "0.1.0"

type Config struct {
	Enabled                  bool
	ServiceName              string
	BaseURL                  string
	QueryURL                 string
	ExporterURL              string
	LogsExporterURL          string
	TelemetryURL             string
	Host                     string
	Port                     int
	StateDir                 string
	DatabasePath             string
	TraceLookbackMinutes     int
	TraceLimit               int
	LogLimit                 int
	RetentionHours           int
	MaxDBSizeMB              int
	RetentionTraceBatch      int
	RetentionLogBatch        int
	RetentionIntervalSeconds int
	MaxRequestBytes          int
	MaxConcurrentIngest      int
	MaxOutstandingRecords    int
	MaxOutstandingBytes      int
	MaxBatchRecords          int
	MaxBatchBytes            int
	MaxBatchWork             int
	WriteTimeoutSeconds      int
	ShutdownTimeoutSeconds   int
	ingestionParseError      string
}

func Load() Config {
	stateDir := stateDirectory()
	base := firstNonBlank(
		os.Getenv("GOTEL_OTEL_BASE_URL"),
		os.Getenv("GOTEL_OTEL_QUERY_URL"),
		os.Getenv("GOTEL_OTEL_COLLECTOR_URL"),
		"http://127.0.0.1:27686",
	)
	parsed, err := url.Parse(ensureTrailingSlash(base))
	if err != nil || parsed.Hostname() == "" {
		parsed, _ = url.Parse("http://127.0.0.1:27686/")
	}
	host := parsed.Hostname()
	port := positiveInt(os.Getenv("GOTEL_OTEL_PORT"), 0)
	if port == 0 {
		port, _ = strconv.Atoi(parsed.Port())
		if port <= 0 {
			port = 80
		}
	}
	host = firstNonBlank(os.Getenv("GOTEL_OTEL_HOST"), host)
	dbPath := firstNonBlank(os.Getenv("GOTEL_OTEL_DB_PATH"), filepath.Join(stateDir, "telemetry.sqlite"))
	exporterURL := firstNonBlank(os.Getenv("GOTEL_OTEL_EXPORTER_URL"), resolveURL(parsed, "v1/traces"))
	telemetryURL := firstNonBlank(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"), standardTraceURL(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")), exporterURL)
	cfg := Config{
		Enabled:                  enabled(os.Getenv("GOTEL_OTEL_ENABLED")),
		ServiceName:              firstNonBlank(os.Getenv("GOTEL_OTEL_SERVICE_NAME"), "gotel-otel-tui"),
		BaseURL:                  base,
		QueryURL:                 base,
		ExporterURL:              exporterURL,
		LogsExporterURL:          firstNonBlank(os.Getenv("GOTEL_OTEL_LOGS_EXPORTER_URL"), resolveURL(parsed, "v1/logs")),
		TelemetryURL:             telemetryURL,
		Host:                     host,
		Port:                     port,
		StateDir:                 stateDir,
		DatabasePath:             dbPath,
		TraceLookbackMinutes:     positiveInt(os.Getenv("GOTEL_OTEL_TRACE_LOOKBACK_MINUTES"), 1440),
		TraceLimit:               positiveInt(os.Getenv("GOTEL_OTEL_TRACE_LIMIT"), 100),
		LogLimit:                 positiveInt(os.Getenv("GOTEL_OTEL_LOG_LIMIT"), 80),
		RetentionHours:           positiveInt(os.Getenv("GOTEL_OTEL_RETENTION_HOURS"), 168),
		MaxDBSizeMB:              positiveInt(os.Getenv("GOTEL_OTEL_MAX_DB_SIZE_MB"), 1024),
		RetentionTraceBatch:      positiveInt(os.Getenv("GOTEL_OTEL_RETENTION_TRACE_BATCH"), 100),
		RetentionLogBatch:        positiveInt(os.Getenv("GOTEL_OTEL_RETENTION_LOG_BATCH"), 5000),
		RetentionIntervalSeconds: positiveInt(os.Getenv("GOTEL_OTEL_RETENTION_INTERVAL_SECONDS"), 10),
	}
	for _, setting := range cfg.ingestionSettings() {
		*setting.value = setting.fallback
		if raw := strings.TrimSpace(os.Getenv(setting.name)); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil {
				cfg.ingestionParseError = setting.name + " must be an integer"
			} else {
				*setting.value = value
			}
		}
	}
	return cfg
}

type ingestionSetting struct {
	name     string
	value    *int
	fallback int
	maximum  int
}

func (cfg *Config) ingestionSettings() []ingestionSetting {
	return []ingestionSetting{
		{"GOTEL_OTEL_MAX_REQUEST_BYTES", &cfg.MaxRequestBytes, 4 << 20, 1 << 30},
		{"GOTEL_OTEL_MAX_CONCURRENT_INGEST", &cfg.MaxConcurrentIngest, 2, 1024},
		{"GOTEL_OTEL_MAX_OUTSTANDING_RECORDS", &cfg.MaxOutstandingRecords, 10000, 1000000},
		{"GOTEL_OTEL_MAX_OUTSTANDING_BYTES", &cfg.MaxOutstandingBytes, 64 << 20, 1 << 30},
		{"GOTEL_OTEL_MAX_BATCH_RECORDS", &cfg.MaxBatchRecords, 256, 1000000},
		{"GOTEL_OTEL_MAX_BATCH_BYTES", &cfg.MaxBatchBytes, 2 << 20, 1 << 30},
		{"GOTEL_OTEL_MAX_BATCH_WORK", &cfg.MaxBatchWork, 8192, 10000000},
		{"GOTEL_OTEL_WRITE_TIMEOUT_SECONDS", &cfg.WriteTimeoutSeconds, 5, 300},
		{"GOTEL_OTEL_SHUTDOWN_TIMEOUT_SECONDS", &cfg.ShutdownTimeoutSeconds, 20, 300},
	}
}

// ValidateIngestion rejects invalid limits rather than silently disabling bounds.
// Load preserves its historical API; numeric environment parse errors surface here.
func (cfg Config) ValidateIngestion() error {
	if cfg.ingestionParseError != "" {
		return fmt.Errorf("invalid ingestion configuration: %s", cfg.ingestionParseError)
	}
	for _, setting := range cfg.ingestionSettings() {
		if *setting.value <= 0 || *setting.value > setting.maximum {
			return fmt.Errorf("%s must be between 1 and %d", setting.name, setting.maximum)
		}
	}
	if cfg.MaxBatchRecords > cfg.MaxOutstandingRecords {
		return fmt.Errorf("GOTEL_OTEL_MAX_BATCH_RECORDS exceeds GOTEL_OTEL_MAX_OUTSTANDING_RECORDS")
	}
	if cfg.MaxBatchBytes > cfg.MaxOutstandingBytes {
		return fmt.Errorf("GOTEL_OTEL_MAX_BATCH_BYTES exceeds GOTEL_OTEL_MAX_OUTSTANDING_BYTES")
	}
	return nil
}

func LoadManaged() Config {
	cfg := Load()
	if stateDir, err := filepath.Abs(cfg.StateDir); err == nil {
		cfg.StateDir = stateDir
	}
	if databasePath, err := filepath.Abs(cfg.DatabasePath); err == nil {
		cfg.DatabasePath = databasePath
	}
	base := firstNonBlank(
		os.Getenv("GOTEL_OTEL_BASE_URL"),
		os.Getenv("GOTEL_OTEL_QUERY_URL"),
		"http://127.0.0.1:27686",
	)
	parsed, err := url.Parse(base)
	if err != nil || parsed.Hostname() == "" {
		parsed, _ = url.Parse("http://127.0.0.1:27686")
	}
	host := firstNonBlank(os.Getenv("GOTEL_OTEL_HOST"), parsed.Hostname())
	port := positiveInt(os.Getenv("GOTEL_OTEL_PORT"), 0)
	if port == 0 {
		port, _ = strconv.Atoi(parsed.Port())
		if port <= 0 {
			port = 27686
		}
	}
	base = "http://" + net.JoinHostPort(host, strconv.Itoa(port))
	cfg.BaseURL = base
	cfg.QueryURL = base
	cfg.ExporterURL = base + "/v1/traces"
	cfg.LogsExporterURL = base + "/v1/logs"
	cfg.Host = host
	cfg.Port = port
	return cfg
}

func ensureTrailingSlash(value string) string {
	if strings.HasSuffix(value, "/") {
		return value
	}
	return value + "/"
}

func resolveURL(base *url.URL, path string) string {
	return base.ResolveReference(&url.URL{Path: path}).String()
}

func standardTraceURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(ensureTrailingSlash(value))
	if err != nil || parsed.Hostname() == "" {
		return ""
	}
	return resolveURL(parsed, "v1/traces")
}

func stateDirectory() string {
	if value := strings.TrimSpace(os.Getenv("GOTEL_RUNTIME_DIR")); value != "" {
		return value
	}
	home, _ := os.UserHomeDir()
	root := strings.TrimSpace(os.Getenv("XDG_STATE_HOME"))
	if root == "" {
		root = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(root, "gotel")
}

func enabled(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func positiveInt(value string, fallback int) int {
	value = strings.TrimSpace(value)
	i := 0
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		i++
	}
	if i == 0 {
		return fallback
	}
	n, err := strconv.Atoi(value[:i])
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

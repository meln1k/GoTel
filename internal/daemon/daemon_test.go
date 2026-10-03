package daemon

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/registry"
)

func TestGetStatusRecognizesManagedIdentity(t *testing.T) {
	stateDir := t.TempDir()
	databasePath := filepath.Join(stateDir, "telemetry.sqlite")
	instanceID := "test-instance"
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	workdir := t.TempDir()
	var serverURL string
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "service": "gotel-local-server", "databasePath": databasePath,
			"pid": os.Getpid(), "url": serverURL, "workdir": workdir, "startedAt": startedAt,
			"version": config.Version, "instanceId": instanceID, "databaseBackend": "sqlite",
			"ready": false, "persistence": map[string]any{"ready": false, "outstandingRecords": 2},
			"shutdownTimeoutSeconds": 60,
		})
	}))
	defer testServer.Close()
	serverURL = testServer.URL
	if err := registry.Write(stateDir, registry.Entry{
		PID: os.Getpid(), URL: serverURL, Workdir: workdir, StartedAt: startedAt, Version: config.Version,
		InstanceID: instanceID, ProcessIdentity: registry.ProcessIdentity(os.Getpid(), startedAt), DatabasePath: databasePath,
	}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Load()
	cfg.StateDir, cfg.DatabasePath, cfg.BaseURL = stateDir, databasePath, serverURL
	status := GetStatus(context.Background(), cfg)
	if !status.Running || !status.Managed || status.PID == nil || *status.PID != os.Getpid() || status.Reason != nil {
		t.Fatalf("unexpected managed status: %#v", status)
	}
	if status.Ready || len(status.Persistence) == 0 {
		t.Fatalf("readiness must not decide managed liveness: %#v", status)
	}
	if status.ShutdownTimeoutSeconds != 60 {
		t.Fatalf("server's shutdown budget was not forwarded: %#v", status)
	}
}

func TestGetStatusChecksBackendIdentity(t *testing.T) {
	for _, backend := range []string{"", "unsupported", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			cfg := config.Load()
			cfg.StateDir = t.TempDir()
			cfg.DatabasePath = filepath.Join(cfg.StateDir, "custom.db")
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok": true, "service": "gotel-local-server", "databasePath": cfg.DatabasePath,
					"databaseBackend": backend, "pid": os.Getpid(),
				})
			}))
			defer testServer.Close()
			cfg.BaseURL = testServer.URL
			status := GetStatus(context.Background(), cfg)
			if status.Running != (backend == "sqlite") || status.Managed {
				t.Fatalf("wrong backend identity accepted: %+v", status)
			}
		})
	}
}

func TestGetStatusRejectsForeignService(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "service": "another-service", "databasePath": "/tmp/other.sqlite", "pid": os.Getpid(),
			"url": "http://example.invalid", "workdir": "/tmp", "startedAt": time.Now().UTC().Format(time.RFC3339Nano), "version": "1",
		})
	}))
	defer testServer.Close()
	cfg := config.Load()
	cfg.StateDir = t.TempDir()
	cfg.BaseURL = testServer.URL
	_, rawPort, _ := netSplitHostPort(testServer.URL)
	cfg.Port, _ = strconv.Atoi(rawPort)
	status := GetStatus(context.Background(), cfg)
	if status.Running || status.Managed || status.Service == nil || *status.Service != "another-service" || status.Reason == nil {
		t.Fatalf("unexpected foreign status: %#v", status)
	}
	if _, err := Ensure(context.Background(), cfg); err == nil {
		t.Fatal("ensure must refuse a foreign service")
	}
}

func netSplitHostPort(rawURL string) (string, string, error) {
	request, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return "", "", err
	}
	host, port, err := net.SplitHostPort(request.URL.Host)
	return host, port, err
}

func TestManagedStopGrace(t *testing.T) {
	for _, test := range []struct {
		seconds int
		want    time.Duration
	}{
		{20, 35 * time.Second}, {1, 16 * time.Second}, {300, 315 * time.Second},
		{0, 35 * time.Second}, {-1, 35 * time.Second}, {1000000, 35 * time.Second},
	} {
		cfg := config.Load()
		cfg.ShutdownTimeoutSeconds = test.seconds
		if got := managedStopGrace(cfg, 0); got != test.want {
			t.Errorf("shutdown budget %d: grace=%v want %v", test.seconds, got, test.want)
		}
	}
	cfg := config.Load()
	cfg.ShutdownTimeoutSeconds = 1
	if got := managedStopGrace(cfg, 60); got != 75*time.Second {
		t.Fatalf("server's advertised drain budget ignored: %v", got)
	}
}

func TestEnsureValidatesBeforeStarting(t *testing.T) {
	cfg := config.Load()
	cfg.MaxConcurrentIngest = 0
	cfg.StateDir = filepath.Join(t.TempDir(), "must-not-create")
	if _, err := Ensure(context.Background(), cfg); err == nil {
		t.Fatal("invalid managed ingestion config accepted")
	}
	if _, err := os.Stat(cfg.StateDir); !os.IsNotExist(err) {
		t.Fatalf("invalid config produced startup side effects: %v", err)
	}
}

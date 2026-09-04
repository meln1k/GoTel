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
	databasePath := filepath.Join(stateDir, "telemetry.duckdb")
	instanceID := "test-instance"
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	workdir := t.TempDir()
	var serverURL string
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "service": "gotel-local-server", "databasePath": databasePath,
			"pid": os.Getpid(), "url": serverURL, "workdir": workdir, "startedAt": startedAt,
			"version": config.Version, "instanceId": instanceID,
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
}

func TestGetStatusRejectsForeignService(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "service": "another-service", "databasePath": "/tmp/other.duckdb", "pid": os.Getpid(),
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

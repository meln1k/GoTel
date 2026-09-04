package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/meln1k/gotel/internal/client"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/registry"
	"github.com/meln1k/gotel/internal/server"
	"github.com/meln1k/gotel/internal/store"
	"github.com/meln1k/gotel/internal/telemetry"
)

type Status struct {
	Running      bool    `json:"running"`
	Managed      bool    `json:"managed"`
	Service      *string `json:"service"`
	PID          *int    `json:"pid"`
	URL          string  `json:"url"`
	DatabasePath string  `json:"databasePath"`
	Workdir      *string `json:"workdir"`
	StartedAt    *string `json:"startedAt"`
	Version      *string `json:"version"`
	SameWorkdir  bool    `json:"sameWorkdir"`
	Reason       *string `json:"reason"`
	LogPath      string  `json:"logPath"`
	LockPath     string  `json:"lockPath"`
	RegistryPID  *int    `json:"registryPid"`
}

func LogPath(cfg config.Config) string  { return filepath.Join(cfg.StateDir, "daemon.log") }
func LockPath(cfg config.Config) string { return filepath.Join(cfg.StateDir, "daemon.lock") }

func GetStatus(ctx context.Context, cfg config.Config) Status {
	status := downStatus(cfg)
	entries, _ := registry.List(cfg.StateDir)
	var registered *registry.Entry
	for _, entry := range entries {
		if entry.URL == cfg.BaseURL && entry.DatabasePath == cfg.DatabasePath && registry.Alive(entry.PID) {
			copy := entry
			registered = &copy
			pid := entry.PID
			status.RegistryPID = &pid
			status.PID = &pid
			workdir, startedAt, version := entry.Workdir, entry.StartedAt, entry.Version
			status.Workdir, status.StartedAt, status.Version = &workdir, &startedAt, &version
			current, _ := os.Getwd()
			status.SameWorkdir = samePath(current, workdir)
			reason := "registry entry exists but server is not healthy"
			status.Reason = &reason
			break
		}
	}
	probe := client.New(cfg.BaseURL)
	probe.HTTP.Timeout = 3 * time.Second
	var health client.Health
	err := probe.GetInto(ctx, "/api/health", nil, &health)
	if err != nil || !health.OK {
		return status
	}
	service, pid, workdir, startedAt, version := health.Service, health.PID, health.Workdir, health.StartedAt, health.Version
	status.Service, status.PID = &service, &pid
	status.URL, status.DatabasePath = health.URL, health.DatabasePath
	status.Workdir, status.StartedAt, status.Version = &workdir, &startedAt, &version
	current, _ := os.Getwd()
	status.SameWorkdir = samePath(current, workdir)
	if health.Service != "gotel-local-server" {
		reason := fmt.Sprintf("port %d is in use by %s, not gotel-local-server", cfg.Port, health.Service)
		status.Reason = &reason
		return status
	}
	if health.DatabasePath != cfg.DatabasePath {
		reason := fmt.Sprintf("port %d is serving Gotel with %s, expected %s", cfg.Port, health.DatabasePath, cfg.DatabasePath)
		status.Reason = &reason
		return status
	}
	status.Running = true
	if registered != nil && registered.PID == health.PID && registered.InstanceID == health.InstanceID {
		status.Managed = true
		status.RegistryPID = &pid
		status.Reason = nil
		return status
	}
	reason := "responsive Gotel server is not an identity-verified managed daemon"
	status.Reason = &reason
	return status
}

func Ensure(ctx context.Context, cfg config.Config) (Status, error) {
	if status := GetStatus(ctx, cfg); status.Running {
		if status.Managed {
			return status, nil
		}
		return status, errors.New(*status.Reason)
	} else if status.Service != nil && status.Reason != nil {
		return status, errors.New(*status.Reason)
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return downStatus(cfg), err
	}
	unlock, err := acquireLock(ctx, cfg)
	if err != nil {
		return downStatus(cfg), err
	}
	defer unlock()
	if status := GetStatus(ctx, cfg); status.Running {
		if status.Managed {
			return status, nil
		}
		return status, errors.New(*status.Reason)
	} else if status.Service != nil && status.Reason != nil {
		return status, errors.New(*status.Reason)
	}
	executable, err := os.Executable()
	if err != nil {
		return downStatus(cfg), err
	}
	logFile, err := os.OpenFile(LogPath(cfg), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return downStatus(cfg), err
	}
	defer logFile.Close()
	instanceID := randomID()
	command := exec.Command(executable, "server")
	command.Stdout, command.Stderr, command.Stdin = logFile, logFile, nil
	command.Env = append(os.Environ(), "GOTEL_DAEMON_INSTANCE_ID="+instanceID)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return downStatus(cfg), err
	}
	pid := command.Process.Pid
	_ = command.Process.Release()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return downStatus(cfg), ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		status := GetStatus(ctx, cfg)
		if status.Managed && status.PID != nil && *status.PID == pid {
			if err := ingestProbe(ctx, cfg); err == nil {
				return status, nil
			}
		}
		if !registry.Alive(pid) {
			break
		}
	}
	if process, findErr := os.FindProcess(pid); findErr == nil {
		_ = process.Signal(syscall.SIGTERM)
	}
	return downStatus(cfg), fmt.Errorf("Gotel server did not become ready; see %s", LogPath(cfg))
}

func Stop(ctx context.Context, cfg config.Config) (Status, error) {
	status := GetStatus(ctx, cfg)
	if !status.Running {
		if status.Service != nil && status.Reason != nil {
			return status, errors.New(*status.Reason)
		}
		return status, nil
	}
	if !status.Managed || status.PID == nil {
		return status, errors.New("refusing to stop an unverified process")
	}
	pid := *status.PID
	process, err := os.FindProcess(pid)
	if err != nil {
		return status, err
	}
	if err := process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return status, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for registry.Alive(pid) && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if registry.Alive(pid) {
		_ = process.Signal(syscall.SIGKILL)
	}
	_ = registry.Remove(cfg.StateDir, pid)
	return GetStatus(ctx, cfg), nil
}

func Restart(ctx context.Context, cfg config.Config) (Status, error) {
	if _, err := Stop(ctx, cfg); err != nil {
		return downStatus(cfg), err
	}
	return Ensure(ctx, cfg)
}

func RunServer(ctx context.Context, cfg config.Config) (runErr error) {
	telemetryStore, err := store.Open(cfg)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, telemetryStore.Close()) }()
	shutdownTelemetry, err := telemetry.Setup(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(shutdownContext)
	}()
	workdir, _ := os.Getwd()
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	instanceID := os.Getenv("GOTEL_DAEMON_INSTANCE_ID")
	if instanceID == "" {
		instanceID = randomID()
	}
	entry := registry.Entry{
		PID: os.Getpid(), URL: cfg.BaseURL, Workdir: workdir, StartedAt: startedAt, Version: config.Version,
		InstanceID: instanceID, ProcessIdentity: registry.ProcessIdentity(os.Getpid(), startedAt), DatabasePath: cfg.DatabasePath,
	}
	if err := registry.Write(cfg.StateDir, entry); err != nil {
		return err
	}
	defer registry.Remove(cfg.StateDir, entry.PID)
	go telemetryStore.RunMaintenance(ctx)
	identity := server.Identity{
		PID: entry.PID, URL: entry.URL, Workdir: entry.Workdir, StartedAt: entry.StartedAt,
		InstanceID: entry.InstanceID, DatabasePath: entry.DatabasePath,
	}
	return server.New(cfg, telemetryStore, identity).Run(ctx)
}

func downStatus(cfg config.Config) Status {
	return Status{URL: cfg.BaseURL, DatabasePath: cfg.DatabasePath, LogPath: LogPath(cfg), LockPath: LockPath(cfg)}
}

func acquireLock(ctx context.Context, cfg config.Config) (func(), error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		file, err := os.OpenFile(LockPath(cfg), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(file, "%d", os.Getpid())
			_ = file.Close()
			return func() { _ = os.Remove(LockPath(cfg)) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if info, statErr := os.Stat(LockPath(cfg)); statErr == nil && time.Since(info.ModTime()) > 30*time.Second {
			_ = os.Remove(LockPath(cfg))
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for %s", LockPath(cfg))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func ingestProbe(ctx context.Context, cfg config.Config) error {
	httpClient := client.New(cfg.BaseURL)
	httpClient.HTTP.Timeout = 3 * time.Second
	if _, err := httpClient.PostJSON(ctx, "/v1/traces", map[string]any{"resourceSpans": []any{}}); err != nil {
		return err
	}
	_, err := httpClient.PostJSON(ctx, "/v1/logs", map[string]any{"resourceLogs": []any{}})
	return err
}

func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}

func samePath(left, right string) bool {
	left, leftErr := filepath.Abs(left)
	right, rightErr := filepath.Abs(right)
	return leftErr == nil && rightErr == nil && left == right
}

func PrintStatus(status Status) error {
	encoded, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

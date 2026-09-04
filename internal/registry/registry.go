package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

type Entry struct {
	PID             int    `json:"pid"`
	URL             string `json:"url"`
	Workdir         string `json:"workdir"`
	StartedAt       string `json:"startedAt"`
	Version         string `json:"version"`
	InstanceID      string `json:"instanceId,omitempty"`
	ProcessIdentity string `json:"processIdentity"`
	DatabasePath    string `json:"databasePath"`
}

func Directory(stateDir string) string { return filepath.Join(stateDir, "instances") }

func Path(stateDir string, pid int) string {
	return filepath.Join(Directory(stateDir), strconv.Itoa(pid)+".json")
}

func Write(stateDir string, entry Entry) error {
	if err := os.MkdirAll(Directory(stateDir), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return err
	}
	target := Path(stateDir, entry.PID)
	temporary := target + ".tmp"
	if err := os.WriteFile(temporary, encoded, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, target)
}

func Remove(stateDir string, pid int) error {
	err := os.Remove(Path(stateDir, pid))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func List(stateDir string) ([]Entry, error) {
	entries, err := os.ReadDir(Directory(stateDir))
	if errors.Is(err, os.ErrNotExist) {
		return []Entry{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]Entry, 0, len(entries))
	for _, file := range entries {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}
		content, readErr := os.ReadFile(filepath.Join(Directory(stateDir), file.Name()))
		if readErr != nil {
			continue
		}
		var entry Entry
		if json.Unmarshal(content, &entry) != nil || entry.PID <= 0 || entry.URL == "" {
			continue
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StartedAt > result[j].StartedAt })
	return result, nil
}

func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, os.ErrPermission)
}

func ProcessIdentity(pid int, startedAt string) string {
	return fmt.Sprintf("%d:%s", pid, startedAt)
}

func BestForWorkdir(entries []Entry, workdir string) *Entry {
	var best *Entry
	for i := range entries {
		entry := &entries[i]
		if !Alive(entry.PID) || !pathContains(workdir, entry.Workdir) {
			continue
		}
		if best == nil || len(entry.Workdir) > len(best.Workdir) {
			copy := *entry
			best = &copy
		}
	}
	return best
}

func pathContains(path, parent string) bool {
	path, pathErr := filepath.Abs(path)
	parent, parentErr := filepath.Abs(parent)
	if pathErr != nil || parentErr != nil {
		return false
	}
	relative, err := filepath.Rel(parent, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

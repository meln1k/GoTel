package cleanup

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var extensions = map[string]bool{
	".js": true, ".jsx": true, ".ts": true, ".tsx": true, ".mts": true, ".cts": true,
}

var ignoredDirectories = map[string]bool{
	".git": true, "node_modules": true, "dist": true, "build": true, "coverage": true,
	".next": true, ".turbo": true, ".gotel-data": true,
}

func Run(root string) ([]string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	changed := make([]string, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && ignoredDirectories[entry.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !extensions[strings.ToLower(filepath.Ext(entry.Name()))] {
			return nil
		}
		updated, didChange, err := cleanFile(path)
		if err != nil {
			return err
		}
		if didChange {
			if err := os.WriteFile(path, updated, 0); err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			changed = append(changed, relative)
		}
		return nil
	})
	return changed, err
}

func cleanFile(path string) ([]byte, bool, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(path); statErr == nil {
		mode = info.Mode()
	}
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	lines := make([]string, 0)
	depth := 0
	found := false
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if strings.Contains(line, "#region gotel debug") {
			depth++
			found = true
			continue
		}
		if strings.Contains(line, "#endregion gotel debug") {
			if depth == 0 {
				return nil, false, fmt.Errorf("Unmatched #endregion gotel debug in %s:%d", path, lineNumber)
			}
			depth--
			continue
		}
		if depth == 0 {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, false, err
	}
	if depth != 0 {
		return nil, false, fmt.Errorf("Unmatched #region gotel debug in %s", path)
	}
	if !found {
		return content, false, nil
	}
	updated := strings.Join(lines, "\n")
	if len(content) > 0 && content[len(content)-1] == '\n' {
		updated += "\n"
	}
	if err := os.Chmod(path, mode); err != nil {
		return nil, false, err
	}
	return []byte(updated), true, nil
}

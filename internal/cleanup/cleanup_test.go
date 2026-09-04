package cleanup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRemovesNestedDebugBlocks(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "example.ts")
	content := `const before = true
// #region gotel debug
debug("outer")
// #region gotel debug
debug("inner")
// #endregion gotel debug
// #endregion gotel debug
const after = true
`
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
	changed, err := Run(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0] != "example.ts" {
		t.Fatalf("changed %#v, want example.ts", changed)
	}
	actual, _ := os.ReadFile(path)
	if string(actual) != "const before = true\nconst after = true\n" {
		t.Fatalf("unexpected output: %q", actual)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("permissions changed to %o", info.Mode().Perm())
	}
}

func TestRunRejectsUnmatchedMarkers(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "example.js")
	if err := os.WriteFile(path, []byte("// #endregion gotel debug\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Run(root)
	if err == nil || !strings.Contains(err.Error(), "Unmatched #endregion gotel debug") {
		t.Fatalf("unexpected error: %v", err)
	}
}

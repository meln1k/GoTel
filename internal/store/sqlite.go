package store

import (
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/mattn/go-sqlite3"
)

const sqliteMaxParameters = 32766

// Admission checks the merged row count without allocating another map. This
// also applies when users raise the transaction work/byte budgets.
func sqliteAttributesFit(attributes, resource map[string]string, columns int) bool {
	rows := len(attributes)
	for key := range resource {
		if _, exists := attributes[key]; !exists {
			rows++
		}
	}
	return rows <= sqliteMaxParameters/columns
}

// Each physical connection gets the same settings and query functions. The
// shared writer remains the only ingestion writer; maintenance uses writeMu.
func init() {
	sql.Register("gotel_sqlite", &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		if conn.GetLimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) < sqliteMaxParameters {
			return fmt.Errorf("SQLite build requires at least %d SQL parameters", sqliteMaxParameters)
		}
		if _, err := conn.Exec("PRAGMA mmap_size=0", nil); err != nil {
			return err
		}
		functions := map[string]any{
			"lower": func(value any) any {
				if text, ok := value.(string); ok {
					return strings.ToLower(text)
				}
				return nil
			},
			"upper": func(value any) any {
				if text, ok := value.(string); ok {
					return strings.ToUpper(text)
				}
				return nil
			},
			"contains": func(value any, needle string) any {
				if text, ok := value.(string); ok {
					return strings.Contains(text, needle)
				}
				return nil
			},
			"regexp_matches": func(value any, pattern string) (any, error) {
				if text, ok := value.(string); ok {
					return regexp.MatchString(pattern, text)
				}
				return nil, nil
			},
			"greatest": func(a, b int64) int64 { return max(a, b) },
			"ceil":     math.Ceil,
			// SQLite CAST accepts numeric prefixes ("12bad" -> 12). Parse
			// the entire attribute, as the Go reference analytics do.
			"gotel_number": func(value any) (any, error) {
				text, ok := value.(string)
				if !ok {
					return nil, nil
				}
				number, err := strconv.ParseFloat(text, 64)
				if err != nil {
					return nil, nil
				}
				if math.IsNaN(number) {
					return nil, fmt.Errorf("non-finite numeric attribute")
				}
				return number, nil
			},
		}
		for name, function := range functions {
			if err := conn.RegisterFunc(name, function, true); err != nil {
				return err
			}
		}
		return nil
	}})
}

func sqliteDSN(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := url.Values{
		"_journal_mode":        {"WAL"},
		"_synchronous":         {"FULL"},
		"_busy_timeout":        {"1000"},
		"_cache_size":          {"-8192"},
		"_txlock":              {"immediate"},
		"_case_sensitive_like": {"1"},
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

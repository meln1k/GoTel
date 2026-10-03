package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
)

func TestSQLiteRejectsUnsupportedFileWithoutChangingIt(t *testing.T) {
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "existing.db")
	const original = "existing telemetry in an unsupported database format"
	if err := os.WriteFile(cfg.DatabasePath, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(cfg)
	if s != nil {
		s.Close()
		t.Fatal("unsupported file opened as a telemetry store")
	}
	var databaseError sqlite3.Error
	if !errors.As(err, &databaseError) || databaseError.Code != sqlite3.ErrNotADB {
		t.Fatalf("expected an unsupported-file error, got %v", err)
	}
	contents, err := os.ReadFile(cfg.DatabasePath)
	if err != nil || string(contents) != original {
		t.Fatalf("unsupported database was changed: %v", err)
	}
}

func TestSQLitePoolSettingsAndFunctions(t *testing.T) {
	s := boundedStore(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Hold every connection so that the next checkout must open a new one.
	for i := 0; i < 8; i++ {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var journal string
		if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
			t.Fatalf("connection %d WAL: %s %v", i, journal, err)
		}
		for query, want := range map[string]int{
			"PRAGMA synchronous": 2, "PRAGMA cache_size": -8192,
			"PRAGMA mmap_size": 0, "PRAGMA busy_timeout": 1000,
			"SELECT 'a' LIKE 'A'": 0,
		} {
			var got int
			if err := conn.QueryRowContext(ctx, query).Scan(&got); err != nil || got != want {
				t.Fatalf("connection %d %s: %d want %d, %v", i, query, got, want, err)
			}
		}
		var nullText sql.NullString
		var lower, upper string
		var contains, matches bool
		var greatest, ceil int
		var invalid sql.NullFloat64
		var number float64
		err = conn.QueryRowContext(ctx, `SELECT lower(NULL), lower('ÉÜ'), upper('éü'),
			contains(lower('ÉÜ'), 'é'), regexp_matches('streamText', '^stream'),
			greatest(0, -2), ceil(2.2), gotel_number('12bad'), gotel_number('1.25')`).Scan(
			&nullText, &lower, &upper, &contains, &matches, &greatest, &ceil, &invalid, &number)
		if err != nil || nullText.Valid || lower != "éü" || upper != "ÉÜ" || !contains || !matches || greatest != 0 || ceil != 3 || invalid.Valid || number != 1.25 {
			t.Fatalf("connection %d function semantics: %v %s %s %v %v %d %d %v %v err=%v", i, nullText, lower, upper, contains, matches, greatest, ceil, invalid, number, err)
		}
	}
}

func TestSQLiteBusyRetryAndConcurrentReader(t *testing.T) {
	s := boundedStore(t, func(c *config.Config) { c.MaxOutstandingRecords, c.MaxBatchRecords = 4, 2 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	locker, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	if _, err := locker.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer locker.ExecContext(context.Background(), "ROLLBACK")
	if _, err := s.IngestLogs(ctx, []model.LogRecord{{Body: "first"}, {Body: "second"}}); err != nil {
		t.Fatal(err)
	}
	s.wake()
	awaitStats(t, s, func(v PersistenceStats) bool { return v.Failures > 0 })
	if _, err := s.IngestLogs(ctx, []model.LogRecord{{Body: "third"}, {Body: "fourth"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IngestLogs(ctx, []model.LogRecord{{Body: "over budget"}}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("busy writer released budget: %v", err)
	}
	// WAL readers must see committed data, without waiting for the locked writer.
	logs, err := s.SearchLogs(ctx, LogFilter{}, 10)
	if err != nil || len(logs) != 0 {
		t.Fatalf("reader blocked or saw uncommitted data: %+v %v", logs, err)
	}
	if err := s.Flush(ctx); err == nil {
		t.Fatal("flush hid a persistence failure")
	}
	if _, err := locker.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	s.wake()
	awaitStats(t, s, func(v PersistenceStats) bool { return v.CommittedRecords == 4 })
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	assertTableCount(t, s, "logs", 4)
	stats := s.Stats()
	if stats.DiscardedRecords != 0 || stats.OutstandingRecords != 0 || stats.CommittedRecords != 4 || !stats.Ready || stats.MaxBatchRecords != 2 {
		t.Fatalf("contention recovery: %+v", stats)
	}
}

func TestSQLiteAttributeParameterBoundary(t *testing.T) {
	for _, columns := range []int{4, 3} {
		t.Run(strconv.Itoa(columns), func(t *testing.T) {
			s := boundedStore(t, func(c *config.Config) {
				c.MaxBatchBytes, c.MaxBatchWork = 8<<20, 50000
			})
			limit := 8191 // 32766 bound parameters, four columns per span attribute.
			if columns == 3 {
				limit = 10922
			}
			attrs := make(map[string]string, limit+1)
			for i := 0; i <= limit; i++ {
				attrs[strconv.Itoa(i)] = "v"
			}
			// The overlapping resource key must not count as an extra row.
			resource := map[string]string{"0": "overridden"}
			admit := func() (int, error) {
				if columns == 4 {
					return s.IngestSpans(context.Background(), []model.SpanRecord{{TraceID: "limit", SpanID: "limit", Attributes: attrs, Resource: resource}})
				}
				return s.IngestLogs(context.Background(), []model.LogRecord{{Body: "limit", Attributes: attrs, Resource: resource}})
			}
			if n, err := admit(); n != 0 || !errors.Is(err, ErrRecordTooLarge) || s.Stats().OutstandingRecords != 0 {
				t.Fatalf("oversized record admitted: %d %v %+v", n, err, s.Stats())
			}
			delete(attrs, strconv.Itoa(limit))
			if n, err := admit(); n != 1 || err != nil {
				t.Fatalf("boundary rejected: %d %v", n, err)
			}
			mustFlush(t, s)
			table := "span_attributes"
			if columns == 3 {
				table = "log_attributes"
			}
			assertTableCount(t, s, table, limit)
			var value string
			if err := s.db.QueryRow("SELECT value FROM " + table + " WHERE key='0'").Scan(&value); err != nil || value != "v" {
				t.Fatalf("resource override changed: %q %v", value, err)
			}
			if stats := s.Stats(); stats.CommittedBatches != 1 || stats.Failures != 0 || stats.CommittedRecords != 1 {
				t.Fatalf("boundary persistence: %+v", stats)
			}
		})
	}
}

package logging

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDailyFileWriterRotatesByLocalDate(t *testing.T) {
	dir := t.TempDir()
	current := time.Date(2026, 6, 25, 23, 59, 0, 0, time.Local)
	writer, err := NewDailyFileWriter(DailyFileWriterConfig{
		Dir:    dir,
		Prefix: "marketdata-test",
		Now: func() time.Time {
			return current
		},
	})
	if err != nil {
		t.Fatalf("NewDailyFileWriter: %v", err)
	}

	if _, err := writer.Write([]byte("first\n")); err != nil {
		t.Fatalf("Write first: %v", err)
	}
	current = current.Add(2 * time.Minute)
	if _, err := writer.Write([]byte("second\n")); err != nil {
		t.Fatalf("Write second: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	first, err := os.ReadFile(filepath.Join(dir, "marketdata-test-2026-06-25.log"))
	if err != nil {
		t.Fatalf("Read first log: %v", err)
	}
	if string(first) != "first\n" {
		t.Fatalf("first log = %q", first)
	}
	second, err := os.ReadFile(filepath.Join(dir, "marketdata-test-2026-06-26.log"))
	if err != nil {
		t.Fatalf("Read second log: %v", err)
	}
	if string(second) != "second\n" {
		t.Fatalf("second log = %q", second)
	}
}

func TestDailyFileWriterRequiresDirectory(t *testing.T) {
	if _, err := NewDailyFileWriter(DailyFileWriterConfig{}); err == nil {
		t.Fatal("NewDailyFileWriter succeeded without directory")
	}
}

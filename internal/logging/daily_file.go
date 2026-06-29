package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const dateLayout = "2006-01-02"

// DailyFileWriter appends log records to one file per local calendar day.
type DailyFileWriter struct {
	mu     sync.Mutex
	dir    string
	prefix string
	now    func() time.Time

	date string
	file *os.File
}

// DailyFileWriterConfig configures daily file log rotation.
type DailyFileWriterConfig struct {
	Dir    string
	Prefix string
	Now    func() time.Time
}

// NewDailyFileWriter creates a writer that rotates files when the local date changes.
func NewDailyFileWriter(cfg DailyFileWriterConfig) (*DailyFileWriter, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("log directory is required")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "marketdata"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	writer := &DailyFileWriter{dir: cfg.Dir, prefix: cfg.Prefix, now: cfg.Now}
	if err := writer.rotateLocked(); err != nil {
		return nil, err
	}
	return writer, nil
}

// Write implements io.Writer.
func (w *DailyFileWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	return w.file.Write(payload)
}

// Close closes the current log file.
func (w *DailyFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *DailyFileWriter) rotateLocked() error {
	date := w.now().Local().Format(dateLayout)
	if w.file != nil && w.date == date {
		return nil
	}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(w.dir, w.prefix+"-"+date+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	old := w.file
	w.file = file
	w.date = date
	if old != nil {
		return old.Close()
	}
	return nil
}

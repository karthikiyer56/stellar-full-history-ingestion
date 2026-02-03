// =============================================================================
// internal/workflow/logging/logger.go - Dual Logging Implementation
// =============================================================================
//
// This package provides a dual-output logger that writes:
//   - Informational messages (Info/Debug) to a log file
//   - Error messages (Error/Warn) to both error file and log file
//
// SCOPED LOGGING:
//   Loggers can be scoped with a prefix using WithScope(). This creates a child
//   logger that prefixes all messages with the scope name, e.g.:
//
//     logger := NewDualLogger("app.log", "error.log", false)
//     ingestionLog := logger.WithScope("INGEST")
//     ingestionLog.Info("Processing ledger 1000") // → [2006-01-02T15:04:05Z] [INGEST] Processing ledger 1000
//
//   The parent logger continues to work without the prefix:
//     logger.Info("Starting workflow") // → [2006-01-02T15:04:05Z] Starting workflow
//
// VERBOSE MODE:
//   When verbose=true, Debug messages are written to log file.
//   When verbose=false, Debug messages are discarded.
//
// =============================================================================

package logging

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// SeparatorLine is the visual separator used in logs
	SeparatorLine = "========================================================================="

	// TimeFormat is the ISO 8601 timestamp format for log messages (per CLAUDE.md)
	TimeFormat = "2006-01-02T15:04:05Z"
)

// =============================================================================
// DualLogger Implementation
// =============================================================================

// DualLogger implements the Logger interface with separate log and error files.
// Thread-safe for concurrent use.
type DualLogger struct {
	mu        sync.Mutex
	logFile   *os.File
	errorFile *os.File
	logPath   string
	errorPath string
	verbose   bool // If true, Debug messages are logged
}

// NewDualLogger creates a new DualLogger that writes to the specified files.
// If the files exist, they are truncated.
// verbose controls whether Debug() calls produce output.
func NewDualLogger(logPath, errorPath string, verbose bool) (*DualLogger, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open error file %s: %w", errorPath, err)
	}

	return &DualLogger{
		logFile:   logFile,
		errorFile: errorFile,
		logPath:   logPath,
		errorPath: errorPath,
		verbose:   verbose,
	}, nil
}

// NewTestLogger creates an in-memory logger for unit tests.
// Returns a logger that writes to os.Stdout and os.Stderr (both go to same stream for testing).
func NewTestLogger() *DualLogger {
	return &DualLogger{
		logFile:   os.Stdout,
		errorFile: os.Stderr,
		logPath:   "<test>",
		errorPath: "<test>",
		verbose:   true,
	}
}

// WithScope creates a scoped logger that prefixes all messages with the scope name.
// The returned ScopedLogger shares the same underlying files as the parent.
//
// Example:
//
//	ingestLog := logger.WithScope("INGEST")
//	ingestLog.Info("Starting") // → [2006-01-02T15:04:05Z] [INGEST] Starting
func (l *DualLogger) WithScope(scope string) interfaces.Logger {
	return &ScopedLogger{
		parent: l,
		scope:  scope,
	}
}

// Info logs an informational message to the log file only.
func (l *DualLogger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.logFile, "[%s] %s\n", timestamp, msg)
}

// Error logs an error message to both the error file and log file.
func (l *DualLogger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)

	fmt.Fprintf(l.errorFile, "[%s] [ERROR] %s\n", timestamp, msg)
	fmt.Fprintf(l.logFile, "[%s] [ERROR] %s\n", timestamp, msg)
}

// Debug logs a debug message to the log file (only if verbose=true).
func (l *DualLogger) Debug(format string, args ...interface{}) {
	if !l.verbose {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.logFile, "[%s] [DEBUG] %s\n", timestamp, msg)
}

// Warn logs a warning message to both error and log files.
func (l *DualLogger) Warn(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)

	fmt.Fprintf(l.errorFile, "[%s] [WARN] %s\n", timestamp, msg)
	fmt.Fprintf(l.logFile, "[%s] [WARN] %s\n", timestamp, msg)
}

// Separator logs a visual separator line to the log file.
func (l *DualLogger) Separator() {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintln(l.logFile, SeparatorLine)
}

// Sync forces a flush of all log data to disk.
func (l *DualLogger) Sync() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logFile.Sync()
	l.errorFile.Sync()
}

// Close closes all log files after syncing.
func (l *DualLogger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.logFile != nil && l.logFile != os.Stdout && l.logFile != os.Stderr {
		l.logFile.Sync()
		l.logFile.Close()
		l.logFile = nil
	}

	if l.errorFile != nil && l.errorFile != os.Stdout && l.errorFile != os.Stderr {
		l.errorFile.Sync()
		l.errorFile.Close()
		l.errorFile = nil
	}
}

// =============================================================================
// ScopedLogger - Logger with a Prefix
// =============================================================================

// ScopedLogger wraps a DualLogger and prefixes all messages with a scope name.
// This is useful for phase-specific logging where you want to identify the source.
//
// ScopedLogger shares the underlying files with its parent DualLogger.
// Closing the parent will close the files; do not close ScopedLogger directly.
type ScopedLogger struct {
	parent *DualLogger
	scope  string
}

// WithScope creates a nested scoped logger.
// The scopes are combined: parent.WithScope("A").WithScope("B") → [A:B]
func (l *ScopedLogger) WithScope(scope string) interfaces.Logger {
	return &ScopedLogger{
		parent: l.parent,
		scope:  l.scope + ":" + scope,
	}
}

// Info logs an informational message with the scope prefix.
func (l *ScopedLogger) Info(format string, args ...interface{}) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.parent.logFile, "[%s] [%s] %s\n", timestamp, l.scope, msg)
}

// Error logs an error message with the scope prefix to both files.
func (l *ScopedLogger) Error(format string, args ...interface{}) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)

	fmt.Fprintf(l.parent.errorFile, "[%s] [%s] [ERROR] %s\n", timestamp, l.scope, msg)
	fmt.Fprintf(l.parent.logFile, "[%s] [%s] [ERROR] %s\n", timestamp, l.scope, msg)
}

// Debug logs a debug message with the scope prefix (only if parent verbose=true).
func (l *ScopedLogger) Debug(format string, args ...interface{}) {
	if !l.parent.verbose {
		return
	}

	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.parent.logFile, "[%s] [%s] [DEBUG] %s\n", timestamp, l.scope, msg)
}

// Warn logs a warning message with the scope prefix to both files.
func (l *ScopedLogger) Warn(format string, args ...interface{}) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)

	fmt.Fprintf(l.parent.errorFile, "[%s] [%s] [WARN] %s\n", timestamp, l.scope, msg)
	fmt.Fprintf(l.parent.logFile, "[%s] [%s] [WARN] %s\n", timestamp, l.scope, msg)
}

// Separator logs a visual separator line (no scope prefix for separators).
func (l *ScopedLogger) Separator() {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	fmt.Fprintln(l.parent.logFile, SeparatorLine)
}

// Sync forces a flush of all log data to disk.
func (l *ScopedLogger) Sync() {
	l.parent.Sync()
}

// Close is a no-op for ScopedLogger. Close the parent DualLogger instead.
func (l *ScopedLogger) Close() {
	// No-op: ScopedLogger does not own the files
}

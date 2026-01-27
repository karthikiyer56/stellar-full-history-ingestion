// =============================================================================
// pkg/logging/logger.go - Dual Logging Implementation
// =============================================================================
//
// This package provides a dual-output logger that writes:
//   - Informational messages to a log file
//   - Error messages to a separate error file
//
// =============================================================================

package logging

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// SeparatorLine is the visual separator used in logs
	SeparatorLine = "========================================================================="

	// TimeFormat is the timestamp format for log messages
	TimeFormat = "2006-01-02 15:04:05.000"
)

// =============================================================================
// DualLogger Implementation
// =============================================================================

// DualLogger implements the Logger interface with separate log and error files.
type DualLogger struct {
	mu        sync.Mutex
	logFile   *os.File
	errorFile *os.File
	logPath   string
	errorPath string
}

// NewDualLogger creates a new DualLogger that writes to the specified files.
// If the files exist, they are truncated.
func NewDualLogger(logPath, errorPath string) (*DualLogger, error) {
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
	}, nil
}

// Info logs an informational message to the log file.
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

	fmt.Fprintf(l.errorFile, "[%s] ERROR: %s\n", timestamp, msg)
	fmt.Fprintf(l.logFile, "[%s] ERROR: %s\n", timestamp, msg)
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

	if l.logFile != nil {
		l.logFile.Sync()
		l.logFile.Close()
		l.logFile = nil
	}

	if l.errorFile != nil {
		l.errorFile.Sync()
		l.errorFile.Close()
		l.errorFile = nil
	}
}

// =============================================================================
// QueryLogger - Specialized Logger for Query Operations
// =============================================================================

// QueryLogger handles logging for SIGHUP query operations.
type QueryLogger struct {
	mu         sync.Mutex
	outputFile *os.File
	logFile    *os.File
	errorFile  *os.File
}

// NewQueryLogger creates a new QueryLogger.
func NewQueryLogger(outputPath, logPath, errorPath string) (*QueryLogger, error) {
	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open query output file %s: %w", outputPath, err)
	}

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		outputFile.Close()
		return nil, fmt.Errorf("failed to open query log file %s: %w", logPath, err)
	}

	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		outputFile.Close()
		logFile.Close()
		return nil, fmt.Errorf("failed to open query error file %s: %w", errorPath, err)
	}

	ql := &QueryLogger{
		outputFile: outputFile,
		logFile:    logFile,
		errorFile:  errorFile,
	}

	// Write CSV header
	fmt.Fprintln(ql.outputFile, "txHash,ledgerSeq,queryTimeUs")

	return ql, nil
}

// Result logs a successful query result to the CSV output file.
func (ql *QueryLogger) Result(txHashHex string, ledgerSeq uint32, queryTimeUs int64) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	fmt.Fprintf(ql.outputFile, "%s,%d,%d\n", txHashHex, ledgerSeq, queryTimeUs)
}

// NotFound logs a query for a txHash that was not found.
func (ql *QueryLogger) NotFound(txHashHex string, queryTimeUs int64) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	fmt.Fprintf(ql.outputFile, "%s,-1,%d\n", txHashHex, queryTimeUs)
}

// Stats logs query statistics to the statistics log file.
func (ql *QueryLogger) Stats(format string, args ...interface{}) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(ql.logFile, "[%s] %s\n", timestamp, msg)
}

// Error logs an error to the query error file.
func (ql *QueryLogger) Error(format string, args ...interface{}) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(ql.errorFile, "[%s] ERROR: %s\n", timestamp, msg)
}

// Sync forces a flush of all query log data to disk.
func (ql *QueryLogger) Sync() {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	ql.outputFile.Sync()
	ql.logFile.Sync()
	ql.errorFile.Sync()
}

// Close closes all query log files after syncing.
func (ql *QueryLogger) Close() {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	if ql.outputFile != nil {
		ql.outputFile.Sync()
		ql.outputFile.Close()
		ql.outputFile = nil
	}
	if ql.logFile != nil {
		ql.logFile.Sync()
		ql.logFile.Close()
		ql.logFile = nil
	}
	if ql.errorFile != nil {
		ql.errorFile.Sync()
		ql.errorFile.Close()
		ql.errorFile = nil
	}
}

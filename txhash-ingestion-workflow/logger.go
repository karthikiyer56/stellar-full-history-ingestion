// =============================================================================
// logger.go - Dual Logging Implementation
// =============================================================================
//
// This file provides a dual-output logger that writes:
//   - Informational messages to a log file
//   - Error messages to a separate error file
//
// DESIGN PHILOSOPHY:
//
//   The logger is designed for long-running batch processes where:
//   1. Operators need to monitor progress via the log file
//   2. Errors need to be easily findable in a separate file
//   3. Logs should be human-readable with timestamps
//   4. Visual separators help demarcate phases
//
// OUTPUT FORMAT:
//
//   Log messages follow this format:
//     [2024-01-15 14:30:45.123] message text here
//
//   Separators look like:
//     =========================================================================
//
// CONCURRENCY:
//
//   The logger is safe for concurrent use from multiple goroutines.
//   All write operations are protected by a mutex.
//
// FLUSHING:
//
//   Logs are buffered for performance. Call Sync() to force a flush.
//   Close() automatically syncs before closing files.
//
// =============================================================================

package main

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
	// separatorLine is the visual separator used in logs
	separatorLine = "========================================================================="

	// timeFormat is the timestamp format for log messages
	timeFormat = "2006-01-02 15:04:05.000"
)

// =============================================================================
// DualLogger Implementation
// =============================================================================

// DualLogger implements the Logger interface with separate log and error files.
//
// USAGE:
//
//	logger, err := NewDualLogger("/path/to/app.log", "/path/to/app.err")
//	if err != nil {
//	    // handle error
//	}
//	defer logger.Close()
//
//	logger.Info("Starting process...")
//	logger.Error("Something went wrong: %v", err)
//	logger.Separator()
//	logger.Sync() // force flush
type DualLogger struct {
	// mu protects all fields
	mu sync.Mutex

	// logFile is the file handle for informational messages
	logFile *os.File

	// errorFile is the file handle for error messages
	errorFile *os.File

	// logPath stores the path to the log file (for error messages)
	logPath string

	// errorPath stores the path to the error file (for error messages)
	errorPath string
}

// NewDualLogger creates a new DualLogger that writes to the specified files.
//
// If the files exist, they are truncated. This matches the requirement that
// query output files are "recreated fresh on each restart".
//
// PARAMETERS:
//   - logPath: Path to the informational log file
//   - errorPath: Path to the error log file
//
// RETURNS:
//   - A new DualLogger instance
//   - An error if file creation fails
func NewDualLogger(logPath, errorPath string) (*DualLogger, error) {
	// Open log file (create or truncate)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	// Open error file (create or truncate)
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
//
// The message is prefixed with a timestamp:
//
//	[2024-01-15 14:30:45.123] Your message here
//
// Supports printf-style formatting.
// Writes are unbuffered for immediate visibility.
func (l *DualLogger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(timeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.logFile, "[%s] %s\n", timestamp, msg)
}

// Error logs an error message to the error file.
//
// The message is prefixed with a timestamp:
//
//	[2024-01-15 14:30:45.123] ERROR: Your error message here
//
// Errors are also written to the log file (for context).
// Supports printf-style formatting.
// Writes are unbuffered for immediate visibility.
func (l *DualLogger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(timeFormat)
	msg := fmt.Sprintf(format, args...)

	// Write to error file
	fmt.Fprintf(l.errorFile, "[%s] ERROR: %s\n", timestamp, msg)

	// Also write to log file for context
	fmt.Fprintf(l.logFile, "[%s] ERROR: %s\n", timestamp, msg)
}

// Separator logs a visual separator line to the log file.
//
// Output:
//
//	=========================================================================
//
// Use separators to demarcate phases or sections in the log.
// Writes are unbuffered for immediate visibility.
func (l *DualLogger) Separator() {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintln(l.logFile, separatorLine)
}

// Sync forces a flush of all log data to disk (fsync).
//
// Call this:
//   - After important checkpoints
//   - Before operations that might crash
//   - Periodically during long operations
//
// With unbuffered logging, this performs an fsync to ensure
// data is durably written to disk.
func (l *DualLogger) Sync() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logFile.Sync()
	l.errorFile.Sync()
}

// Close closes all log files after syncing.
//
// Always call Close() when done logging (use defer):
//
//	logger, err := NewDualLogger(...)
//	defer logger.Close()
//
// It is safe to call Close() multiple times.
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
//
// This is a separate logger because:
//   - Query results go to a CSV file (different format)
//   - Query statistics go to a separate log
//   - Query errors go to a separate error file
//
// This separation allows:
//   - CSV output to be parsed programmatically
//   - Main log file to not be cluttered with query details
//
// CSV OUTPUT FORMAT:
//
//	txHash,ledgerSeq,queryTimeUs
//	abc123...,12345678,42
//	def456...,87654321,38
type QueryLogger struct {
	// mu protects all fields
	mu sync.Mutex

	// outputFile is for CSV results
	outputFile *os.File

	// logFile is for query statistics
	logFile *os.File

	// errorFile is for query errors
	errorFile *os.File
}

// NewQueryLogger creates a new QueryLogger.
//
// All files are truncated on creation (not appended).
// This matches the requirement that query files are "recreated fresh on each restart".
//
// PARAMETERS:
//   - outputPath: Path for CSV results (txHash,ledgerSeq,queryTimeUs)
//   - logPath: Path for query statistics
//   - errorPath: Path for query errors
func NewQueryLogger(outputPath, logPath, errorPath string) (*QueryLogger, error) {
	// Open output file (CSV results)
	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open query output file %s: %w", outputPath, err)
	}

	// Open log file (statistics)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		outputFile.Close()
		return nil, fmt.Errorf("failed to open query log file %s: %w", logPath, err)
	}

	// Open error file
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
//
// FORMAT: txHash,ledgerSeq,queryTimeUs
//
// PARAMETERS:
//   - txHashHex: Transaction hash as hex string (64 characters)
//   - ledgerSeq: Ledger sequence number
//   - queryTimeUs: Query duration in microseconds
//
// Writes are unbuffered for immediate visibility.
func (ql *QueryLogger) Result(txHashHex string, ledgerSeq uint32, queryTimeUs int64) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	fmt.Fprintf(ql.outputFile, "%s,%d,%d\n", txHashHex, ledgerSeq, queryTimeUs)
}

// NotFound logs a query for a txHash that was not found.
//
// This goes to the CSV output with ledgerSeq=-1 to indicate not found.
//
// PARAMETERS:
//   - txHashHex: Transaction hash as hex string (64 characters)
//   - queryTimeUs: Query duration in microseconds
//
// Writes are unbuffered for immediate visibility.
func (ql *QueryLogger) NotFound(txHashHex string, queryTimeUs int64) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	fmt.Fprintf(ql.outputFile, "%s,-1,%d\n", txHashHex, queryTimeUs)
}

// Stats logs query statistics to the statistics log file.
//
// Supports printf-style formatting.
// Writes are unbuffered for immediate visibility.
func (ql *QueryLogger) Stats(format string, args ...interface{}) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	timestamp := time.Now().Format(timeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(ql.logFile, "[%s] %s\n", timestamp, msg)
}

// Error logs an error to the query error file.
//
// Supports printf-style formatting.
// Writes are unbuffered for immediate visibility.
func (ql *QueryLogger) Error(format string, args ...interface{}) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	timestamp := time.Now().Format(timeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(ql.errorFile, "[%s] ERROR: %s\n", timestamp, msg)
}

// Sync forces a flush of all query log data to disk (fsync).
//
// With unbuffered logging, this performs an fsync to ensure
// data is durably written to disk.
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

// =============================================================================
// Ensure Interfaces are Implemented
// =============================================================================

// Compile-time check that DualLogger implements Logger
var _ Logger = (*DualLogger)(nil)

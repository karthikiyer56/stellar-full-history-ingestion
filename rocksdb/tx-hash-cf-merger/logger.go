// logger.go
// =============================================================================
// Dual Logger for Info and Error Logging
// =============================================================================
//
// This module provides a dual-output logger that:
// - Writes INFO logs to stdout and optionally to a log file
// - Writes ERROR logs to stderr and optionally to an error file
// - Supports log.Logger interface for consistent logging
//
// =============================================================================

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// =============================================================================
// DualLogger
// =============================================================================

// DualLogger provides separate logging for info and error messages.
type DualLogger struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger

	infoFile  *os.File
	errorFile *os.File

	mu sync.Mutex
}

// LoggerConfig holds configuration for the dual logger.
type LoggerConfig struct {
	LogFilePath   string // Path to info log file (optional)
	ErrorFilePath string // Path to error log file (optional)
}

// NewDualLogger creates a new DualLogger with optional file outputs.
func NewDualLogger(cfg LoggerConfig) (*DualLogger, error) {
	dl := &DualLogger{}

	// Setup info logger
	var infoWriters []io.Writer
	infoWriters = append(infoWriters, os.Stdout)

	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", cfg.LogFilePath, err)
		}
		dl.infoFile = f
		infoWriters = append(infoWriters, f)
	}

	// Setup error logger
	var errorWriters []io.Writer
	errorWriters = append(errorWriters, os.Stderr)

	if cfg.ErrorFilePath != "" {
		f, err := os.OpenFile(cfg.ErrorFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			if dl.infoFile != nil {
				dl.infoFile.Close()
			}
			return nil, fmt.Errorf("failed to open error file %s: %w", cfg.ErrorFilePath, err)
		}
		dl.errorFile = f
		errorWriters = append(errorWriters, f)
	}

	// Create loggers
	dl.infoLogger = log.New(io.MultiWriter(infoWriters...), "", log.LstdFlags)
	dl.errorLogger = log.New(io.MultiWriter(errorWriters...), "", log.LstdFlags)

	return dl, nil
}

// Close closes any open log files.
func (dl *DualLogger) Close() {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.infoFile != nil {
		dl.infoFile.Close()
		dl.infoFile = nil
	}
	if dl.errorFile != nil {
		dl.errorFile.Close()
		dl.errorFile = nil
	}
}

// Info logs an info message.
func (dl *DualLogger) Info(format string, args ...interface{}) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.infoLogger.Printf(format, args...)
}

// Error logs an error message.
func (dl *DualLogger) Error(format string, args ...interface{}) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.errorLogger.Printf("ERROR: "+format, args...)
}

// =============================================================================
// Progress Logging
// =============================================================================

// ProgressLogger provides formatted progress updates.
type ProgressLogger struct {
	logger       *DualLogger
	startTime    time.Time
	lastLogTime  time.Time
	logInterval  time.Duration
	totalItems   int64
	lastLogItems int64

	mu sync.Mutex
}

// NewProgressLogger creates a new progress logger.
func NewProgressLogger(logger *DualLogger, totalItems int64, logInterval time.Duration) *ProgressLogger {
	return &ProgressLogger{
		logger:      logger,
		startTime:   time.Now(),
		lastLogTime: time.Now(),
		logInterval: logInterval,
		totalItems:  totalItems,
	}
}

// ShouldLog returns true if enough time has passed or enough progress made.
func (pl *ProgressLogger) ShouldLog(currentItems int64) bool {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	elapsed := time.Since(pl.lastLogTime)

	// Log every interval OR every 1% progress
	progressPct := float64(currentItems-pl.lastLogItems) / float64(pl.totalItems) * 100
	return elapsed >= pl.logInterval || progressPct >= 1.0
}

// LogProgress logs a formatted progress update.
func (pl *ProgressLogger) LogProgress(ledgersProcessed, transactionsProcessed int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(pl.startTime)
	intervalElapsed := now.Sub(pl.lastLogTime)

	// Calculate rates
	ledgerRate := float64(ledgersProcessed) / elapsed.Seconds()
	txRate := float64(transactionsProcessed) / elapsed.Seconds()

	// Calculate ETA
	pct := float64(ledgersProcessed) / float64(pl.totalItems) * 100
	var eta time.Duration
	if ledgerRate > 0 && pct < 100 {
		remaining := float64(pl.totalItems - ledgersProcessed)
		eta = time.Duration(remaining/ledgerRate) * time.Second
	}

	// Format and log
	pl.logger.Info("================================================================================")
	pl.logger.Info("    PROGRESS: %.2f%% | %s / %s ledgers",
		pct,
		formatNumber(ledgersProcessed),
		formatNumber(pl.totalItems))
	pl.logger.Info("    Transactions: %s | Rate: %s ledgers/sec | %s tx/sec",
		formatNumber(transactionsProcessed),
		formatNumber(int64(ledgerRate)),
		formatNumber(int64(txRate)))
	pl.logger.Info("    Elapsed: %s | ETA: %s",
		formatDuration(elapsed),
		formatDuration(eta))
	pl.logger.Info("================================================================================")

	pl.lastLogTime = now
	pl.lastLogItems = ledgersProcessed

	// Avoid unused variable warning
	_ = intervalElapsed
}

// =============================================================================
// Helper Functions (local copies to avoid circular imports)
// =============================================================================

func formatNumber(n int64) string {
	if n < 0 {
		return fmt.Sprintf("-%s", formatNumber(-n))
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	// Format with commas
	s := fmt.Sprintf("%d", n)
	result := make([]byte, 0, len(s)+(len(s)-1)/3)

	leadingDigits := len(s) % 3
	if leadingDigits == 0 {
		leadingDigits = 3
	}

	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}

	return string(result)
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "N/A"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, mins)
}

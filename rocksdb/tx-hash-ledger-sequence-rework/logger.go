package txhashrework

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

// =============================================================================
// Logger
// =============================================================================

// Logger provides separate logging for info and error messages.
//
// Logging behavior:
//   - If log file is specified: logs go ONLY to the file (not stdout)
//   - If log file is NOT specified: logs go to stdout
//   - If error file is specified: errors go ONLY to the file (not stderr)
//   - If error file is NOT specified: errors go to stderr
type Logger struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger

	infoFile  *os.File
	errorFile *os.File

	mu sync.Mutex
}

// LoggerConfig holds configuration for the logger.
type LoggerConfig struct {
	LogFilePath   string // Path to info log file (if set, logs ONLY to file)
	ErrorFilePath string // Path to error log file (if set, errors ONLY to file)
}

// NewLogger creates a new Logger with the specified configuration.
//
// If LogFilePath is set, info logs go ONLY to that file.
// If LogFilePath is empty, info logs go to stdout.
//
// If ErrorFilePath is set, error logs go ONLY to that file.
// If ErrorFilePath is empty, error logs go to stderr.
func NewLogger(cfg LoggerConfig) (*Logger, error) {
	l := &Logger{}

	// Setup info logger
	var infoWriter io.Writer
	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", cfg.LogFilePath, err)
		}
		l.infoFile = f
		infoWriter = f
	} else {
		infoWriter = os.Stdout
	}

	// Setup error logger
	var errorWriter io.Writer
	if cfg.ErrorFilePath != "" {
		f, err := os.OpenFile(cfg.ErrorFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			if l.infoFile != nil {
				l.infoFile.Close()
			}
			return nil, fmt.Errorf("failed to open error file %s: %w", cfg.ErrorFilePath, err)
		}
		l.errorFile = f
		errorWriter = f
	} else {
		errorWriter = os.Stderr
	}

	l.infoLogger = log.New(infoWriter, "", log.LstdFlags)
	l.errorLogger = log.New(errorWriter, "", log.LstdFlags)

	return l, nil
}

// Close closes any open log files.
func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.infoFile != nil {
		l.infoFile.Close()
		l.infoFile = nil
	}
	if l.errorFile != nil {
		l.errorFile.Close()
		l.errorFile = nil
	}
}

// Info logs an info message.
func (l *Logger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infoLogger.Printf(format, args...)
}

// Error logs an error message.
func (l *Logger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errorLogger.Printf("ERROR: "+format, args...)
}

// =============================================================================
// Progress Logger
// =============================================================================

// ProgressLogger provides formatted progress updates.
type ProgressLogger struct {
	logger       *Logger
	startTime    time.Time
	lastLogTime  time.Time
	logInterval  time.Duration
	totalItems   int64
	lastLogItems int64

	mu sync.Mutex
}

// NewProgressLogger creates a new progress logger.
func NewProgressLogger(logger *Logger, totalItems int64, logInterval time.Duration) *ProgressLogger {
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

	// Calculate rates
	ledgerRate := float64(0)
	txRate := float64(0)
	if elapsed.Seconds() > 0 {
		ledgerRate = float64(ledgersProcessed) / elapsed.Seconds()
		txRate = float64(transactionsProcessed) / elapsed.Seconds()
	}

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
		helpers.FormatNumber(ledgersProcessed),
		helpers.FormatNumber(pl.totalItems))
	pl.logger.Info("    Transactions: %s | Rate: %s ledgers/sec | %s tx/sec",
		helpers.FormatNumber(transactionsProcessed),
		helpers.FormatNumber(int64(ledgerRate)),
		helpers.FormatNumber(int64(txRate)))
	pl.logger.Info("    Elapsed: %s | ETA: %s",
		helpers.FormatDuration(elapsed),
		helpers.FormatDuration(eta))
	pl.logger.Info("================================================================================")

	pl.lastLogTime = now
	pl.lastLogItems = ledgersProcessed
}

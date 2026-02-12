package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDualLogger_Info(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Info("test message %d", 42)
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("test message 42")) {
		t.Errorf("Expected 'test message 42' in log file, got: %s", string(logContent))
	}

	errContent, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("Failed to read error file: %v", err)
	}

	if len(errContent) > 0 {
		t.Errorf("Expected error file to be empty, got: %s", string(errContent))
	}
}

func TestDualLogger_Error(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Error("error message %s", "test")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("error message test")) {
		t.Errorf("Expected 'error message test' in log file, got: %s", string(logContent))
	}

	if !bytes.Contains(logContent, []byte("[ERROR]")) {
		t.Errorf("Expected '[ERROR]' tag in log file, got: %s", string(logContent))
	}

	errContent, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("Failed to read error file: %v", err)
	}

	if !bytes.Contains(errContent, []byte("error message test")) {
		t.Errorf("Expected 'error message test' in error file, got: %s", string(errContent))
	}

	if !bytes.Contains(errContent, []byte("[ERROR]")) {
		t.Errorf("Expected '[ERROR]' tag in error file, got: %s", string(errContent))
	}
}

func TestDualLogger_Warn(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Warn("warning %d", 123)
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("warning 123")) {
		t.Errorf("Expected 'warning 123' in log file, got: %s", string(logContent))
	}

	if !bytes.Contains(logContent, []byte("[WARN]")) {
		t.Errorf("Expected '[WARN]' tag in log file, got: %s", string(logContent))
	}

	errContent, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("Failed to read error file: %v", err)
	}

	if !bytes.Contains(errContent, []byte("warning 123")) {
		t.Errorf("Expected 'warning 123' in error file, got: %s", string(errContent))
	}
}

func TestDualLogger_Debug_Verbose(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, true)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Debug("debug message %s", "verbose")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("debug message verbose")) {
		t.Errorf("Expected 'debug message verbose' in log file with verbose=true, got: %s", string(logContent))
	}

	if !bytes.Contains(logContent, []byte("[DEBUG]")) {
		t.Errorf("Expected '[DEBUG]' tag in log file, got: %s", string(logContent))
	}
}

func TestDualLogger_Debug_NonVerbose(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Debug("debug message %s", "silent")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if bytes.Contains(logContent, []byte("debug message silent")) {
		t.Errorf("Expected 'debug message silent' NOT in log file with verbose=false, got: %s", string(logContent))
	}
}

func TestDualLogger_Separator(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Separator()
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte(SeparatorLine)) {
		t.Errorf("Expected separator line in log file, got: %s", string(logContent))
	}
}

func TestDualLogger_WithScope_Prefix(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	scoped := logger.WithScope("INGEST")
	scoped.Info("processing ledger %d", 1000)
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("[INGEST]")) {
		t.Errorf("Expected '[INGEST]' scope prefix in log, got: %s", string(logContent))
	}

	if !bytes.Contains(logContent, []byte("processing ledger 1000")) {
		t.Errorf("Expected 'processing ledger 1000' in log, got: %s", string(logContent))
	}
}

func TestDualLogger_WithScope_NestedScopes(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	scoped1 := logger.WithScope("PHASE1")
	scoped2 := scoped1.WithScope("PHASE2")
	scoped2.Info("nested scope message")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("[PHASE1:PHASE2]")) {
		t.Errorf("Expected '[PHASE1:PHASE2]' nested scope prefix, got: %s", string(logContent))
	}
}

func TestDualLogger_WithScope_Error(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	scoped := logger.WithScope("COMPACT")
	scoped.Error("compaction failed: %s", "disk full")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("[COMPACT]")) {
		t.Errorf("Expected '[COMPACT]' scope in log, got: %s", string(logContent))
	}

	if !bytes.Contains(logContent, []byte("[ERROR]")) {
		t.Errorf("Expected '[ERROR]' tag in log, got: %s", string(logContent))
	}

	if !bytes.Contains(logContent, []byte("compaction failed: disk full")) {
		t.Errorf("Expected error message in log, got: %s", string(logContent))
	}

	errContent, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("Failed to read error file: %v", err)
	}

	if !bytes.Contains(errContent, []byte("[COMPACT]")) {
		t.Errorf("Expected '[COMPACT]' scope in error file, got: %s", string(errContent))
	}

	if !bytes.Contains(errContent, []byte("[ERROR]")) {
		t.Errorf("Expected '[ERROR]' tag in error file, got: %s", string(errContent))
	}
}

func TestDualLogger_TimestampFormat(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Info("timestamp test")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("T")) && !bytes.Contains(logContent, []byte("Z")) {
		t.Errorf("Expected ISO 8601 timestamp format (with T and Z), got: %s", string(logContent))
	}
}

func TestTestLogger_Basic(t *testing.T) {
	logger := NewTestLogger()
	logger.Info("test info")
	logger.Error("test error")
	logger.Debug("test debug")
	logger.Warn("test warn")
	logger.Separator()

	logger.Close()
}

func TestDualLogger_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(n int) {
			logger.Info("message %d", n)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if len(logContent) == 0 {
		t.Errorf("Expected log content from concurrent writes")
	}
}

func TestScopedLogger_StillWritesAfterParentScope(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	errPath := filepath.Join(tmpDir, "test.err")

	logger, err := NewDualLogger(logPath, errPath, false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	scoped := logger.WithScope("TEST")
	scoped.Info("first message")

	scoped.Info("second message")
	logger.Sync()

	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(logContent, []byte("first message")) || !bytes.Contains(logContent, []byte("second message")) {
		t.Errorf("Expected both messages to be logged, got: %s", string(logContent))
	}
}

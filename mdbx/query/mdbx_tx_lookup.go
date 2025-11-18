package main

// Filename: mdbx_tx_lookup.go
// Usage: go run mdbx_tx_lookup.go --db /path/to/db --tx abc123...

import (
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/karthikiyer56/stellar-full-history-ingestion/tx_data"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

func main() {
	var dbPath, txHashHex string

	flag.StringVar(&dbPath, "db", "", "txHash->txData MDBX database path")
	flag.StringVar(&txHashHex, "tx", "", "tx hash in hex")
	flag.Parse()

	if dbPath == "" || txHashHex == "" {
		log.Fatal("Usage: go run mdbx_tx_lookup.go --db /path/to/db --tx abc12...")
	}

	// Convert hex string to binary bytes
	txHashBytes, err := hexStringToBytes(txHashHex)
	if err != nil {
		log.Fatalf("Failed to convert tx hash to bytes: %v", err)
	}

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Error: DB directory does not exist: %s", dbPath)
	}

	// Open MDBX environment
	env, err := mdbx.NewEnv()
	if err != nil {
		log.Fatalf("Failed to create MDBX environment: %v", err)
	}
	defer env.Close()

	// Set max DBs
	t1 := time.Now()
	err = env.SetOption(mdbx.OptMaxDB, uint64(2))
	if err != nil {
		log.Fatalf("Failed to set max DBs: %v", err)
	}
	setOptTime := time.Since(t1)

	// Open in read-only mode
	t2 := time.Now()
	err = env.Open(dbPath, mdbx.Readonly|mdbx.Accede, 0644)
	if err != nil {
		log.Fatalf("Failed to open MDBX database: %v", err)
	}
	envOpenTime := time.Since(t2)

	// First DBI open
	t3 := time.Now()
	var dbi mdbx.DBI
	err = env.View(func(txn *mdbx.Txn) error {
		var err error
		dbi, err = txn.OpenDBI("data", 0, nil, nil)
		return err
	})
	if err != nil {
		log.Fatalf("Failed to open DBI: %v", err)
	}
	dbiOpenTime := time.Since(t3)

	totalOpenTime := setOptTime + envOpenTime + dbiOpenTime

	// Get database stats
	var statsBefore mdbx.Stat
	var envInfoBefore *mdbx.EnvInfo

	err = env.View(func(txn *mdbx.Txn) error {
		stat, err := txn.StatDBI(dbi)
		if err != nil {
			return err
		}
		statsBefore = *stat

		info, err := env.Info(txn)
		if err != nil {
			return err
		}
		envInfoBefore = info

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to get initial stats: %v", err)
	}

	// Query for the transaction
	var data []byte
	var found bool

	queryStart := time.Now()
	err = env.View(func(txn *mdbx.Txn) error {
		val, err := txn.Get(dbi, txHashBytes)
		if err != nil {
			if mdbx.IsNotFound(err) {
				found = false
				return nil
			}
			return err
		}

		// Copy the data since it's only valid within the transaction
		data = make([]byte, len(val))
		copy(data, val)
		found = true

		return nil
	})
	queryTime := time.Since(queryStart)

	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}

	if !found {
		fmt.Printf("\n========================================\n")
		fmt.Printf("Transaction not found in DB\n")
		fmt.Printf("========================================\n")
		fmt.Printf("TX Hash: %s\n", txHashHex)
		fmt.Printf("DB Path: %s\n", dbPath)
		fmt.Printf("\nOpen Timing Breakdown:\n")
		fmt.Printf("  SetOption:    %s\n", formatDuration(setOptTime))
		fmt.Printf("  env.Open:     %s\n", formatDuration(envOpenTime))
		fmt.Printf("  DBI open:     %s\n", formatDuration(dbiOpenTime))
		fmt.Printf("  Total open:   %s\n", formatDuration(totalOpenTime))
		fmt.Printf("  Query time:   %s\n", formatDuration(queryTime))
		fmt.Printf("========================================\n\n")
		return
	}

	// Decompress the data
	decompressStart := time.Now()
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatalf("Failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()

	decompressedData, err := decoder.DecodeAll(data, nil)
	if err != nil {
		log.Fatalf("Failed to decompress data: %v", err)
	}
	decompressTime := time.Since(decompressStart)

	// Unmarshal TxData proto
	unmarshalStart := time.Now()
	var txData tx_data.TxData
	if err := proto.Unmarshal(decompressedData, &txData); err != nil {
		log.Fatalf("Failed to unmarshal TxData: %v", err)
	}
	unmarshalTime := time.Since(unmarshalStart)

	// Convert transaction components to base64
	txEnvelopeBase64 := base64.StdEncoding.EncodeToString(txData.TxEnvelope)
	txResultBase64 := base64.StdEncoding.EncodeToString(txData.TxResult)
	txMetaBase64 := base64.StdEncoding.EncodeToString(txData.TxMeta)

	totalTime := totalOpenTime + queryTime + decompressTime + unmarshalTime

	// Display results
	fmt.Printf("\n========================================\n")
	fmt.Printf("MDBX Transaction Lookup\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Transaction Hash: %s\n", txHashHex)
	fmt.Printf("DB Path: %s\n", dbPath)
	fmt.Printf("========================================\n")

	fmt.Printf("\nTransaction Details:\n")
	fmt.Printf("  Ledger sequence:    %d\n", txData.LedgerSequence)
	fmt.Printf("  Transaction index:  %d\n", txData.Index)
	fmt.Printf("  Closed at:          %v\n", txData.ClosedAt.AsTime())

	fmt.Printf("\nData Sizes:\n")
	fmt.Printf("  Compressed:         %s\n", formatBytes(int64(len(data))))
	fmt.Printf("  Uncompressed:       %s\n", formatBytes(int64(len(decompressedData))))
	fmt.Printf("  Compression ratio:  %.2f%%\n", 100*(1-float64(len(data))/float64(len(decompressedData))))

	fmt.Printf("\nComponent Sizes:\n")
	fmt.Printf("  Envelope:           %s\n", formatBytes(int64(len(txData.TxEnvelope))))
	fmt.Printf("  Result:             %s\n", formatBytes(int64(len(txData.TxResult))))
	fmt.Printf("  Meta:               %s\n", formatBytes(int64(len(txData.TxMeta))))

	fmt.Printf("\n========================================\n")
	fmt.Printf("MDBX Database Statistics\n")
	fmt.Printf("========================================\n")
	fmt.Printf("  Page size:          %d bytes\n", statsBefore.PSize)
	fmt.Printf("  Tree depth:         %d\n", statsBefore.Depth)
	fmt.Printf("  Branch pages:       %s\n", formatNumber(int64(statsBefore.BranchPages)))
	fmt.Printf("  Leaf pages:         %s\n", formatNumber(int64(statsBefore.LeafPages)))
	fmt.Printf("  Overflow pages:     %s\n", formatNumber(int64(statsBefore.OverflowPages)))
	fmt.Printf("  Total entries:      %s\n", formatNumber(int64(statsBefore.Entries)))
	fmt.Printf("  Map size:           %s\n", formatBytes(int64(envInfoBefore.MapSize)))

	// Calculate pages read for this query
	pagesRead := statsBefore.Depth
	overflowPagesRead := uint((len(data) / int(statsBefore.PSize)) + 1)
	if len(data) > int(statsBefore.PSize)/2 {
		pagesRead += overflowPagesRead
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Query Performance Metrics\n")
	fmt.Printf("========================================\n")
	fmt.Printf("\nOpen Timing Breakdown:\n")
	fmt.Printf("  SetOption:          %s\n", formatDuration(setOptTime))
	fmt.Printf("  env.Open:           %s\n", formatDuration(envOpenTime))
	fmt.Printf("  DBI open:           %s\n", formatDuration(dbiOpenTime))
	fmt.Printf("  Total open:         %s\n", formatDuration(totalOpenTime))
	fmt.Printf("\nQuery Timing:\n")
	fmt.Printf("  Query time:         %s\n", formatDuration(queryTime))
	fmt.Printf("  Decompress time:    %s\n", formatDuration(decompressTime))
	fmt.Printf("  Unmarshal time:     %s\n", formatDuration(unmarshalTime))
	fmt.Printf("  Total time:         %s\n", formatDuration(totalTime))
	fmt.Printf("\nI/O Metrics:\n")
	fmt.Printf("  Pages traversed:    %d (tree depth)\n", statsBefore.Depth)
	fmt.Printf("  Est. pages read:    %d\n", pagesRead)
	fmt.Printf("  Bytes read (data):  %s\n", formatBytes(int64(len(data))))
	fmt.Printf("  Est. I/O (pages):   %s\n", formatBytes(int64(pagesRead)*int64(statsBefore.PSize)))

	fmt.Printf("\n========================================\n")
	fmt.Printf("Transaction Data (Base64 Encoded)\n")
	fmt.Printf("========================================\n")
	fmt.Printf("\nTxEnvelope (base64):\n%s\n", wrapText(txEnvelopeBase64, 80))
	fmt.Printf("\nTxResult (base64):\n%s\n", wrapText(txResultBase64, 80))
	fmt.Printf("\nTxMeta (base64):\n%s\n", wrapText(txMetaBase64, 80))
	fmt.Printf("========================================\n\n")
}

// hexStringToBytes converts a hex string to bytes
func hexStringToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
}

// wrapText wraps text at the specified width
func wrapText(text string, width int) string {
	if len(text) <= width {
		return text
	}

	result := ""
	for i := 0; i < len(text); i += width {
		end := i + width
		if end > len(text) {
			end = len(text)
		}
		result += text[i:end] + "\n"
	}
	return result
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
	}

	d = d.Round(time.Millisecond)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	d -= s * time.Second
	ms := d / time.Millisecond

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds %dms", h, m, s, ms)
	} else if m > 0 {
		return fmt.Sprintf("%dm %ds %dms", m, s, ms)
	} else if s > 0 {
		return fmt.Sprintf("%ds %dms", s, ms)
	}
	return fmt.Sprintf("%dms", ms)
}

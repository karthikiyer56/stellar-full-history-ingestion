package main

// Filename: mdbx_tx_lookup.go
// Usage:
//   Single query:
//     go run mdbx_tx_lookup.go --db /path/to/db --tx abc123...
//
//   Ongoing stdin:
//     go run mdbx_tx_lookup.go --db /path/to/db --ongoing
//     (then type hashes or pipe them)
//
//   Quiet mode:
//     go run mdbx_tx_lookup.go --db /path --ongoing --quiet

import (
	"bufio"
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
	var ongoing bool
	var quiet bool

	flag.StringVar(&dbPath, "db", "", "txHash->txData MDBX database path")
	flag.StringVar(&txHashHex, "tx", "", "tx hash in hex (omit when using --ongoing)")
	flag.BoolVar(&ongoing, "ongoing", false, "read transaction hashes from stdin continuously")
	flag.BoolVar(&quiet, "quiet", false, "print only metrics instead of full tx data output")

	flag.Parse()

	if dbPath == "" {
		log.Fatalf("Missing required --db path")
	}
	if ongoing && txHashHex != "" {
		log.Fatalf("--tx cannot be used when --ongoing is provided")
	}
	if !ongoing && txHashHex == "" {
		log.Fatalf("Provide --tx OR --ongoing")
	}

	// Check DB exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Error: DB directory does not exist: %s", dbPath)
	}

	// Open environment
	env, err := mdbx.NewEnv()
	if err != nil {
		log.Fatalf("Failed to create MDBX env: %v", err)
	}
	defer env.Close()

	t1 := time.Now()
	err = env.SetOption(mdbx.OptMaxDB, uint64(2))
	if err != nil {
		log.Fatalf("Failed SetOption: %v", err)
	}
	setOptTime := time.Since(t1)

	t2 := time.Now()
	err = env.Open(dbPath, mdbx.Readonly|mdbx.Accede, 0644)
	if err != nil {
		log.Fatalf("Failed env.Open: %v", err)
	}
	envOpenTime := time.Since(t2)

	var dbi mdbx.DBI
	t3 := time.Now()
	err = env.View(func(txn *mdbx.Txn) error {
		var err error
		dbi, err = txn.OpenDBI("data", 0, nil, nil)
		return err
	})
	if err != nil {
		log.Fatalf("Failed to open DBI: %v", err)
	}
	dbiOpenTime := time.Since(t3)

	totalOpen := setOptTime + envOpenTime + dbiOpenTime

	// Stats before
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
		log.Fatalf("Failed to get stats: %v", err)
	}

	// Print initialization once
	fmt.Printf("\n=== MDBX Initialization ===\n")
	fmt.Printf("SetOption:    %s\n", formatDuration(setOptTime))
	fmt.Printf("env.Open:     %s\n", formatDuration(envOpenTime))
	fmt.Printf("DBI open:     %s\n", formatDuration(dbiOpenTime))
	fmt.Printf("Total open:   %s\n\n", formatDuration(totalOpen))

	// ------------------------------------------------------------
	// ONGOING MODE
	// ------------------------------------------------------------
	if ongoing {
		reader := bufio.NewScanner(os.Stdin)

		fmt.Println("Ongoing mode: enter tx hashes (or STOP to quit):")
		for reader.Scan() {
			line := strings.TrimSpace(reader.Text())
			if line == "" {
				continue
			}
			if strings.EqualFold(line, "STOP") {
				fmt.Println("Stopping.")
				return
			}

			runQuery(env, dbi, statsBefore, envInfoBefore, line, quiet)
		}
		return
	}

	// ------------------------------------------------------------
	// SINGLE QUERY MODE
	// ------------------------------------------------------------
	runQuery(env, dbi, statsBefore, envInfoBefore, txHashHex, quiet)
}

///////////////////////////////////////////////////////////////////////////
// runQuery
///////////////////////////////////////////////////////////////////////////

func runQuery(
	env *mdbx.Env,
	dbi mdbx.DBI,
	statsBefore mdbx.Stat,
	envInfoBefore *mdbx.EnvInfo,
	txHashHex string,
	quiet bool,
) {
	// Parse key
	keyBytes, err := hexStringToBytes(txHashHex)
	if err != nil {
		fmt.Printf("Invalid hex: %s\n", txHashHex)
		return
	}

	// ----------------------------------
	// Query database
	// ----------------------------------
	var data []byte
	var found bool

	queryStart := time.Now()
	err = env.View(func(txn *mdbx.Txn) error {
		val, err := txn.Get(dbi, keyBytes)
		if err != nil {
			if mdbx.IsNotFound(err) {
				found = false
				return nil
			}
			return err
		}
		data = append([]byte{}, val...)
		found = true
		return nil
	})
	queryTime := time.Since(queryStart)

	if err != nil {
		fmt.Printf("DB error: %v\n", err)
		return
	}

	if !found {
		fmt.Printf("TX %s not found.\n", txHashHex)
		return
	}

	// ----------------------------------
	// Decompress
	// ----------------------------------
	decStart := time.Now()
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	decompressed, err := dec.DecodeAll(data, nil)
	if err != nil {
		fmt.Printf("Decompress failed: %v\n", err)
		return
	}
	decTime := time.Since(decStart)

	// ----------------------------------
	// Unmarshal
	// ----------------------------------
	unmarshalStart := time.Now()
	var txData tx_data.TxData
	if err := proto.Unmarshal(decompressed, &txData); err != nil {
		fmt.Printf("Unmarshal failed: %v\n", err)
		return
	}
	unmarshalTime := time.Since(unmarshalStart)

	// ----------------------------------
	// Compute per-query metrics
	// ----------------------------------
	pageSize := int(statsBefore.PSize)
	overflowPages := (len(data) / pageSize) + 1
	pagesRead := int(statsBefore.Depth) + overflowPages
	bytesRead := int64(pagesRead) * int64(pageSize)

	// ----------------------------------
	// QUIET MODE OUTPUT
	// ----------------------------------
	if quiet {
		fmt.Printf("TX: %s\n", txHashHex)
		fmt.Printf("Compressed:         %s\n", formatBytes(int64(len(data))))
		fmt.Printf("Uncompressed:       %s\n", formatBytes(int64(len(decompressed))))
		fmt.Printf("Compression Ratio:  %.2f%%\n\n", 100*(1-float64(len(data))/float64(len(decompressed))))

		fmt.Printf("Query:              %s\n", formatDuration(queryTime))
		fmt.Printf("Decompress:         %s\n", formatDuration(decTime))
		fmt.Printf("Unmarshal:          %s\n", formatDuration(unmarshalTime))
		fmt.Printf("Total:              %s\n\n", formatDuration(queryTime+decTime+unmarshalTime))

		fmt.Printf("TreeDepth:          %d\n", statsBefore.Depth)
		fmt.Printf("OverflowPages:      %d\n", overflowPages)
		fmt.Printf("PagesRead:          %d\n", pagesRead)
		fmt.Printf("BytesRead:          %s\n\n", formatBytes(bytesRead))
		return
	}

	// -----------------------------------------------------------------
	// FULL VERBOSE MODE (ORIGINAL OUTPUT)
	// -----------------------------------------------------------------

	txEnvelopeBase64 := base64.StdEncoding.EncodeToString(txData.TxEnvelope)
	txResultBase64 := base64.StdEncoding.EncodeToString(txData.TxResult)
	txMetaBase64 := base64.StdEncoding.EncodeToString(txData.TxMeta)

	totalTime := queryTime + decTime + unmarshalTime

	fmt.Printf("\n========================================\n")
	fmt.Printf("MDBX Transaction Lookup\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Transaction Hash: %s\n", txHashHex)
	fmt.Printf("========================================\n")

	fmt.Printf("\nTransaction Details:\n")
	fmt.Printf("  Ledger sequence:    %d\n", txData.LedgerSequence)
	fmt.Printf("  Transaction index:  %d\n", txData.Index)
	fmt.Printf("  Closed at:          %v\n", txData.ClosedAt.AsTime())

	fmt.Printf("\nData Sizes:\n")
	fmt.Printf("  Compressed:         %s\n", formatBytes(int64(len(data))))
	fmt.Printf("  Uncompressed:       %s\n", formatBytes(int64(len(decompressed))))
	fmt.Printf("  Compression ratio:  %.2f%%\n", 100*(1-float64(len(data))/float64(len(decompressed))))

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

	fmt.Printf("\n========================================\n")
	fmt.Printf("Query Performance Metrics\n")
	fmt.Printf("========================================\n")
	fmt.Printf("  Query time:         %s\n", formatDuration(queryTime))
	fmt.Printf("  Decompress time:    %s\n", formatDuration(decTime))
	fmt.Printf("  Unmarshal time:     %s\n", formatDuration(unmarshalTime))
	fmt.Printf("  Total time:         %s\n", formatDuration(totalTime))
	fmt.Printf("\nI/O Metrics:\n")
	fmt.Printf("  Tree depth:         %d\n", statsBefore.Depth)
	fmt.Printf("  Overflow pages:     %d\n", overflowPages)
	fmt.Printf("  Pages read:         %d\n", pagesRead)
	fmt.Printf("  Bytes read:         %s\n", formatBytes(bytesRead))

	fmt.Printf("\n========================================\n")
	fmt.Printf("Transaction Data (Base64 Encoded)\n")
	fmt.Printf("========================================\n")
	fmt.Printf("\nTxEnvelope (base64):\n%s\n", wrapText(txEnvelopeBase64, 80))
	fmt.Printf("\nTxResult (base64):\n%s\n", wrapText(txResultBase64, 80))
	fmt.Printf("\nTxMeta (base64):\n%s\n", wrapText(txMetaBase64, 80))
	fmt.Printf("========================================\n\n")
}

///////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////

func hexStringToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
}

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

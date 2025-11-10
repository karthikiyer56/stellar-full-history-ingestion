package main

// Filename: tx_lookup.go
// Usage: go run tx_lookup.go <db-path> <db3-path> <tx-hash-hex>
// Example: go run tx_lookup.go /data/db /data/db3 abc123...

import (
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/karthikiyer56/stellar-full-history-ingestion/tx_data"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"google.golang.org/protobuf/proto"
)

func main() {
	flag.Parse()

	if flag.NArg() < 3 {
		fmt.Println("Usage: tx_lookup <path to txHash->txData db> <tx-hash-hex>")
		fmt.Println("Example: tx_lookup /data/db  abc123...")
		os.Exit(1)
	}

	dbPath := flag.Arg(0)
	txHashHex := flag.Arg(1)

	// Convert hex string to binary bytes
	txHashBytes, err := hexStringToBytes(txHashHex)
	if err != nil {
		log.Fatalf("Failed to convert tx hash to bytes: %v", err)
	}

	// Check if databases exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Error: DB directory does not exist: %s", dbPath)
	}

	// Open DB2 (txHash -> TxData)
	opts2 := grocksdb.NewDefaultOptions()
	defer opts2.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts2, dbPath, false)
	if err != nil {
		log.Fatalf("Failed to open DB2: %v", err)
	}
	defer db.Close()

	// Open DB3 (txHash -> ledgerSeq)
	opts3 := grocksdb.NewDefaultOptions()
	defer opts3.Destroy()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Query DB2 for transaction data
	value, err := db.Get(ro, txHashBytes)
	if err != nil {
		log.Fatalf("Failed to read from DB: %v", err)
	}
	defer value.Free()

	data := value.Data()
	if len(data) == 0 {
		fmt.Printf("\n========================================\n")
		fmt.Printf("Transaction data not found in DB\n")
		fmt.Printf("========================================\n\n")
		return
	}

	// Decompress the data
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatalf("Failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()

	decompressedData, err := decoder.DecodeAll(data, nil)
	if err != nil {
		log.Fatalf("Failed to decompress data: %v", err)
	}

	// Unmarshal TxData proto
	var txData tx_data.TxData
	if err := proto.Unmarshal(decompressedData, &txData); err != nil {
		log.Fatalf("Failed to unmarshal TxData: %v", err)
	}

	// Convert transaction components to base64
	txEnvelopeBase64 := base64.StdEncoding.EncodeToString(txData.TxEnvelope)
	txResultBase64 := base64.StdEncoding.EncodeToString(txData.TxResult)
	txMetaBase64 := base64.StdEncoding.EncodeToString(txData.TxMeta)

	// Display results
	fmt.Printf("\n========================================\n")
	fmt.Printf("Transaction Hash: %s\n", txHashHex)
	fmt.Printf("========================================\n")
	fmt.Printf("Ledger sequence:      %d\n", txData.LedgerSequence)
	fmt.Printf("Transaction index:    %d\n", txData.Index)
	fmt.Printf("Closed at:            %v\n", txData.ClosedAt.AsTime())
	fmt.Printf("\nData sizes:\n")
	fmt.Printf("  Compressed:         %s\n", formatBytes(int64(len(data))))
	fmt.Printf("  Uncompressed:       %s\n", formatBytes(int64(len(decompressedData))))
	fmt.Printf("  Compression ratio:  %.2f%%\n", 100*(1-float64(len(data))/float64(len(decompressedData))))
	fmt.Printf("\nComponent sizes:\n")
	fmt.Printf("  Envelope:           %s\n", formatBytes(int64(len(txData.TxEnvelope))))
	fmt.Printf("  Result:             %s\n", formatBytes(int64(len(txData.TxResult))))
	fmt.Printf("  Meta:               %s\n", formatBytes(int64(len(txData.TxMeta))))
	fmt.Printf("\n========================================\n")
	fmt.Printf("Transaction Data (Base64 Encoded)\n")
	fmt.Printf("========================================\n")
	fmt.Printf("\nTxEnvelope (base64):\n%s\n", wrapText(txEnvelopeBase64, 80))
	fmt.Printf("\nTxResult (base64):\n%s\n", wrapText(txResultBase64, 80))
	fmt.Printf("\nTxMeta (base64):\n%s\n", wrapText(txMetaBase64, 80))
	fmt.Printf("========================================\n\n")
}

// hexStringToBytes converts a hex string to bytes
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

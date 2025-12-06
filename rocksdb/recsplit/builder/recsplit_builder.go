package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/recsplit"
	newLog "github.com/ledgerwatch/log/v3"
)

func main() {
	keyCount := 1_000_000
	tmpDir := "/tmp/recsplit-test"
	os.MkdirAll(tmpDir, 0755)

	logger := newLog.New()

	// Generate random keys once
	keys := make([][32]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		rand.Read(keys[i][:])
	}

	// =======================================================================
	// Test 1: Enums=true (should be minimal - just the hash function + enum)
	// =======================================================================
	fmt.Println("=== TEST 1: Enums=true, sequential offsets ===")
	idxPath1 := filepath.Join(tmpDir, "test1.idx")
	os.Remove(idxPath1)

	rs1, _ := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keyCount,
		Enums:      true,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxPath1,
		BaseDataID: 0,
	}, logger)

	for i := 0; i < keyCount; i++ {
		rs1.AddKey(keys[i][:], uint64(i)) // Sequential: 0, 1, 2, ...
	}
	rs1.Build(context.Background())

	info1, _ := os.Stat(idxPath1)
	fmt.Printf("Index size: %d bytes (%.2f bits/key)\n\n", info1.Size(), float64(info1.Size()*8)/float64(keyCount))

	// =======================================================================
	// Test 2: Enums=false, sequential offsets
	// =======================================================================
	fmt.Println("=== TEST 2: Enums=false, sequential offsets ===")
	idxPath2 := filepath.Join(tmpDir, "test2.idx")
	os.Remove(idxPath2)

	rs2, _ := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxPath2,
		BaseDataID: 0,
	}, logger)

	for i := 0; i < keyCount; i++ {
		rs2.AddKey(keys[i][:], uint64(i)) // Sequential: 0, 1, 2, ...
	}
	rs2.Build(context.Background())

	info2, _ := os.Stat(idxPath2)
	fmt.Printf("Index size: %d bytes (%.2f bits/key)\n\n", info2.Size(), float64(info2.Size()*8)/float64(keyCount))

	// =======================================================================
	// Test 3: Enums=false, random offsets (like your ledgerSeq)
	// =======================================================================
	fmt.Println("=== TEST 3: Enums=false, random offsets (your case) ===")
	idxPath3 := filepath.Join(tmpDir, "test3.idx")
	os.Remove(idxPath3)

	rs3, _ := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxPath3,
		BaseDataID: 0,
	}, logger)

	for i := 0; i < keyCount; i++ {
		// Random offset in range 0 - 50,000,000 (like ledgerSeq)
		randomOffset := uint64(keys[i][0])<<24 | uint64(keys[i][1])<<16 |
			uint64(keys[i][2])<<8 | uint64(keys[i][3])
		randomOffset = randomOffset % 50_000_000
		rs3.AddKey(keys[i][:], randomOffset)
	}
	rs3.Build(context.Background())

	info3, _ := os.Stat(idxPath3)
	fmt.Printf("Index size: %d bytes (%.2f bits/key)\n\n", info3.Size(), float64(info3.Size()*8)/float64(keyCount))

	// =======================================================================
	// Test 4: Enums=false, small range offsets
	// =======================================================================
	fmt.Println("=== TEST 4: Enums=false, small range offsets (0-1000) ===")
	idxPath4 := filepath.Join(tmpDir, "test4.idx")
	os.Remove(idxPath4)

	rs4, _ := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxPath4,
		BaseDataID: 0,
	}, logger)

	for i := 0; i < keyCount; i++ {
		smallOffset := uint64(keys[i][0])<<8 | uint64(keys[i][1])
		smallOffset = smallOffset % 1000
		rs4.AddKey(keys[i][:], smallOffset)
	}
	rs4.Build(context.Background())

	info4, _ := os.Stat(idxPath4)
	fmt.Printf("Index size: %d bytes (%.2f bits/key)\n\n", info4.Size(), float64(info4.Size()*8)/float64(keyCount))

	// =======================================================================
	// Summary
	// =======================================================================
	fmt.Println("=== SUMMARY ===")
	fmt.Printf("Test 1 (Enums=true, sequential):     %8d bytes (%5.2f bits/key)\n", info1.Size(), float64(info1.Size()*8)/float64(keyCount))
	fmt.Printf("Test 2 (Enums=false, sequential):    %8d bytes (%5.2f bits/key)\n", info2.Size(), float64(info2.Size()*8)/float64(keyCount))
	fmt.Printf("Test 3 (Enums=false, random 0-50M):  %8d bytes (%5.2f bits/key)\n", info3.Size(), float64(info3.Size()*8)/float64(keyCount))
	fmt.Printf("Test 4 (Enums=false, random 0-1K):   %8d bytes (%5.2f bits/key)\n", info4.Size(), float64(info4.Size()*8)/float64(keyCount))

	fmt.Println("\n=== ANALYSIS ===")
	hashFunctionSize := float64(info1.Size()*8) / float64(keyCount)
	valueSizeTest3 := float64(info3.Size()*8)/float64(keyCount) - hashFunctionSize
	fmt.Printf("Estimated hash function:  %.2f bits/key\n", hashFunctionSize)
	fmt.Printf("Estimated value storage:  %.2f bits/key (for random 0-50M values)\n", valueSizeTest3)
}

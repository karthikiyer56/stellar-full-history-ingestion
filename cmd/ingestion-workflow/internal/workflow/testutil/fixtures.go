package testutil

import (
	"encoding/binary"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// CreateTestLCM creates a minimal LedgerCloseMeta for testing.
// Includes ledger header + txHashes for testing dual-store writes.
//
// The tx hash extraction logic uses lcm.V1.TxProcessing[i].Result.TransactionHash.
// This fixture populates that field.
func CreateTestLCM(seq uint32, txHashes ...xdr.Hash) xdr.LedgerCloseMeta {
	// Build TxProcessing entries for each tx hash
	txProcessing := make([]xdr.TransactionResultMeta, len(txHashes))
	for i, hash := range txHashes {
		txProcessing[i] = xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
			},
		}
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxProcessing: txProcessing,
		},
	}
}

// CreateTestLedgers creates a range of test ledgers for integration tests.
// Each ledger has one deterministic txHash based on the ledger sequence.
func CreateTestLedgers(start, end uint32) map[uint32]xdr.LedgerCloseMeta {
	ledgers := make(map[uint32]xdr.LedgerCloseMeta)
	for seq := start; seq <= end; seq++ {
		// Generate deterministic txHash based on seq
		var txHash xdr.Hash
		binary.BigEndian.PutUint32(txHash[:], seq)
		ledgers[seq] = CreateTestLCM(seq, txHash)
	}
	return ledgers
}

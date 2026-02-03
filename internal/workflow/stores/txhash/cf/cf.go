package cf

// Names contains the names of all 16 column families.
// Each column family is named by the hex character it handles (0-9, a-f).
//
// PARTITIONING SCHEME:
//
//	txHash[0] >> 4 gives the CF index (0-15)
//	This distributes data roughly evenly (assuming uniform hash distribution)
var Names = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// Count is the number of column families.
const Count = 16

// GetIndex returns the column family index (0-15) for a transaction hash.
// Uses high nibble of first byte: txHash[0] >> 4
func GetIndex(txHash []byte) int {
	if len(txHash) < 1 {
		return 0
	}
	return int(txHash[0] >> 4)
}

// GetName returns the column family name for a transaction hash.
func GetName(txHash []byte) string {
	idx := GetIndex(txHash)
	if idx < 0 || idx >= Count {
		return "0"
	}
	return Names[idx]
}

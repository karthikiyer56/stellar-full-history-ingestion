package testutil

import (
	"encoding/base64"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// ValidLCMBase64 is a base64-encoded LedgerCloseMeta from pubnet (ledger with no transactions).
// This is a real LCM that can be used for testing serialization/deserialization.
const ValidLCMBase64 = "AAAAAHYUzG9I8LBHnYl32xf9WurpKshIzjNmGwhKGOpxEhi7AAAAAot+k+gX2q6nlPvMVmm5+Lm+ism/E3hZlwmLcROl3XCtdDKhhsUzrfydCS0mYJ9jQ1BQUuNx+9UY0gfc+ahZQd4AAAAAV8YYwgAAAAAAAAAA3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERnPDdx5bPNjHjlREjUDyJHfkk1gOz2j3A87v89v8dlgYgBd6X8OLWUr5gW+7QAABBRMu4iVAAAAcAAAAAAAAACjAAAAZAX14QAAAAAyFB3howWpd0J9sVRtzBRez88hpFEl9+S+1agMHAaxJP45LflTsZcC99pV8LXBtiEcgbJ5jjjiwK5tEDX12gLR4oJvoAO2yqJGgpwOCTnYWhe8saXU4CIicSXhzgB7y6MuumtkhjbBFEbzD6cFRqgpfgNkGGYalBw5B8RyJ1E6R4AAAAAAAAAAAIt+k+gX2q6nlPvMVmm5+Lm+ism/E3hZlwmLcROl3XCtAAAAAAAAAAAAAAAAAAAAAA=="

// CreateTestLCM creates a valid LedgerCloseMeta for testing by decoding a real pubnet LCM
// and modifying the ledger sequence. This LCM has no transactions.
func CreateTestLCM(seq uint32) xdr.LedgerCloseMeta {
	data, err := base64.StdEncoding.DecodeString(ValidLCMBase64)
	if err != nil {
		panic("failed to decode ValidLCMBase64: " + err.Error())
	}

	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(data); err != nil {
		panic("failed to unmarshal LedgerCloseMeta: " + err.Error())
	}

	if lcm.V == 0 && lcm.V0 != nil {
		lcm.V0.LedgerHeader.Header.LedgerSeq = xdr.Uint32(seq)
	} else if lcm.V == 1 && lcm.V1 != nil {
		lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(seq)
	}

	return lcm
}

// CreateTestLedgers creates a range of test ledgers for integration tests.
// Each ledger has no transactions (uses real pubnet LCM structure).
func CreateTestLedgers(start, end uint32) map[uint32]xdr.LedgerCloseMeta {
	ledgers := make(map[uint32]xdr.LedgerCloseMeta)
	for seq := start; seq <= end; seq++ {
		ledgers[seq] = CreateTestLCM(seq)
	}
	return ledgers
}

package main

import (
	"bytes"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/tx_data"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
)

func update_maps(lcm xdr.LedgerCloseMeta,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32) error {

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return errors.Wrap(err, "failed to create reader")
	}
	defer txReader.Close()

	ledgerSeq := lcm.LedgerSequence()
	closedAt := lcm.ClosedAt()

	lcmBytes, err := lcm.MarshalBinary()
	if err != nil {
		return errors.Wrapf(err, "failed to marshal lcm for ledger: %d", ledgerSeq)
	}

	var compressedLcm bytes.Buffer
	// Create an encoder. You can tune level/options as needed.
	enc, err := zstd.NewWriter(&compressedLcm)
	if err != nil {
		return errors.Wrap(err, "zstd.NewWriter")
	}

	if _, err := enc.Write(lcmBytes); err != nil {
		return errors.Wrap(err, "zstd.Write")
	}
	if err := enc.Close(); err != nil {
		return errors.Wrap(err, "zstd.Close")
	}
	ledgerSeqToLcm[ledgerSeq] = compressedLcm.Bytes()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		txEnvelopeBytes, err := tx.Envelope.MarshalBinary()
		if err != nil {
			return fmt.Errorf("error marshalling transaction envelope: %w", err)
		}
		txResultBytes, err := tx.Result.MarshalBinary()
		if err != nil {
			return fmt.Errorf("error marshalling transaction result: %w", err)
		}
		txMetaBytes, err := tx.UnsafeMeta.MarshalBinary()
		if err != nil {
			return fmt.Errorf("error marshalling transaction meta: %w", err)
		}

		txDataProto := tx_data.TxData{
			LedgerSequence: ledgerSeq,
			ClosedAt:       timestamppb.New(closedAt),
			Index:          tx.Index,
			TxEnvelope:     txEnvelopeBytes,
			TxResult:       txResultBytes,
			TxMeta:         txMetaBytes,
		}
		txDataBytes, err := proto.Marshal(&txDataProto)
		if err != nil {
			return errors.Wrap(err, "marshalling proto transaction data")
		}

		var compressedTxData bytes.Buffer
		enc, err = zstd.NewWriter(&compressedTxData)
		if err != nil {
			return errors.Wrap(err, "zstd.NewWriter")
		}

		if _, err := enc.Write(txDataBytes); err != nil {
			return errors.Wrap(err, "zstd.Write")
		}
		if err := enc.Close(); err != nil {
			return errors.Wrap(err, "zstd.Close")
		}

		txHashToTxData[tx.Hash.HexString()] = compressedTxData.Bytes()
		txHashToLedgerSeq[tx.Hash.HexString()] = ledgerSeq
	}

	return nil
}

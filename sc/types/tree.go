package types

import (
	"io"

	dbm "github.com/cometbft/cometbft-db"
	ics23 "github.com/cosmos/ics23/go"
)

type Tree interface {
	Get(key []byte) []byte

	Has(key []byte) bool

	Set(key, value []byte)

	Remove(key []byte)

	Version() int64

	RootHash() []byte

	Iterator(start, end []byte, ascending bool) dbm.Iterator

	GetProof(key []byte) *ics23.CommitmentProof

	io.Closer
}

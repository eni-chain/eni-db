package utils

import (
	"cosmossdk.io/core/store"
	cdb "github.com/cometbft/cometbft-db"
	dbm "github.com/cosmos/iavl/db"
)

type WrapDB struct {
	cdb.DB
}

func NewWrapDB(db cdb.DB) dbm.DB {
	return &WrapDB{DB: db}
}
func (w WrapDB) Iterator(start, end []byte) (store.Iterator, error) {
	iter, err := w.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &WrapIterator{Iterator: iter}, nil
}

func (w WrapDB) ReverseIterator(start, end []byte) (store.Iterator, error) {
	iter, err := w.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &WrapIterator{Iterator: iter}, nil
}

func (w WrapDB) NewBatch() store.Batch {
	batch := w.DB.NewBatch()
	return &WrapBatch{Batch: batch}
}

func (w WrapDB) NewBatchWithSize(i int) store.Batch {
	batch := w.DB.NewBatch()
	return &WrapBatch{Batch: batch}
}

var _ dbm.DB = (*WrapDB)(nil)

type WrapIterator struct {
	cdb.Iterator
}

var _ store.Iterator = (*WrapIterator)(nil)

type WrapBatch struct {
	cdb.Batch
}

func (w WrapBatch) GetByteSize() (int, error) {
	return 0, nil
}

var _ store.Batch = (*WrapBatch)(nil)

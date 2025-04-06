package leveldb

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Batch struct {
	storage *leveldb.DB
	batch   *leveldb.Batch
	version int64
}

func NewBatch(storage *leveldb.DB, version int64) (*Batch, error) {
	var versionBz [VersionSize]byte
	binary.LittleEndian.PutUint64(versionBz[:], uint64(version))

	batch := new(leveldb.Batch)
	batch.Put([]byte(latestVersionKey), versionBz[:])

	return &Batch{
		storage: storage,
		batch:   batch,
		version: version,
	}, nil
}

func (b *Batch) Size() int {
	return b.batch.Len()
}

func (b *Batch) Reset() {
	b.batch.Reset()
}

func (b *Batch) set(storeKey string, key, value []byte) error {
	prefixedKey := append([]byte(storeKey), key...)
	b.batch.Put(prefixedKey, value)
	return nil
}

func (b *Batch) Set(storeKey string, key, value []byte) error {
	return b.set(storeKey, key, value)
}

func (b *Batch) Delete(storeKey string, key []byte) error {
	prefixedKey := append([]byte(storeKey), key...)
	b.batch.Delete(prefixedKey)
	return nil
}

func (b *Batch) Write() error {
	return b.storage.Write(b.batch, &opt.WriteOptions{Sync: true})
}

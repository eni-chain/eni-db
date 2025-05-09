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

func newBatch(storage *leveldb.DB, version int64) (*Batch, error) {
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

type HisBatch struct {
	history *leveldb.DB
	batch   *leveldb.Batch
	version int64
}

func newHisBatch(history *leveldb.DB, version int64) *HisBatch {
	return &HisBatch{
		history: history,
		batch:   new(leveldb.Batch),
		version: version,
	}
}

func (hb *HisBatch) Size() int {
	return hb.batch.Len()
}

func (hb *HisBatch) Reset() {
	hb.batch.Reset()
}

func (hb *HisBatch) constructKey(storeKey string, key []byte) []byte {
	var versionBz [8]byte
	binary.LittleEndian.PutUint64(versionBz[:], uint64(hb.version))

	// Key: storeKey + key + version
	return append(append([]byte(storeKey), key...), versionBz[:]...)
}

func (hb *HisBatch) set(storeKey string, key, value []byte) error {
	prefixedKey := hb.constructKey(storeKey, key)
	hb.batch.Put(prefixedKey, value)
	return nil
}

func (hb *HisBatch) Set(storeKey string, key, value []byte) error {
	return hb.set(storeKey, key, value)
}

func (hb *HisBatch) Delete(storeKey string, key []byte) error {
	return hb.set(storeKey, key, nil)
}

func (hb *HisBatch) Write() error {
	return hb.history.Write(hb.batch, &opt.WriteOptions{Sync: true})
}

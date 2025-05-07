package leveldb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/eni-chain/eni-db/config"
	"github.com/eni-chain/eni-db/proto"
	"github.com/eni-chain/eni-db/ss/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	VersionSize      = 8
	latestVersionKey = "latestVersion"
)

var (
	errBatchClosed = errors.New("batch has been written or closed")
	errKeyEmpty    = errors.New("key cannot be empty")
	errValueNil    = errors.New("value cannot be nil")
)

type Database struct {
	storage *leveldb.DB
	history *leveldb.DB
	config  config.StateStoreConfig
	mu      sync.RWMutex
}

func New(dataDir string, config config.StateStoreConfig) (*Database, error) {
	db, err := leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open LevelDB: %w", err)
	}
	history, err := leveldb.OpenFile(dataDir+"/history", nil)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open LevelDB history: %w", err)
	}
	d := &Database{
		storage: db,
		history: history,
		config:  config,
	}
	return d, nil
}

func (db *Database) Close() error {
	if db.history != nil {
		if err := db.history.Close(); err != nil {
			return fmt.Errorf("failed to close LevelDB history: %w", err)
		}
		db.history = nil
	}
	return db.storage.Close()
}
func (db *Database) getDb(version int64) *leveldb.DB {
	if version <= 0 {
		return db.storage
	}
	return db.history
}
func (db *Database) Get(storeKey string, version int64, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	var res []byte
	var err error
	if version <= 0 {
		res, err = db.storage.Get(append([]byte(storeKey), key...), nil)
	} else {
		res, err = getFromVersionDb(db.history, append([]byte(storeKey), key...), version)
	}
	if err != nil && errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	return res, err
}
func getFromVersionDb(db *leveldb.DB, key []byte, version int64) ([]byte, error) {
	iter := db.NewIterator(util.BytesPrefix(key), nil)
	defer iter.Release()
	// get keyVersion<=version
	var value []byte
	for iter.Next() {
		k := iter.Key()
		// get version
		ver := binary.LittleEndian.Uint64(k[len(k)-8:])
		if ver <= uint64(version) {
			//copy iter.Value()
			value = make([]byte, len(iter.Value()))
			copy(value, iter.Value())
		} else {
			break
		}
	}
	return value, iter.Error()
}

func (db *Database) Has(storeKey string, version int64, key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	if version <= 0 {
		return db.storage.Has(append([]byte(storeKey), key...), nil)
	}
	return hasFromVersionDb(db.history, append([]byte(storeKey), key...), version)
}
func hasFromVersionDb(db *leveldb.DB, key []byte, version int64) (bool, error) {
	iter := db.NewIterator(util.BytesPrefix(key), nil)
	defer iter.Release()
	// get keyVersion<=version
	has := false
	for iter.Next() {
		k := iter.Key()
		// get version
		ver := binary.LittleEndian.Uint64(k[len(k)-8:])
		if ver <= uint64(version) {
			if iter.Value() != nil {
				has = true
			} else {
				has = false
			}
		} else {
			break
		}
	}
	return has, iter.Error()
}

func (db *Database) Iterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	if version <= 0 {
		return newIterator(db.storage, storeKey, version, start, end, false), nil
	}
	return newHisIterator(db.history, storeKey, version, start, end, false), nil
}

func (db *Database) ReverseIterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
	if version <= 0 {
		return newIterator(db.storage, storeKey, version, start, end, true), nil
	}
	return newHisIterator(db.history, storeKey, version, start, end, true), nil
}

func (db *Database) RawIterate(storeKey string, fn func([]byte, []byte, int64) bool) (bool, error) {
	iter := db.storage.NewIterator(util.BytesPrefix([]byte(storeKey)), nil)
	defer iter.Release()
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if !fn(key, value, 0) {
			return false, nil
		}
	}
	return true, iter.Error()
}

func (db *Database) GetLatestVersion() (int64, error) {
	bz, err := db.storage.Get([]byte(latestVersionKey), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(bz) == 0 {
		return 0, nil
	}
	return int64(binary.LittleEndian.Uint64(bz)), nil
}

func (db *Database) SetLatestVersion(version int64) error {
	var ts [VersionSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))
	return db.storage.Put([]byte(latestVersionKey), ts[:], nil)
}

func (db *Database) GetEarliestVersion() (int64, error) {
	bz, err := db.storage.Get([]byte("earliestVersion"), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(bz) == 0 {
		return 0, nil
	}
	return int64(binary.LittleEndian.Uint64(bz)), nil
}

func (db *Database) SetEarliestVersion(version int64, ignoreVersion bool) error {
	var ts [VersionSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))
	return db.storage.Put([]byte("earliestVersion"), ts[:], nil)
}

func (db *Database) GetLatestMigratedKey() ([]byte, error) {
	return db.storage.Get([]byte("latestMigratedKey"), nil)
}

func (db *Database) SetLatestMigratedKey(key []byte) error {
	return db.storage.Put([]byte("latestMigratedKey"), key, nil)
}

func (db *Database) GetLatestMigratedModule() (string, error) {
	bz, err := db.storage.Get([]byte("latestMigratedModule"), nil)
	if err != nil {
		return "", err
	}
	return string(bz), nil
}

func (db *Database) SetLatestMigratedModule(module string) error {
	return db.storage.Put([]byte("latestMigratedModule"), []byte(module), nil)
}

func (db *Database) ApplyChangeset(version int64, cs *proto.NamedChangeSet) error {
	batch, err := NewBatch(db.storage, version)
	if err != nil {
		return err
	}
	defer batch.Reset()
	historyBatch := newHisBatch(db.history, version)
	defer historyBatch.Reset()
	for _, change := range cs.Changeset.Pairs {
		if change.Delete {
			err = batch.Delete(cs.Name, change.Key)
			if err != nil {
				return err
			}
			err = historyBatch.Delete(cs.Name, change.Key)
			if err != nil {
				return err
			}
		} else {
			err = batch.Set(cs.Name, change.Key, change.Value)
			if err != nil {
				return err
			}
			err = historyBatch.Set(cs.Name, change.Key, change.Value)
			if err != nil {
				return err
			}
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := batch.Write(); err != nil {
			panic(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := historyBatch.Write(); err != nil {
			panic(err)
		}
	}()
	wg.Wait()
	return nil
}

func (db *Database) ApplyChangesetAsync(version int64, changesets []*proto.NamedChangeSet) error {
	for _, change := range changesets {
		err := db.ApplyChangeset(version, change)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) Import(version int64, ch <-chan types.SnapshotNode) error {
	panic("Import not implement")
}

func (db *Database) RawImport(ch <-chan types.RawSnapshotNode) error {
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		batch := new(leveldb.Batch)
		for entry := range ch {
			key := append([]byte(entry.StoreKey), entry.Key...)
			batch.Put(key, entry.Value)
		}
		if err := db.storage.Write(batch, nil); err != nil {
			panic(err)
		}
	}

	wg.Add(db.config.ImportNumWorkers)
	for i := 0; i < db.config.ImportNumWorkers; i++ {
		go worker()
	}

	wg.Wait()
	return nil
}

func (db *Database) Prune(version int64) error {
	// Implement prune logic here
	return nil
}

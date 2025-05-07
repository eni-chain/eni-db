package leveldb

import (
	"bytes"
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type HisIterator struct {
	source  iterator.Iterator
	prefix  []byte
	start   []byte
	end     []byte
	version int64
	valid   bool
	reverse bool
}

func newHisIterator(history *leveldb.DB, storeKey string, version int64, start, end []byte, reverse bool) *HisIterator {
	var versionBz [8]byte
	binary.LittleEndian.PutUint64(versionBz[:], uint64(version))

	prefixBytes := []byte(storeKey)
	startIter := append([]byte(nil), prefixBytes...)
	if start != nil {
		startIter = append(startIter, start...)
	}

	var endIter []byte
	if end != nil {
		endIter = append(append([]byte(nil), prefixBytes...), end...)
	}

	source := history.NewIterator(&util.Range{Start: startIter, Limit: endIter}, nil)

	// initialize the iterator
	if reverse {
		if end == nil {
			source.Last()
		} else {
			source.Seek(endIter)
			if source.Valid() {
				source.Prev()
			} else {
				source.Last()
			}
		}
	} else {
		source.First()
	}

	itr := &HisIterator{
		source:  source,
		prefix:  prefixBytes,
		start:   start,
		end:     end,
		version: version,
		valid:   source.Valid(),
		reverse: reverse,
	}
	itr.updateValidity()
	return itr
}

func (itr *HisIterator) Domain() (start []byte, end []byte) {
	return itr.start, itr.end
}

func (itr *HisIterator) Valid() bool {
	return itr.valid
}

func (itr *HisIterator) Key() (key []byte) {
	fullKey := itr.source.Key()
	// remove prefix
	keyWithVersion := fullKey[len(itr.prefix):]
	// remove version
	return keyWithVersion[:len(keyWithVersion)-8]
}

func (itr *HisIterator) Value() (value []byte) {
	return itr.source.Value()
}

func (itr *HisIterator) Error() error {
	return itr.source.Error()
}

func (itr *HisIterator) Close() error {
	itr.source.Release()
	itr.valid = false
	return nil
}

func (itr *HisIterator) Next() {
	if itr.reverse {
		itr.nextReverse()
	} else {
		itr.nextForward()
	}
}

func (itr *HisIterator) nextForward() {
	itr.source.Next()
	itr.updateValidity()
}

func (itr *HisIterator) nextReverse() {
	itr.source.Prev()
	itr.updateValidity()
}

func (itr *HisIterator) updateValidity() {
	itr.valid = false

	// Validate the current key and check if it matches the prefix and version
	for itr.source.Valid() {
		if itr.isKeyValid() {
			itr.valid = true
			break
		}
		itr.skipInvalidKey()
	}

	// Ensure only the largest version <= specified version is returned for forward iteration
	if itr.valid && !itr.reverse {
		itr.ensureLargestVersion()
	}
}

// isKeyValid checks if the current key matches the prefix and is <= the specified version.
func (itr *HisIterator) isKeyValid() bool {
	key := itr.source.Key()
	if !bytes.HasPrefix(key, itr.prefix) {
		return false
	}

	keyVersion := key[len(key)-8:]
	version := int64(binary.LittleEndian.Uint64(keyVersion))
	return version <= itr.version
}

// skipInvalidKey moves the iterator to the next or previous key based on the iteration direction.
func (itr *HisIterator) skipInvalidKey() {
	if itr.reverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

// ensureLargestVersion ensures that only the largest version <= specified version is returned.
func (itr *HisIterator) ensureLargestVersion() {
	currentKey := itr.source.Key()
	// Remove the version suffix (last 8 bytes) from the key
	baseKey := currentKey[:len(currentKey)-8]
	baseKey = append([]byte(nil), baseKey...) // Create a copy of the base key

	for itr.source.Next() {
		nextKey := itr.source.Key()

		// If the base key changes, move back to the previous key
		if !bytes.Equal(baseKey, nextKey[:len(nextKey)-8]) {
			itr.source.Prev()
			break
		}

		if !bytes.HasPrefix(nextKey, itr.prefix) {
			break
		}

		nextVersion := int64(binary.LittleEndian.Uint64(nextKey[len(nextKey)-8:]))
		if nextVersion > itr.version {
			itr.source.Prev()
			break
		}
	}

	// Handle edge case where the iterator goes out of bounds
	if itr.source.Key() == nil {
		itr.source.Prev()
	}
}

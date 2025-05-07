package leveldb

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Iterator struct {
	source  iterator.Iterator
	prefix  []byte
	start   []byte
	end     []byte
	version int64
	valid   bool
	reverse bool
}

func newIterator(db *leveldb.DB, storeKey string, version int64, start, end []byte, reverse bool) *Iterator {
	prefixBytes := []byte(storeKey)
	startIter := append([]byte(nil), prefixBytes...)
	if start != nil {
		startIter = append(startIter, start...)
	}

	var endIter []byte
	if end != nil {
		endIter = append(append([]byte(nil), prefixBytes...), end...)
	}

	source := db.NewIterator(&util.Range{Start: startIter, Limit: endIter}, nil)

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

	valid := source.Valid()

	return &Iterator{
		source:  source,
		prefix:  prefixBytes,
		start:   start,
		end:     end,
		version: version,
		valid:   valid,
		reverse: reverse,
	}
}

func (itr *Iterator) Domain() (start []byte, end []byte) {
	return itr.start, itr.end
}

func (itr *Iterator) Valid() bool {
	if !itr.valid || !itr.source.Valid() {
		itr.valid = false
		return false
	}

	if err := itr.source.Error(); err != nil {
		itr.valid = false
		return false
	}

	// check if the key has the correct prefix
	if !bytes.HasPrefix(itr.source.Key(), itr.prefix) {
		itr.valid = false
		return false
	}

	// get the key
	key := itr.Key()

	// check if the key is within the start and end range
	if itr.start != nil && bytes.Compare(key, itr.start) < 0 {
		itr.valid = false
		return false
	}
	if itr.end != nil && bytes.Compare(key, itr.end) >= 0 {
		itr.valid = false
		return false
	}

	return true
}

func (itr *Iterator) Key() (key []byte) {
	fullKey := itr.source.Key()
	// remove the prefix
	return fullKey[len(itr.prefix):]
}
func (itr *Iterator) Value() (value []byte) {
	return itr.source.Value()
}

func (itr *Iterator) Error() error {
	return itr.source.Error()
}

func (itr *Iterator) Close() error {
	itr.source.Release()
	itr.valid = false
	return nil
}

func (itr *Iterator) Next() {
	if itr.reverse {
		itr.nextReverse()
	} else {
		itr.nextForward()
	}
}

func (itr *Iterator) nextForward() {
	itr.source.Next()
	itr.updateValidity()
}

func (itr *Iterator) nextReverse() {
	itr.source.Prev()
	itr.updateValidity()
}

func (itr *Iterator) updateValidity() {
	if !itr.source.Valid() {
		itr.valid = false
		return
	}

	key := itr.source.Key()
	if !bytes.HasPrefix(key, itr.prefix) {
		itr.valid = false
	}
}

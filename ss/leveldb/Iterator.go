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
	version int64
	valid   bool
	reverse bool
}

func NewIterator(db *leveldb.DB, storeKey string, version int64, start, end []byte, reverse bool) *Iterator {
	startIter := append([]byte(storeKey), start...)
	endIter := append([]byte(storeKey), end...)
	source := db.NewIterator(&util.Range{Start: startIter, Limit: endIter}, nil)
	return &Iterator{
		source:  source,
		prefix:  []byte(storeKey),
		version: version,
		valid:   true,
		reverse: reverse,
	}
}

func (itr *Iterator) Domain() (start []byte, end []byte) {
	return itr.prefix, nil
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

	if end := itr.prefix; end != nil {
		if bytes.Compare(end, itr.Key()) <= 0 {
			itr.valid = false
			return false
		}
	}

	return true
}

func (itr *Iterator) Key() (key []byte) {
	return itr.source.Key()
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

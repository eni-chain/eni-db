package leveldb

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type Iterator struct {
	source  iterator.Iterator
	prefix  []byte
	version int64
	valid   bool
	reverse bool
}

func NewIterator(source iterator.Iterator, prefix []byte, version int64, reverse bool) *Iterator {
	return &Iterator{
		source:  source,
		prefix:  prefix,
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

package leveldb_test

import (
	"testing"

	"github.com/cosmos/iavl"
	"github.com/eni-chain/eni-db/config"
	"github.com/eni-chain/eni-db/proto"
	"github.com/eni-chain/eni-db/ss/leveldb"
	"github.com/stretchr/testify/require"
)

func TestDatabase_BasicOperations(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Initialize the database
	db, err := leveldb.New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	// Test Set and Get
	storeKey := "testStore"
	key := []byte("key1")
	value := []byte("value1")
	err = db.ApplyChangeset(1, &proto.NamedChangeSet{
		Name: storeKey,
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{Key: key, Value: value},
			},
		},
	})
	require.NoError(t, err)

	// Verify Get
	gotValue, err := db.Get(storeKey, 1, key)
	require.NoError(t, err)
	require.Equal(t, value, gotValue)

	// Verify Has
	has, err := db.Has(storeKey, 1, key)
	require.NoError(t, err)
	require.True(t, has)
}

func TestIterator(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Initialize the database
	db, err := leveldb.New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	// Write data
	storeKey := "testStore"
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i)}
		err := db.ApplyChangeset(1, &proto.NamedChangeSet{
			Name: storeKey,
			Changeset: iavl.ChangeSet{
				Pairs: []*iavl.KVPair{
					{Key: key, Value: value},
				},
			},
		})
		require.NoError(t, err)
	}
	// new version
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i + 1)}
		err := db.ApplyChangeset(2, &proto.NamedChangeSet{
			Name: storeKey,
			Changeset: iavl.ChangeSet{
				Pairs: []*iavl.KVPair{
					{Key: key, Value: value},
				},
			},
		})
		require.NoError(t, err)
	}
	// Test Iterator
	iter, err := db.Iterator(storeKey, 1, nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	count := 0
	for iter.Valid() {
		require.Equal(t, iter.Key(), iter.Value())
		iter.Next()
		count++
	}
	require.Equal(t, 5, count)

	iter, err = db.Iterator(storeKey, 2, nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	count = 0
	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		//require key == value -1
		require.Equal(t, key[0], value[0]-1)
		iter.Next()
		count++
	}
	require.Equal(t, 5, count)
}

func TestDatabase_Get(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Initialize the database
	db, err := leveldb.New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	storeKey := "testStore"
	key := []byte("key1")
	value := []byte("value1")

	// test empty key
	_, err = db.Get(storeKey, 1, nil)
	require.Error(t, err)
	require.Equal(t, "key cannot be empty", err.Error())

	// test data not exist
	res, err := db.Get(storeKey, 1, key)
	require.NoError(t, err)
	require.Nil(t, res)

	// write data to version 1
	err = db.ApplyChangeset(1, &proto.NamedChangeSet{
		Name: storeKey,
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{Key: key, Value: value},
			},
		},
	})
	require.NoError(t, err)

	// test getting the key at version 1
	res, err = db.Get(storeKey, 1, key)
	require.NoError(t, err)
	require.Equal(t, value, res)

	// test getting the key at version 0 (latest version)
	res, err = db.Get(storeKey, 0, key)
	require.NoError(t, err)
	require.Equal(t, value, res)

	// write data to version 2
	newValue := []byte("value2")
	err = db.ApplyChangeset(2, &proto.NamedChangeSet{
		Name: storeKey,
		Changeset: iavl.ChangeSet{
			Pairs: []*iavl.KVPair{
				{Key: key, Value: newValue},
			},
		},
	})
	require.NoError(t, err)

	// test getting the key at version 2
	res, err = db.Get(storeKey, 2, key)
	require.NoError(t, err)
	require.Equal(t, newValue, res)

	// test getting the key at version 1 (should still be the old value)
	res, err = db.Get(storeKey, 1, key)
	require.NoError(t, err)
	require.Equal(t, value, res)

	// test getting the key at version 3 (should be the new value)
	res, err = db.Get(storeKey, 3, key)
	require.NoError(t, err)
	require.Equal(t, newValue, res)
}

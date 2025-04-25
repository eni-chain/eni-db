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
		value := []byte{byte(i + 10)}
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
}

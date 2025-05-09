package leveldb

import (
	"testing"

	"github.com/cosmos/iavl"
	"github.com/eni-chain/eni-db/config"
	"github.com/eni-chain/eni-db/proto"
	"github.com/stretchr/testify/require"
)

func TestIterator_BasicForward(t *testing.T) {
	tempDir := t.TempDir()

	// 初始化数据库
	db, err := New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	storeKey := "testStore"

	// 写入数据
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

	// 创建正向迭代器
	iter := newIterator(db.storage, storeKey, 0, nil, nil, false)
	defer iter.Close()

	count := 0
	for iter.Valid() {
		require.Equal(t, iter.Key(), iter.Value())
		iter.Next()
		count++
	}
	require.Equal(t, 5, count)
}

func TestIterator_BasicReverse(t *testing.T) {
	tempDir := t.TempDir()

	// 初始化数据库
	db, err := New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	storeKey := "testStore"

	// 写入数据
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

	// 创建反向迭代器
	iter := newIterator(db.storage, storeKey, 0, nil, nil, true)
	defer iter.Close()

	count := 0
	expected := 4
	for iter.Valid() {
		require.Equal(t, iter.Key()[0], byte(expected))
		require.Equal(t, iter.Value()[0], byte(expected))
		iter.Next()
		count++
		expected--
	}
	require.Equal(t, 5, count)
}

func TestIterator_Boundaries(t *testing.T) {
	tempDir := t.TempDir()

	// 初始化数据库
	db, err := New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	storeKey := "testStore"

	// 写入数据
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

	// 测试起始边界
	start := []byte{2}
	end := []byte{4}
	iter := newIterator(db.storage, storeKey, 0, start, end, false)
	defer iter.Close()

	count := 0
	for iter.Valid() {
		key := iter.Key()
		require.GreaterOrEqual(t, key[0], start[0])
		require.Less(t, key[0], end[0])
		iter.Next()
		count++
	}
	require.Equal(t, 2, count)
}

func TestIterator_InvalidKeys(t *testing.T) {
	tempDir := t.TempDir()

	// 初始化数据库
	db, err := New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	storeKey := "testStore"

	// 写入数据
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

	// 创建迭代器并验证无效键
	iter := newIterator(db.storage, "invalidStore", 0, nil, nil, false)
	defer iter.Close()

	require.False(t, iter.Valid())
}

func TestIterator_EmptyRange(t *testing.T) {
	tempDir := t.TempDir()

	// 初始化数据库
	db, err := New(tempDir, config.StateStoreConfig{})
	require.NoError(t, err)
	defer db.Close()

	storeKey := "testStore"

	// 写入数据
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

	// 测试空范围
	start := []byte{5}
	end := []byte{6}
	iter := newIterator(db.storage, storeKey, 0, start, end, false)
	defer iter.Close()

	require.False(t, iter.Valid())
}

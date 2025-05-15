package leveldb

import (
	"testing"

	"github.com/eni-chain/eni-db/config"
	"github.com/stretchr/testify/require"
)

func TestBatch(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()

	// 初始化LevelDB
	db, err := New(tempDir, config.StateStoreConfig{Enable: false})
	require.NoError(t, err)
	defer db.Close()

	// 测试NewBatch
	version := int64(1)
	batch, err := newBatch(db.storage, version)
	require.NoError(t, err)
	require.NotNil(t, batch)

	// 测试Set
	storeKey := "testStore"
	key := []byte("key1")
	value := []byte("value1")
	err = batch.Set(storeKey, key, value)
	require.NoError(t, err)

	// 测试Delete
	err = batch.Delete(storeKey, key)
	require.NoError(t, err)

	// 测试Write
	err = batch.Write()
	require.NoError(t, err)

	// 验证数据是否写入
	gotValue, err := db.Get(storeKey, 0, key)
	require.NoError(t, err)
	require.Nil(t, gotValue)

	// 测试Reset
	batch.Reset()
	require.Equal(t, 0, batch.Size())
}

func TestHisBatch(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()

	// 初始化LevelDB
	db, err := New(tempDir, config.StateStoreConfig{Enable: true})
	require.NoError(t, err)
	defer db.Close()

	// 测试newHisBatch
	version := int64(1)
	hisBatch := newHisBatch(db.history, version)
	require.NotNil(t, hisBatch)

	// 测试Set
	storeKey := "testStore"
	key := []byte("key1")
	value := []byte("value1")
	err = hisBatch.Set(storeKey, key, value)
	require.NoError(t, err)

	// 测试Delete
	err = hisBatch.Delete(storeKey, key)
	require.NoError(t, err)

	// 测试Write
	err = hisBatch.Write()
	require.NoError(t, err)

	// 验证数据是否写入
	gotValue, err := db.Get(storeKey, 0, key)

	require.NoError(t, err)
	require.Nil(t, gotValue)

	// 测试Reset
	hisBatch.Reset()
	require.Equal(t, 0, hisBatch.Size())
}

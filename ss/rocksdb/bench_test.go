//go:build rocksdbBackend
// +build rocksdbBackend

package rocksdb

import (
	"testing"

	"github.com/eni-chain/eni-db/config"
	sstest "github.com/eni-chain/eni-db/ss/test"
	"github.com/eni-chain/eni-db/ss/types"
)

func BenchmarkDBBackend(b *testing.B) {
	s := &sstest.StorageBenchSuite{
		NewDB: func(dir string) (types.StateStore, error) {
			return New(dir, config.DefaultStateStoreConfig())
		},
		BenchBackendName: "RocksDB",
	}

	s.BenchmarkGet(b)
	s.BenchmarkApplyChangeset(b)
	s.BenchmarkIterate(b)
}

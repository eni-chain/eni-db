package ss

import (
	"github.com/eni-chain/eni-db/common/utils"
	"github.com/eni-chain/eni-db/config"
	"github.com/eni-chain/eni-db/ss/leveldb"
	"github.com/eni-chain/eni-db/ss/types"
)

func init() {
	initializer := func(dir string, configs config.StateStoreConfig) (types.StateStore, error) {
		dbHome := utils.GetStateStorePath(dir, configs.Backend)
		if configs.DBDirectory != "" {
			dbHome = configs.DBDirectory
		}
		return leveldb.New(dbHome, configs)
	}
	RegisterBackend(LevelDBBackend, initializer)
}

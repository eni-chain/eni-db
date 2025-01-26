package ss

import (
	"github.com/eni-chain/eni-db/common/utils"
	"github.com/eni-chain/eni-db/config"
	"github.com/eni-chain/eni-db/ss/pebbledb"
	"github.com/eni-chain/eni-db/ss/types"
)

func init() {
	initializer := func(dir string, configs config.StateStoreConfig) (types.StateStore, error) {
		dbHome := utils.GetStateStorePath(dir, configs.Backend)
		if configs.DBDirectory != "" {
			dbHome = configs.DBDirectory
		}
		return pebbledb.New(dbHome, configs)
	}
	RegisterBackend(PebbleDBBackend, initializer)
}

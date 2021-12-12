package storage

import "github.com/yedf/dtm/common"

var config = &common.DtmConfig

func dbGet() *common.DB {
	return common.DbGet(config.DB)
}

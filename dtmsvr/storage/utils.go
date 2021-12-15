package storage

import (
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/yedf/dtm/common"
	"gorm.io/gorm"
)

var config = &common.DtmConfig

func dbGet() *common.DB {
	return common.DbGet(config.DB)
}

func checkAffected(db1 *gorm.DB) {
	if db1.RowsAffected == 0 {
		panic(fmt.Errorf("rows affected 0, please check for abnormal trans"))
	}
}

func redisGet() *redis.Client {
	return common.RedisGet()
}

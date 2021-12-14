package storage

import (
	"fmt"
	"sync"

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

var rdb *redis.Client
var once sync.Once

func redisGet() *redis.Client {
	once.Do(func() {
		rdb = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", config.DB["host"], config.DB["port"]),
			Password: config.DB["password"],
		})
	})
	return rdb
}

package storage

import (
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
)

var ErrNotFound = errors.New("storage: NotFound")
var ErrUniqueConflict = errors.New("storage: UniqueKeyConflict")

type Store interface {
	PopulateData(skipDrop bool)
	GetTransGlobal(gid string, trans *TransGlobalStore) error
	GetTransGlobals(lid int, globals interface{})
	GetBranches(gid string) []TransBranchStore
	UpdateBranches(branches []TransBranchStore, updates []string) *gorm.DB
	LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore, branchStart int) error
	SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error
	ChangeGlobalStatus(global *TransGlobalStore, oldStatus string, updates []string, finished bool) error
	TouchCronTime(global *TransGlobalStore, nextCronInterval int64)
	LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration) error
}

// var store Store

func GetStore() Store {
	if config.DB["driver"] == dtmimp.DBTypeRedis {
		return &RedisStore{}
	} else {
		return &SqlStore{}
	}

}

func wrapError(err error) error {
	if err == gorm.ErrRecordNotFound || err == redis.Nil {
		return ErrNotFound
	}
	dtmimp.E2P(err)
	return err
}

package storage

import (
	"errors"

	"github.com/go-redis/redis/v8"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
)

var ErrNotFound = errors.New("storage: NotFound")
var ErrUniqueConflict = errors.New("storage: UniqueKeyConflict")

// type Store interface {
// 	GetTransGlobal(gid string, trans TransGlobalStore) error
// 	LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore) error
// 	SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error
// }

// var store Store

func GetStore() *SqlStore {
	return &SqlStore{}
}

func wrapError(err error) error {
	if err == gorm.ErrRecordNotFound || err == redis.Nil {
		return ErrNotFound
	}
	dtmimp.E2P(err)
	return err
}

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
	GetTransGlobal(gid string, trans *TransGlobalStore) error
	GetTransGlobals(lid int, globals interface{})
	GetBranches(gid string) []TransBranchStore
	UpdateBranches(branches []TransBranchStore, updates []string) *gorm.DB
	LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore) error
	SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error
	ChangeGlobalStatus(global *TransGlobalStore, oldStatus string, updates []string)
	TouchGlobal(global *TransGlobalStore, updates []string)
	LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration, updates []string) error
}

// var store Store

func GetStore() Store {
	return &SqlStore{}
}

func wrapError(err error) error {
	if err == gorm.ErrRecordNotFound || err == redis.Nil {
		return ErrNotFound
	}
	dtmimp.E2P(err)
	return err
}

package storage

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SqlStore struct {
}

func (s *SqlStore) GetTransGlobal(gid string, trans *TransGlobalStore) error {
	dbr := dbGet().Model(trans).Where("gid=?", gid).First(trans)
	return wrapError(dbr.Error)
}

func (s *SqlStore) LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore) error {
	return dbGet().Transaction(func(tx *gorm.DB) error {
		err := lockTransGlobal(tx, gid, status)
		if err != nil {
			return err
		}
		dbr := tx.Save(branches)
		return dbr.Error
	})
}

func (s *SqlStore) SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error {
	return dbGet().Transaction(func(db1 *gorm.DB) error {
		db := &common.DB{DB: db1}
		dbr := db.Must().Clauses(clause.OnConflict{
			DoNothing: true,
		}).Create(global)
		if dbr.RowsAffected <= 0 { // 如果这个不是新事务，返回错误
			return ErrUniqueConflict
		}
		if len(branches) > 0 {
			db.Must().Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&branches)
		}
		return nil
	})
}

func (s *SqlStore) ChangeGlobalStatus(global *TransGlobalStore, oldStatus string, updates []string) {
	dbr := dbGet().Must().Model(global).Where("status=? and gid=?", oldStatus, global.Gid).Select(updates).Updates(global)
	checkAffected(dbr)
}

func (s *SqlStore) LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration, updates []string) error {
	db := dbGet()
	getTime := dtmimp.GetDBSpecial().TimestampAdd
	expire := int(expireIn / time.Second)
	whereTime := fmt.Sprintf("next_cron_time < %s and update_time < %s", getTime(expire), getTime(expire-3))
	// 这里next_cron_time需要限定范围，否则数据量累计之后，会导致查询变慢
	// 限定update_time < now - 3，否则会出现刚被这个应用取出，又被另一个取出
	owner := uuid.NewString()
	dbr := db.Must().Model(global).
		Where(whereTime+"and status in ('prepared', 'aborting', 'submitted')").Limit(1).Update("owner", owner)
	if dbr.RowsAffected == 0 {
		return ErrNotFound
	}
	dbr = db.Must().Where("owner=?", owner).Find(global)
	db.Must().Model(global).Select(updates).Updates(global)
	return nil
}

func lockTransGlobal(db *gorm.DB, gid string, status string) error {
	g := &TransGlobalStore{}
	dbr := db.Clauses(clause.Locking{Strength: "UPDATE"}).Model(g).Where("gid=? and status=?", gid, status).First(g)
	return wrapError(dbr.Error)
}

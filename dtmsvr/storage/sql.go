package storage

import (
	"github.com/yedf/dtm/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SqlStore struct {
}

func (s *SqlStore) GetTransGlobal(gid string, trans interface{}) error {
	dbr := dbGet().Model(trans).Where("gid=?", gid).First(trans)
	return wrapError(dbr.Error)
}

func (s *SqlStore) LockGlobalSaveBranches(gid string, status string, branches interface{}) error {
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

func lockTransGlobal(db *gorm.DB, gid string, status string) error {
	g := &TransGlobalStore{}
	dbr := db.Clauses(clause.Locking{Strength: "UPDATE"}).Model(g).Where("gid=? and status=?", gid, status).First(g)
	return wrapError(dbr.Error)
}

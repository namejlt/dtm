package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const prefix = "{}"

var ctx context.Context = context.Background()

func gkey(gid string) string {
	return prefix + "-g-" + gid
}

func bkey(gid string) string {
	return prefix + "-b-" + gid
}

type RedisStore struct {
}

func (s *RedisStore) GetTransGlobal(gid string, trans *TransGlobalStore) error {
	r, err := redisGet().Get(ctx, prefix+"-g-"+gid).Result()
	if err != nil {
		return err
	}
	dtmimp.MustUnmarshalString(r, trans)
	return nil
}

func (s *RedisStore) GetTransGlobals(lid int, globals interface{}) {
	panic("not implemented")
}

func (s *RedisStore) GetBranches(gid string) []TransBranchStore {
	sa, err := redisGet().LRange(ctx, prefix+"-b-"+gid, 0, -1).Result()
	dtmimp.E2P(err)
	branches := make([]TransBranchStore, len(sa))
	for k, v := range sa {
		dtmimp.MustUnmarshalString(v, &branches[k])
	}
	return branches
}

func (s *RedisStore) UpdateBranches(branches []TransBranchStore, updates []string) *gorm.DB {
	panic("not implemented")
}

func (s *RedisStore) LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore) error {
	keys := []string{gkey(gid)}
	values := []interface{}{status}
	for _, b := range branches {
		keys = append(keys, bkey(gid))
		values = append(values, dtmimp.MustMarshalString(b))
	}
	redisGet().Eval(ctx, `
local g = redis.call('GET', KEYS[0])
local js = cjson.decode(g)
if (js.status ~= VALUES[1]) then
	return 'unexpected status: ' + js.status
end
for k = 2, table.getn(KEYS) do
	redis.call('LSET', KEYS[k], k-2, VALUES[k])
end
return ''
	`, keys, values...)
	return dbGet().Transaction(func(tx *gorm.DB) error {
		err := lockTransGlobal(tx, gid, status)
		if err != nil {
			return err
		}
		dbr := tx.Save(branches)
		return dbr.Error
	})
}

func (s *RedisStore) SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error {
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

func (s *RedisStore) ChangeGlobalStatus(global *TransGlobalStore, oldStatus string, updates []string) {
	dbr := dbGet().Must().Model(global).Where("status=? and gid=?", oldStatus, global.Gid).Select(updates).Updates(global)
	checkAffected(dbr)
}

func (s *RedisStore) TouchGlobal(global *TransGlobalStore, updates []string) {
	dbGet().Must().Model(global).Where("status=? and gid=?", global.Status, global.Gid).Select(updates).Updates(global)
}

func (s *RedisStore) LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration, updates []string) error {
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

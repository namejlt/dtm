package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
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

func buildReq(global *TransGlobalStore, branches []TransBranchStore) ([]string, []interface{}) {
	keys := []string{"prefix", "global"}
	values := []interface{}{prefix, dtmimp.MustMarshalString(global)}
	for _, b := range branches {
		keys = append(keys, "branch")
		values = append(values, dtmimp.MustMarshalString(b))
	}
	return keys, values
}
func (s *RedisStore) LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore) error {
	keys, values := buildReq(&TransGlobalStore{Gid: gid, Status: status}, branches)
	r, err := redisGet().Eval(ctx, `
local gs = cjson.decode(ARGS[2])
local g = redis.call('GET', ARGS[1]+'-g-'+ gs.Gid)
if (g == '') then
	return 'LOCK_FAILED'
end
local js = cjson.decode(g)
if (js.status ~= gs.status) then
	return 'LOCK_FAILED'
end
for k = 3, table.getn(KEYS) do
	redis.call('LSET', KEYS[k], k-3, VALUES[k])
end
return ''
	`, keys, values...).Result()
	if r == "LOCK_FAILED" {
		return ErrNotFound
	}
	return err
}

func (s *RedisStore) SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error {
	keys, args := buildReq(global, branches)
	r, err := redisGet().Eval(ctx, `
local gs = cjson.decode(ARGS[2])
local g = redis.call('GET', ARGS[1]+'-g-'+gs.gid)
if (g ~= '') then
	return 'EXISTS'
end
redis.call('SET', KEYS[1], VALUES[1])
redis.call('SET', ARGS[1]+'-i-'+gs.next_cron_time+gs.gid, ARGS[2])
for k = 2, table.getn(KEYS) do
	redis.call('LSET', KEYS[k], k-2, VALUES[k])
end
return ''
	`, keys, args...).Result()
	if r == "EXISTS" {
		return ErrUniqueConflict
	}
	return err
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

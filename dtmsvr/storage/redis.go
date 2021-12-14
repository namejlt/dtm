package storage

import (
	"context"
	"errors"
	"time"

	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
)

const prefix = "{0}"

var ctx context.Context = context.Background()

func gkey(gid string) string {
	return prefix + "-g-" + gid
}

func bkey(gid string) string {
	return prefix + "-b-" + gid
}

type RedisStore struct {
}

func (s *RedisStore) PopulateData(skipDrop bool) {
	_, err := redisGet().FlushAll(ctx).Result()
	dtmimp.PanicIf(err != nil, err)
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

type argList struct {
	List []interface{}
}

func newArgList() *argList { return &argList{} }

func (a *argList) AppendRaw(v interface{}) *argList {
	a.List = append(a.List, v)
	return a
}

func (a *argList) AppendObject(v interface{}) *argList {
	a.List = append(a.List, dtmimp.MustMarshalString(v))
	return a
}

func (a *argList) AppendBranches(branches []TransBranchStore) *argList {
	for _, b := range branches {
		a.List = append(a.List, dtmimp.MustMarshalString(b))
	}
	return a
}

func (s *RedisStore) LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore) error {
	args := newArgList().
		AppendRaw(prefix).
		AppendObject(&TransGlobalStore{Gid: gid, Status: status}).
		AppendBranches(branches).
		List
	r, err := redisGet().Eval(ctx, `
local pre = ARGS[1]
local gs = cjson.decode(ARGS[2])
local g = redis.call('GET', pre+'-g-'+ gs.Gid)
if (g == '') then
	return 'LOCK_FAILED'
end
local js = cjson.decode(g)
if (js.status ~= gs.status) then
	return 'LOCK_FAILED'
end
for k = 2, table.getn(ARGS) do
	redis.call('LSET', pre+'-b-', k-2, ARGS[k-2])
end
return ''
	`, []string{"-"}, args...).Result()
	if r == "LOCK_FAILED" {
		return ErrNotFound
	}
	return err
}

func (s *RedisStore) SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error {
	args := newArgList().AppendRaw(prefix).AppendObject(global).AppendBranches(branches).List
	r, err := redisGet().Eval(ctx, `
local gs = cjson.decode(ARGS[2])
local g = redis.call('GET', ARGS[1]+'-g-'+gs.gid)
if (g ~= '') then
	return 'EXISTS'
end
redis.call('SET', KEYS[1], VALUES[1])
redis.call('SET', ARGS[1]+'-u-'+gs.next_cron_time+gs.gid, ARGS[2])
for k = 2, table.getn(KEYS) do
	redis.call('LSET', KEYS[k], k-2, VALUES[k])
end
return ''
	`, []string{"-"}, args...).Result()
	if r == "EXISTS" {
		return ErrUniqueConflict
	}
	return err
}

func (s *RedisStore) ChangeGlobalStatus(global *TransGlobalStore, newStatus string, updates []string, finished bool) {
	old := global.Status
	global.Status = newStatus
	args := newArgList().AppendObject(global).AppendRaw(old).List
	r, err := redisGet().Eval(ctx, `
local p = ARGS[1]
local gs = cjson.decode(ARGS[2])
local old = redis.call('GET', p+'-g-'+gid)
local os = cjson.decode(old)
if (os.status ~= ARGS[3]) then
  return 'LOCK_FAILED'
end
redis.call('SET', p+'-g-'+gid,  ARGS[2])
return ''
`, []string{"-"}, args...).Result()
	if r != "" {
		panic("bad result")
	}
	dtmimp.PanicIf(err != nil, err)
}

func (s *RedisStore) TouchCronTime(global *TransGlobalStore, nextCronInterval int64) {
	global.NextCronTime = common.GetNextTime(nextCronInterval)
	global.UpdateTime = common.GetNextTime(0)
	global.NextCronInterval = nextCronInterval
	args := newArgList().AppendObject(global).AppendRaw(global.NextCronTime.Unix()).List
	r, err := redisGet().Eval(ctx, `
local p = ARGS[1]
local g = cjson.decode(ARGS[2])
redis.call('ZADD', p+'-u', g.gid, ARGS[3])
redis.call('SET', p+'-g-'+g.gid, ARGS[2])
	`, []string{"-"}, args...).Result()
	if r != "" {
		panic(errors.New("redis error"))
	}
	dtmimp.E2P(err)
}

func (s *RedisStore) LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration) error {
	unixNow := time.Now().Add(expireIn).Unix()
	args := newArgList().AppendRaw(prefix).AppendRaw(unixNow).AppendRaw(common.DtmConfig.RetryInterval).List
	r, err := redisGet().Eval(ctx, `
local k = ARGS[1]+'-u'
local gid = redis.call('ZRANGE', k, 0, 0)
if (gid == '') then
	return 'NOT_FOUND'
end
local g = redis.call('GET', gid)
if (g == '') then
	redis.call('ZREM', k, gid)
	return 'BAD_GID'
end

redis.call('ZADD', k, gid, now+10)
return g
	`, []string{"-"}, args...).Result()
	if err != nil {
		return err
	}
	if r == "NOT_FOUND" {
		return ErrNotFound
	} else if r != "" {
		return errors.New("ERROR")
	}
	return nil
}

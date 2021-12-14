package storage

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
)

const prefix = "{-}"

var ctx context.Context = context.Background()

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

func newArgList() *argList {
	a := &argList{}
	return a.AppendRaw(prefix)
}

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

func callLua(args []interface{}, lua string) error {
	dtmimp.Logf("calling lua. args: %v\nlua:%s", args, lua)
	r, err := redisGet().Eval(ctx, lua, []string{"-"}, args...).Result()
	dtmimp.Logf("result is: '%v', err: '%v'", r, err)
	if err != nil && err != redis.Nil {
		return err
	}
	s := r.(string)
	err = map[string]error{
		"NOT_FOUND":       ErrNotFound,
		"UNIQUE_CONFLICT": ErrUniqueConflict,
	}[s]
	if err == nil && s != "" {
		return errors.New(s)
	}
	return err
}

func (s *RedisStore) SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error {
	args := newArgList().
		AppendObject(global).
		AppendRaw(global.NextCronTime.Unix()).
		AppendBranches(branches).
		List

	return callLua(args, `
local gs = cjson.decode(ARGV[2])
local g = redis.call('GET', ARGV[1] .. '-g-' .. gs.gid)
if g ~= false then
	return 'UNIQUE_CONFLICT'
end

redis.call('SET', ARGV[1] .. '-g-' .. gs.gid, ARGV[2])
redis.call('ZADD', ARGV[1] .. '-u-' .. gs.gid, ARGV[3], gs.gid)
for k = 4, table.getn(ARGV) do
	redis.call('RPUSH', ARGV[1] .. '-b-' .. gs.gid, ARGV[k])
end
return ''
`)
}

func (s *RedisStore) LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore, branchStart int) error {
	args := newArgList().
		AppendObject(&TransGlobalStore{Gid: gid, Status: status}).
		AppendRaw(branchStart).
		AppendBranches(branches).
		List
	return callLua(args, `
local pre = ARGV[1]
local gs = cjson.decode(ARGV[2])
local g = redis.call('GET', pre .. '-g-' .. gs.gid)
if (g == false) then
	return 'NOT_FOUND'
end
local js = cjson.decode(g)
if js.status ~= gs.status then
	return 'NOT_FOUND'
end
local start = ARGV[3]
for k = 4, table.getn(ARGV) do
	if start == "-1" then
		redis.call('RPUSH', pre .. '-b-' .. gs.gid, ARGV[k])
	else
		redis.call('LSET', pre .. '-b-' .. gs.gid, start+k-4, ARGV[k])
	end
end
return ''
	`)
}

func (s *RedisStore) ChangeGlobalStatus(global *TransGlobalStore, newStatus string, updates []string, finished bool) {
	old := global.Status
	global.Status = newStatus
	args := newArgList().AppendObject(global).AppendRaw(old).List
	err := callLua(args, `
local p = ARGV[1]
local gs = cjson.decode(ARGV[2])
local old = redis.call('GET', p+'-g-'+gid)
local os = cjson.decode(old)
if (os.status ~= ARGV[3]) then
  return 'LOCK_FAILED'
end
redis.call('SET', p+'-g-'+gid,  ARGV[2])
return ''
`)
	dtmimp.E2P(err)
}

func (s *RedisStore) TouchCronTime(global *TransGlobalStore, nextCronInterval int64) {
	global.NextCronTime = common.GetNextTime(nextCronInterval)
	global.UpdateTime = common.GetNextTime(0)
	global.NextCronInterval = nextCronInterval
	args := newArgList().AppendObject(global).AppendRaw(global.NextCronTime.Unix()).List
	err := callLua(args, `
local p = ARGV[1]
local g = cjson.decode(ARGV[2])
redis.call('ZADD', p+'-u', g.gid, ARGV[3])
redis.call('SET', p+'-g-'+g.gid, ARGV[2])
	`)
	dtmimp.E2P(err)
}

func (s *RedisStore) LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration) error {
	unixNow := time.Now().Add(expireIn).Unix()
	args := newArgList().AppendRaw(prefix).AppendRaw(unixNow).AppendRaw(common.DtmConfig.RetryInterval).List
	return callLua(args, `
local k = ARGV[1]+'-u'
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
	`)
}

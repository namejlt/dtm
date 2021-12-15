package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"gorm.io/gorm"
)

const prefix = "a"

var ctx context.Context = context.Background()

type RedisStore struct {
}

func (s *RedisStore) PopulateData(skipDrop bool) {
	_, err := redisGet().FlushAll(ctx).Result()
	dtmimp.PanicIf(err != nil, err)
}

func (s *RedisStore) GetTransGlobal(gid string, trans *TransGlobalStore) error {
	r, err := redisGet().Get(ctx, prefix+"_g_"+gid).Result()
	if err == redis.Nil {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	dtmimp.MustUnmarshalString(r, trans)
	return nil
}

func (s *RedisStore) GetTransGlobals(lastID *string, globals *[]TransGlobalStore) {
	lid := uint64(0)
	if *lastID != "" {
		lid = uint64(dtmimp.MustAtoi(*lastID))
	}
	keys, cursor, err := redisGet().Scan(ctx, lid, prefix+"_g_*", 100).Result()
	dtmimp.E2P(err)
	*lastID = fmt.Sprintf("%d", cursor)
	values, err := redisGet().MGet(ctx, keys...).Result()
	dtmimp.E2P(err)
	for _, v := range values {
		global := TransGlobalStore{}
		dtmimp.MustUnmarshalString(v.(string), &global)
		*globals = append(*globals, global)
	}
}

func (s *RedisStore) GetBranches(gid string) []TransBranchStore {
	sa, err := redisGet().LRange(ctx, prefix+"_b_"+gid, 0, -1).Result()
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

func handleRedisResult(ret interface{}, err error) (string, error) {
	dtmimp.Logf("result is: '%v', err: '%v'", ret, err)
	if err != nil && err != redis.Nil {
		return "", err
	}
	s, _ := ret.(string)
	err = map[string]error{
		"NOT_FOUND":       ErrNotFound,
		"UNIQUE_CONFLICT": ErrUniqueConflict,
	}[s]
	return s, err
}

func callLua(args []interface{}, lua string) (string, error) {
	dtmimp.Logf("calling lua. args: %v\nlua:%s", args, lua)
	ret, err := redisGet().Eval(ctx, lua, []string{"-"}, args...).Result()
	return handleRedisResult(ret, err)
}

func (s *RedisStore) SaveNewTrans(global *TransGlobalStore, branches []TransBranchStore) error {
	now := time.Now()
	global.CreateTime = &now
	global.UpdateTime = &now
	args := newArgList().
		AppendObject(global).
		AppendRaw(global.NextCronTime.Unix()).
		AppendBranches(branches).
		List

	_, err := callLua(args, `
local gs = cjson.decode(ARGV[2])
local g = redis.call('GET', ARGV[1] .. '_g_' .. gs.gid)
if g ~= false then
	return 'UNIQUE_CONFLICT'
end

redis.call('SET', ARGV[1] .. '_g_' .. gs.gid, ARGV[2])
redis.call('ZADD', ARGV[1] .. '_u', ARGV[3], gs.gid)
for k = 4, table.getn(ARGV) do
	redis.call('RPUSH', ARGV[1] .. '_b_' .. gs.gid, ARGV[k])
end
`)
	return err
}

func (s *RedisStore) LockGlobalSaveBranches(gid string, status string, branches []TransBranchStore, branchStart int) error {
	args := newArgList().
		AppendObject(&TransGlobalStore{Gid: gid, Status: status}).
		AppendRaw(branchStart).
		AppendBranches(branches).
		List
	_, err := callLua(args, `
local pre = ARGV[1]
local gs = cjson.decode(ARGV[2])
local g = redis.call('GET', pre .. '_g_' .. gs.gid)
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
		redis.call('RPUSH', pre .. '_b_' .. gs.gid, ARGV[k])
	else
		redis.call('LSET', pre .. '_b_' .. gs.gid, start+k-4, ARGV[k])
	end
end
	`)
	return err
}

func (s *RedisStore) ChangeGlobalStatus(global *TransGlobalStore, newStatus string, updates []string, finished bool) error {
	old := global.Status
	global.Status = newStatus
	args := newArgList().AppendObject(global).AppendRaw(old).AppendRaw(finished).List
	_, err := callLua(args, `
local p = ARGV[1]
local gs = cjson.decode(ARGV[2])
local old = redis.call('GET', p .. '_g_' .. gs.gid)
if old == false then
	return 'NOT_FOUND'
end
local os = cjson.decode(old)
if os.status ~= ARGV[3] then
  return 'NOT_FOUND'
end
redis.call('SET', p .. '_g_' .. gs.gid,  ARGV[2])
redis.log(redis.LOG_WARNING, 'finished: ', ARGV[4])
if ARGV[4] == '1' then
	redis.call('ZREM', p .. '_u', gs.gid)
end
`)
	return err
}

func (s *RedisStore) LockOneGlobalTrans(global *TransGlobalStore, expireIn time.Duration) error {
	expired := time.Now().Add(expireIn).Unix()
	next := time.Now().Add(time.Duration(config.RetryInterval) * time.Second).Unix()
	args := newArgList().AppendRaw(expired).AppendRaw(next).List
	r, err := callLua(args, `
local k = ARGV[1] .. '_u'
local r = redis.call('ZRANGE', k, 0, 0, 'WITHSCORES')
local gid = r[1]
if gid == nil then
	return 'NOT_FOUND'
end
local g = redis.call('GET', ARGV[1] .. '_g_' .. gid)
redis.log(redis.LOG_WARNING, 'g is: ', g, 'gid is: ', gid)
if g == false then
	redis.call('ZREM', k, gid)
	return 'NOT_FOUND'
end

if tonumber(r[2]) > tonumber(ARGV[2]) then
	return 'NOT_FOUND'
end
redis.call('ZADD', k, ARGV[3], gid)
return g
	`)
	if err == nil {
		dtmimp.MustUnmarshalString(r, global)
	}
	return err
}

func (s *RedisStore) TouchCronTime(global *TransGlobalStore, nextCronInterval int64) {
	global.NextCronTime = common.GetNextTime(nextCronInterval)
	global.UpdateTime = common.GetNextTime(0)
	global.NextCronInterval = nextCronInterval
	args := newArgList().AppendObject(global).AppendRaw(global.NextCronTime.Unix()).List
	_, err := callLua(args, `
local p = ARGV[1]
local g = cjson.decode(ARGV[2])
redis.call('ZADD', p .. '_u', ARGV[3], g.gid)
redis.call('SET', p .. '_g_' .. g.gid, ARGV[2])
	`)
	dtmimp.E2P(err)
}

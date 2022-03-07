package workers

import "github.com/garyburd/redigo/redis"

// KEYS[1]: queue.
// KEYS[2]: inprogress list.
// KEYS[3]: arguments hash table.
// ARGV[1]: time out.
var popMessageScript = redis.NewScript(3, `
	local jid = redis.call('RPOP', KEYS[1])
	local data = nil
	if jid ~= false then
		redis.call('ZADD', KEYS[2], ARGV[1], jid)
		data = redis.call('HGET', KEYS[3], jid)
	end
	return data
`)

// KEYS[1]: delay queue.
// KEYS[2]: arguments hash table.
// ARGV[1]: jid.
// ARGV[2]: scheduled at.
// ARGV[3]: job arguments.
var enqueueAtScript = redis.NewScript(2, `
	redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
	redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
`)

// KEYS[1]: queue.
// KEYS[2]: arguments hash table.
// ARGV[1]: jid.
// ARGV[2]: job arguments.
var enqueueScript = redis.NewScript(2, `
	redis.call('LPUSH', KEYS[1], ARGV[1])
	redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
`)

// KEYS[1]: delay queue.
// KEYS[2]: arguments hash table.
// ARGV[1]: time now.
var scheduledScript = redis.NewScript(2, `
	local jid = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'limit', 0, 1)
	if #jid > 0 then
		local data = redis.call('HGET', KEYS[2], jid[1])
		return {jid[1], data}
	else
		return {}
	end
`)

// KEYS[1]: delay queue.
// KEYS[2]: arguments hash table.
// ARGV[1]: jid.
// ARGV[2]: set arguments.
var remFromZ = redis.NewScript(2, `
	local removed = redis.call('ZREM', KEYS[1], ARGV[1])
	if ARGV[2] ~= nil then
		redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
	else
		redis.call('HDEL', KEYS[2], ARGV[1])
	end
	return removed
`)

// KEYS[1]: delay queue.
// KEYS[2]: queue.
// KEYS[3]: arguments hash table.
// ARGV[1]: jid.
// ARGV[2]: job arguments.
var moveToL = redis.NewScript(3, `
	local removed = redis.call('ZREM', KEYS[1], ARGV[1])
	if removed ~= 0 then
		redis.call('LPUSH', KEYS[2], ARGV[1])
		redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
	end
	return removed
`)

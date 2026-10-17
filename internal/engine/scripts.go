package engine

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Redis layout of a namespace. Every key carries the namespace in a hash
// tag, so a namespace lives on a single Redis Cluster slot and the scripts
// below can touch several of its keys atomically.
//
//	<prefix>{ns}:m        hash: spec, epoch, floor, wrev
//	<prefix>{ns}:k:<key>  string: entry (see entry.go), always with a TTL
//	<prefix>{ns}:i        stream: invalidation log (fields k, r; or x on reset)
//	<prefix>{ns}:w        string: watcher lease holder, with a TTL
//
// Meta fields:
//
//	spec   the normalized NamespaceSpec (protobuf)
//	epoch  8 random bytes; changing it kills every entry at once (purge)
//	floor  loads that read below this revision are never installed
//	wrev   every backend change up to this revision has been applied
type keyspace struct {
	meta, stream, lease, entryPrefix string
}

func newKeyspace(prefix, ns string) keyspace {
	base := prefix + "{" + ns + "}:"
	return keyspace{
		meta:        base + "m",
		stream:      base + "i",
		lease:       base + "w",
		entryPrefix: base + "k:",
	}
}

func (k keyspace) entry(key string) string { return k.entryPrefix + key }

// Script result codes shared by the scripts.
const (
	resNamespaceMissing = -1
	resEpochChanged     = -2
	resRejected         = 0
	resStored           = 1
	resRefreshed        = 2
)

// luaHeader reads the current entry header. cur_kind is 0 when there is no
// live entry (absent, or from another epoch); cur_quiet is its quiet flag
// (see installScript).
const luaHeader = `
local meta_epoch = redis.call('HGET', KEYS[1], 'epoch')
if not meta_epoch then return {-1} end
if meta_epoch ~= ARGV[2] then return {-2, meta_epoch} end
local cur_kind, cur_rev, cur_quiet = 0, 0, false
local hdr = redis.call('GETRANGE', KEYS[2], 0, 24)
if #hdr == 25 and string.sub(hdr, 2, 9) == meta_epoch then
  local b = struct.unpack('>B', hdr)
  cur_quiet = b >= 128
  cur_kind = b % 128
  cur_rev = struct.unpack('>i8', hdr, 10)
end
`

// installScript applies one state change to an entry, guarded by revision.
//
// KEYS: meta, entry, stream
// ARGV: mode, epoch, rev, read_rev, payload, ttl_ms, fence_payload,
// fence_ttl_ms, stream_maxlen, key
//
// Modes:
//
//	fill   a load (read-through, refresh, cache-aside fill). Installed if it
//	       read at or after the floor and any fence, and is not older than
//	       the cached state. Equal state just refreshes storedAt and TTL.
//	write  the result of a write-through write (rev = write revision).
//	fence  a write-around write: drop the cached state if older than rev.
//	watch  a backend change: update a cached entry in place, otherwise fence.
//	drop   explicit invalidation: delete, and tell near-caches to drop
//	       unconditionally (r=0).
//
// Every state change is published to the invalidation stream, so that
// near-caches drop older copies. A load (fill) is the exception, because
// the change it reflects is normally published by whoever made it: a write
// through the cache, or the watcher. But a load can see a write that
// bypassed the cache before the watcher does; the watcher's event then
// finds Redis already up to date, and without care nobody would publish
// the change. So a fill into an empty slot or over a fence marks the entry
// "quiet" (bit 0x80 of the kind byte), and the first later sign of that
// change (watch event, write) publishes it and clears the flag. A fill that
// replaces an older state publishes at once.
var installScript = redis.NewScript(luaHeader + `
local mode = ARGV[1]
local rev = tonumber(ARGV[3])
local function publish(r)
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[9], '*', 'k', ARGV[10], 'r', r)
end
local function quiet(payload)
  return string.char(string.byte(payload, 1) + 128) .. string.sub(payload, 2)
end
-- A change at or below a quiet entry's revision: publish it, and once the
-- entry's own change is published the entry is no longer quiet.
local function announce_quiet()
  if cur_quiet and rev <= cur_rev then
    publish(ARGV[3])
    if rev == cur_rev then redis.call('SETRANGE', KEYS[2], 0, string.char(cur_kind)) end
  end
end

if mode == 'fill' then
  local read_rev = tonumber(ARGV[4])
  local floor = tonumber(redis.call('HGET', KEYS[1], 'floor') or '0')
  if read_rev < floor then return {0} end
  local payload = ARGV[5]
  if cur_kind == 3 then
    if read_rev < cur_rev then return {0} end
    -- The fence's change was published; anything newer may not be.
    if rev > cur_rev then payload = quiet(payload) end
  elseif cur_kind ~= 0 then
    if rev < cur_rev then return {0} end
    if rev == cur_rev then
      if cur_quiet then payload = quiet(payload) end
      redis.call('SET', KEYS[2], payload, 'PX', ARGV[6])
      return {2}
    end
    redis.call('SET', KEYS[2], payload, 'PX', ARGV[6])
    publish(ARGV[3])
    return {1}
  else
    payload = quiet(payload)
  end
  redis.call('SET', KEYS[2], payload, 'PX', ARGV[6])
  return {1}
end

if mode == 'write' then
  if cur_kind == 3 then
    if rev < cur_rev then return {0} end
  elseif cur_kind ~= 0 then
    if rev <= cur_rev then
      announce_quiet()
      return {0}
    end
  end
  redis.call('SET', KEYS[2], ARGV[5], 'PX', ARGV[6])
  if cur_kind == 0 or rev > cur_rev then publish(ARGV[3]) end
  return {1}
end

if mode == 'fence' then
  if cur_kind ~= 0 and rev <= cur_rev then
    announce_quiet()
    return {0}
  end
  redis.call('SET', KEYS[2], ARGV[7], 'PX', ARGV[8])
  publish(ARGV[3])
  return {1}
end

if mode == 'watch' then
  if cur_kind ~= 0 and rev <= cur_rev then
    announce_quiet()
    return {0}
  end
  if cur_kind == 1 or cur_kind == 2 then
    redis.call('SET', KEYS[2], ARGV[5], 'PX', ARGV[6])
  else
    redis.call('SET', KEYS[2], ARGV[7], 'PX', ARGV[8])
  end
  publish(ARGV[3])
  return {1}
end

if mode == 'drop' then
  redis.call('DEL', KEYS[2])
  publish('0')
  return {1}
end

return redis.error_reply('unknown mode ' .. mode)
`)

// getScript reads an entry together with the namespace epoch.
//
// KEYS: meta, entry
// Returns {-1} if the namespace is missing, else {1, epoch, entry|nil}.
var getScript = redis.NewScript(`
local e = redis.call('HGET', KEYS[1], 'epoch')
if not e then return {-1} end
return {1, e, redis.call('GET', KEYS[2])}
`)

// registerScript creates a namespace unless it exists.
//
// KEYS: meta
// ARGV: spec, epoch, revision (initial floor and wrev)
// Returns {created(0|1), spec, epoch}.
var registerScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'spec')
if cur then return {0, cur, redis.call('HGET', KEYS[1], 'epoch')} end
redis.call('HSET', KEYS[1], 'spec', ARGV[1], 'epoch', ARGV[2], 'floor', ARGV[3], 'wrev', ARGV[3])
return {1, ARGV[1], ARGV[2]}
`)

// metaScript reads the namespace meta.
//
// KEYS: meta
// Returns {-1} or {1, spec, epoch, floor, wrev}.
var metaScript = redis.NewScript(`
local m = redis.call('HMGET', KEYS[1], 'spec', 'epoch', 'floor', 'wrev')
if not m[1] or not m[2] then return {-1} end
return {1, m[1], m[2], m[3] or '0', m[4] or '0'}
`)

// purgeScript kills every entry of a namespace by changing its epoch, and
// tells near-caches to reset.
//
// KEYS: meta, stream
// ARGV: expected epoch, new epoch, floor, wrev, stream maxlen
// (an empty floor or wrev keeps the current one)
//
// Returns {-1}, {-2, epoch} or {1}.
var purgeScript = redis.NewScript(`
local e = redis.call('HGET', KEYS[1], 'epoch')
if not e then return {-1} end
if e ~= ARGV[1] then return {-2, e} end
redis.call('HSET', KEYS[1], 'epoch', ARGV[2])
if ARGV[3] ~= '' then
  local floor = tonumber(redis.call('HGET', KEYS[1], 'floor') or '0')
  if tonumber(ARGV[3]) > floor then redis.call('HSET', KEYS[1], 'floor', ARGV[3]) end
end
if ARGV[4] ~= '' then redis.call('HSET', KEYS[1], 'wrev', ARGV[4]) end
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[5], '*', 'x', ARGV[2])
return {1}
`)

// wrevScript records watch progress. wrev only moves forward.
//
// KEYS: meta
// ARGV: revision
// Returns -1 if the namespace is missing, else the stored wrev.
var wrevScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], 'spec') == 0 then return -1 end
local cur = tonumber(redis.call('HGET', KEYS[1], 'wrev') or '0')
local rev = tonumber(ARGV[1])
if rev > cur then
  redis.call('HSET', KEYS[1], 'wrev', ARGV[1])
  return rev
end
return cur
`)

// leaseScript acquires or renews the watcher lease.
//
// KEYS: lease
// ARGV: holder, ttl_ms
// Returns 1 if the caller holds the lease.
var leaseScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`)

// releaseScript releases the watcher lease if the caller holds it.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

var allScripts = []*redis.Script{installScript, getScript, registerScript, metaScript, purgeScript, wrevScript, leaseScript, releaseScript}

// loadScripts loads every script into the Redis script cache.
func loadScripts(ctx context.Context, rdb redis.UniversalClient) error {
	for _, s := range allScripts {
		if err := s.Load(ctx, rdb).Err(); err != nil {
			return err
		}
	}
	return nil
}

func isNoScript(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT")
}

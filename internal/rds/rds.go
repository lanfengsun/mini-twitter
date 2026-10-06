// Package rds centralises Redis key names and the Lua scripts that need atomicity.
package rds

import (
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	StreamEvents = "events"      // the write queue
	StreamDLQ    = "events:dlq"  // events that failed permanently
	Group        = "workers"     // consumer group
	KeyCelebs    = "celebs"      // set of user ids whose posts are NOT fanned out
	KeyDirty     = "dirty:posts" // set of post ids with unflushed like/reply deltas
	KeyNodeSeq   = "idgen:node"  // counter used to hand each API instance an id-generator node
)

func n(id int64) string { return strconv.FormatInt(id, 10) }

func Session(token string) string    { return "sess:" + token }
func UserID(username string) string  { return "uid:" + username }        // username -> id cache
func Feed(uid int64) string          { return "feed:" + n(uid) }         // zset of post ids, score = id
func AuthorPosts(uid int64) string   { return "author_posts:" + n(uid) } // zset: author's latest posts
func Following(uid int64) string     { return "following:" + n(uid) }    // set of followee ids (+ "0" sentinel)
func FollowerCount(uid int64) string { return "fcnt:" + n(uid) }
func Post(id int64) string           { return "post:" + n(id) } // hash: cached post
func Counts(id int64) string         { return "cnt:" + n(id) }  // hash: unflushed likes/replies deltas

// ScriptZAddIfExists adds a post id to a sorted set only if the key already exists,
// then trims to the newest ARGV[2] entries and refreshes the TTL (ARGV[3] seconds).
// Used for fan-out: inactive users have no feed key, so they cost nothing.
var ScriptZAddIfExists = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1])
  redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[2]) + 1))
  redis.call('EXPIRE', KEYS[1], ARGV[3])
  return 1
end
return 0
`)

// ScriptIncrIfExists increments a counter only if it exists (a missing counter is
// recomputed from Postgres instead of silently starting from 1). Returns -1 if absent.
var ScriptIncrIfExists = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.call('INCR', KEYS[1])
end
return -1
`)

// ScriptTakeCounts atomically reads and deletes the unflushed {likes, replies} deltas
// of one post.
var ScriptTakeCounts = redis.NewScript(`
local l = redis.call('HGET', KEYS[1], 'likes')
local r = redis.call('HGET', KEYS[1], 'replies')
redis.call('DEL', KEYS[1])
return {l or '0', r or '0'}
`)

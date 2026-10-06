// Package feedsvc is the feed engine shared by the API (reads) and the worker (writes):
// hybrid fan-out (push for normal authors, merge-at-read for celebrities), lazy rebuild
// of cold feeds, and post hydration with a Redis cache.
package feedsvc

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"minitwitter/internal/feed"
	"minitwitter/internal/idgen"
	"minitwitter/internal/metrics"
	"minitwitter/internal/rds"
	"minitwitter/internal/store"
)

type Config struct {
	CelebThreshold int64         // followers at or above which an author is not fanned out
	FeedCap        int           // max entries kept in a pushed feed
	FeedTTL        time.Duration // idle feeds expire; refreshed on read and on push
	PostTTL        time.Duration // cached post hashes
	FollowingTTL   time.Duration // cached followee sets
	AuthorTTL      time.Duration // cached per-author recent posts
	AuthorPosts    int           // recent posts kept per author
	FollowingLimit int           // max followees loaded when rebuilding
}

func DefaultConfig() Config {
	return Config{
		CelebThreshold: 10000,
		FeedCap:        200,
		FeedTTL:        7 * 24 * time.Hour,
		PostTTL:        10 * time.Minute,
		FollowingTTL:   time.Hour,
		AuthorTTL:      6 * time.Hour,
		AuthorPosts:    50,
		FollowingLimit: 5000,
	}
}

type Service struct {
	rdb *redis.Client
	st  *store.Store
	cfg Config
}

func New(rdb *redis.Client, st *store.Store, cfg Config) *Service {
	return &Service{rdb: rdb, st: st, cfg: cfg}
}

func (s *Service) Config() Config { return s.cfg }

// PostView is what the API returns to clients.
type PostView struct {
	ID        int64     `json:"id"`
	AuthorID  int64     `json:"author_id"`
	Author    string    `json:"author"`
	Text      string    `json:"text"`
	Likes     int       `json:"likes"`
	Replies   int       `json:"replies"`
	CreatedAt time.Time `json:"created_at"`
}

func hit(cache string)  { metrics.CacheRequests.WithLabelValues(cache, "hit").Inc() }
func miss(cache string) { metrics.CacheRequests.WithLabelValues(cache, "miss").Inc() }

func idStr(id int64) string { return strconv.FormatInt(id, 10) }

// ------------------------------------------------------------------ celebrities

// IsCeleb reports whether an author's posts are merged at read time instead of being
// pushed to followers. Membership of the "celebs" set is sticky; otherwise the follower
// counter decides (computed from Postgres on first use, then maintained by the worker).
func (s *Service) IsCeleb(ctx context.Context, uid int64) (bool, error) {
	ok, err := s.rdb.SIsMember(ctx, rds.KeyCelebs, idStr(uid)).Result()
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	cnt, err := s.rdb.Get(ctx, rds.FollowerCount(uid)).Int64()
	if errors.Is(err, redis.Nil) {
		cnt, err = s.st.CountFollowers(ctx, uid)
		if err != nil {
			return false, err
		}
		s.rdb.SetNX(ctx, rds.FollowerCount(uid), cnt, 24*time.Hour)
	} else if err != nil {
		return false, err
	}
	if cnt >= s.cfg.CelebThreshold {
		s.rdb.SAdd(ctx, rds.KeyCelebs, idStr(uid))
		return true, nil
	}
	return false, nil
}

// ------------------------------------------------------------------ following cache

func (s *Service) loadFollowing(ctx context.Context, uid int64) error {
	ids, err := s.st.ListFollowing(ctx, uid, s.cfg.FollowingLimit)
	if err != nil {
		return err
	}
	// "0" is a sentinel so that a user who follows nobody still has a (non-empty) key.
	members := make([]interface{}, 0, len(ids)+1)
	members = append(members, "0")
	for _, id := range ids {
		members = append(members, idStr(id))
	}
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, rds.Following(uid), members...)
	pipe.Expire(ctx, rds.Following(uid), s.cfg.FollowingTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// CelebFollowees returns the celebrities this user follows (intersection of the cached
// followee set and the celebs set), loading the followee set from Postgres on a miss.
func (s *Service) CelebFollowees(ctx context.Context, uid int64) ([]int64, error) {
	key := rds.Following(uid)
	pipe := s.rdb.Pipeline()
	ex := pipe.Exists(ctx, key)
	inter := pipe.SInter(ctx, key, rds.KeyCelebs)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	if ex.Val() == 0 {
		miss("following")
		if err := s.loadFollowing(ctx, uid); err != nil {
			return nil, err
		}
		vals, err := s.rdb.SInter(ctx, key, rds.KeyCelebs).Result()
		if err != nil {
			return nil, err
		}
		return feed.ParseIDs(vals), nil
	}
	hit("following")
	return feed.ParseIDs(inter.Val()), nil
}

// ------------------------------------------------------------------ author cache

// EnsureAuthorPosts makes sure author_posts:{id} exists for each author, loading the
// newest posts from Postgres (scatter-gather over all shards) for the ones that do not.
func (s *Service) EnsureAuthorPosts(ctx context.Context, authors []int64) error {
	if len(authors) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(authors))
	for i, a := range authors {
		cmds[i] = pipe.Exists(ctx, rds.AuthorPosts(a))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	var missing []int64
	for i, a := range authors {
		if cmds[i].Val() == 0 {
			miss("author_posts")
			missing = append(missing, a)
		} else {
			hit("author_posts")
		}
	}
	if len(missing) == 0 {
		return nil
	}
	recent, err := s.st.RecentPerAuthor(ctx, missing, s.cfg.AuthorPosts)
	if err != nil {
		return err
	}
	pipe2 := s.rdb.Pipeline()
	for _, a := range missing {
		key := rds.AuthorPosts(a)
		zs := []redis.Z{{Score: 0, Member: "0"}} // sentinel: "loaded, possibly empty"
		for _, id := range recent[a] {
			zs = append(zs, redis.Z{Score: float64(id), Member: idStr(id)})
		}
		pipe2.ZAdd(ctx, key, zs...)
		pipe2.Expire(ctx, key, s.cfg.AuthorTTL)
	}
	_, err = pipe2.Exec(ctx)
	return err
}

// AddAuthorPost records a new post in the author's cache (only if it is loaded).
func (s *Service) AddAuthorPost(ctx context.Context, author, postID int64) error {
	return rds.ScriptZAddIfExists.Run(ctx, s.rdb, []string{rds.AuthorPosts(author)},
		postID, s.cfg.AuthorPosts, int(s.cfg.AuthorTTL.Seconds())).Err()
}

// ------------------------------------------------------------------ fan-out (worker)

// PushFeed adds a post to the feeds of the given users. Users without a live feed key
// (inactive) are skipped by the Lua script, so they cost nothing.
func (s *Service) PushFeed(ctx context.Context, users []int64, postID int64) error {
	if len(users) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	ttl := int(s.cfg.FeedTTL.Seconds())
	for _, u := range users {
		rds.ScriptZAddIfExists.Eval(ctx, pipe, []string{rds.Feed(u)}, postID, s.cfg.FeedCap, ttl)
	}
	_, err := pipe.Exec(ctx)
	if err == nil {
		metrics.FanoutRecipients.Add(float64(len(users)))
	}
	return err
}

// BackfillFollow copies a followee's latest posts into a new follower's feed so the
// feed is not empty after a follow. Celebrities are merged at read time instead.
func (s *Service) BackfillFollow(ctx context.Context, follower, followee int64) error {
	celeb, err := s.IsCeleb(ctx, followee)
	if err != nil || celeb {
		return err
	}
	exists, err := s.rdb.Exists(ctx, rds.Feed(follower)).Result()
	if err != nil || exists == 0 {
		return err // a cold feed is rebuilt lazily on the next read
	}
	if err := s.EnsureAuthorPosts(ctx, []int64{followee}); err != nil {
		return err
	}
	members, err := s.rdb.ZRevRange(ctx, rds.AuthorPosts(followee), 0, 19).Result()
	if err != nil {
		return err
	}
	for _, id := range feed.ParseIDs(members) {
		if id > 0 {
			if err := s.PushFeed(ctx, []int64{follower}, id); err != nil {
				return err
			}
		}
	}
	return nil
}

// ------------------------------------------------------------------ post cache

func postHash(p store.Post) map[string]interface{} {
	return map[string]interface{}{
		"aid": p.AuthorID, "an": p.AuthorName, "body": p.Body, "likes": p.Likes, "replies": p.Replies,
	}
}

func postFromHash(id int64, m map[string]string) store.Post {
	aid, _ := strconv.ParseInt(m["aid"], 10, 64)
	likes, _ := strconv.Atoi(m["likes"])
	replies, _ := strconv.Atoi(m["replies"])
	return store.Post{ID: id, AuthorID: aid, AuthorName: m["an"], Body: m["body"], Likes: likes, Replies: replies}
}

// CachePost stores a freshly written post so its first readers hit Redis, not Postgres.
func (s *Service) CachePost(ctx context.Context, p store.Post) error {
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, rds.Post(p.ID), postHash(p))
	pipe.Expire(ctx, rds.Post(p.ID), s.cfg.PostTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Hydrate turns post ids into views: cached post hash + unflushed like/reply deltas,
// falling back to Postgres (one query per owning shard) for cache misses.
func (s *Service) Hydrate(ctx context.Context, ids []int64) ([]PostView, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	posts := make([]*redis.MapStringStringCmd, len(ids))
	deltas := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		posts[i] = pipe.HGetAll(ctx, rds.Post(id))
		deltas[i] = pipe.HGetAll(ctx, rds.Counts(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	base := make(map[int64]store.Post, len(ids))
	var missing []int64
	for i, id := range ids {
		m := posts[i].Val()
		if len(m) == 0 {
			miss("post")
			missing = append(missing, id)
			continue
		}
		hit("post")
		base[id] = postFromHash(id, m)
	}
	if len(missing) > 0 {
		fetched, err := s.st.PostsByIDs(ctx, missing)
		if err != nil {
			return nil, err
		}
		pipe2 := s.rdb.Pipeline()
		for id, p := range fetched {
			base[id] = p
			pipe2.HSet(ctx, rds.Post(id), postHash(p))
			pipe2.Expire(ctx, rds.Post(id), s.cfg.PostTTL)
		}
		_, _ = pipe2.Exec(ctx) // best effort: the cache is an optimisation
	}
	out := make([]PostView, 0, len(ids))
	for i, id := range ids {
		p, ok := base[id]
		if !ok {
			continue
		}
		dl, _ := strconv.Atoi(deltas[i].Val()["likes"])
		dr, _ := strconv.Atoi(deltas[i].Val()["replies"])
		out = append(out, PostView{
			ID: id, AuthorID: p.AuthorID, Author: p.AuthorName, Text: p.Body,
			Likes: p.Likes + dl, Replies: p.Replies + dr, CreatedAt: idgen.Time(id),
		})
	}
	return out, nil
}

// ------------------------------------------------------------------ feed read (API)

// ReadFeed returns one page of the user's home feed, newest first, plus the cursor for
// the next page (0 when there is no more). The page is the merge of
//   - the user's pushed feed (fan-out on write), and
//   - author_posts:{celebrity} for every celebrity the user follows (merge on read).
//
// A missing feed key means a cold feed, which is rebuilt from Postgres.
func (s *Service) ReadFeed(ctx context.Context, uid, cursor int64, limit int) ([]PostView, int64, error) {
	celebs, err := s.CelebFollowees(ctx, uid)
	if err != nil {
		return nil, 0, err
	}
	own, celebLists, err := s.fetchLists(ctx, uid, celebs, cursor, limit)
	if err != nil {
		return nil, 0, err
	}
	if cursor == 0 {
		refetch := false
		if len(own) == 0 { // pushed feed absent (always holds at least the sentinel when present)
			miss("feed")
			if err := s.RebuildFeed(ctx, uid); err != nil {
				return nil, 0, err
			}
			refetch = true
		} else {
			hit("feed")
		}
		var cold []int64
		for i, l := range celebLists {
			if len(l) == 0 {
				cold = append(cold, celebs[i])
			}
		}
		if len(cold) > 0 {
			if err := s.EnsureAuthorPosts(ctx, cold); err != nil {
				return nil, 0, err
			}
			refetch = true
		}
		if refetch {
			if own, celebLists, err = s.fetchLists(ctx, uid, celebs, cursor, limit); err != nil {
				return nil, 0, err
			}
		}
	}
	lists := make([][]int64, 0, 1+len(celebLists))
	lists = append(lists, feed.ParseIDs(own))
	for _, l := range celebLists {
		lists = append(lists, feed.ParseIDs(l))
	}
	ids := feed.MergeDesc(limit, lists...)
	views, err := s.Hydrate(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	var next int64
	if len(ids) == limit {
		next = ids[len(ids)-1]
	}
	return views, next, nil
}

func (s *Service) fetchLists(ctx context.Context, uid int64, celebs []int64, cursor int64, limit int) ([]string, [][]string, error) {
	maxScore := "+inf"
	if cursor > 0 {
		maxScore = "(" + idStr(cursor) // exclusive: ids strictly older than the cursor
	}
	pipe := s.rdb.Pipeline()
	rng := func(key string) *redis.StringSliceCmd {
		return pipe.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: maxScore, Offset: 0, Count: int64(limit)})
	}
	ownCmd := rng(rds.Feed(uid))
	celebCmds := make([]*redis.StringSliceCmd, len(celebs))
	for i, c := range celebs {
		celebCmds[i] = rng(rds.AuthorPosts(c))
	}
	pipe.Expire(ctx, rds.Feed(uid), s.cfg.FeedTTL) // reading keeps an active user's feed alive
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, nil, err
	}
	celebLists := make([][]string, len(celebs))
	for i, c := range celebCmds {
		celebLists[i] = c.Val()
	}
	return ownCmd.Val(), celebLists, nil
}

// RebuildFeed constructs a cold user's feed from the newest posts of the (non-celebrity)
// accounts they follow, plus their own. Celebrities are never stored in a pushed feed.
func (s *Service) RebuildFeed(ctx context.Context, uid int64) error {
	metrics.FeedRebuilds.Inc()
	exists, err := s.rdb.Exists(ctx, rds.Following(uid)).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		if err := s.loadFollowing(ctx, uid); err != nil {
			return err
		}
	}
	members, err := s.rdb.SDiff(ctx, rds.Following(uid), rds.KeyCelebs).Result()
	if err != nil {
		return err
	}
	authors := []int64{uid}
	for _, id := range feed.ParseIDs(members) {
		if id > 0 && id != uid { // skip the "0" sentinel
			authors = append(authors, id)
		}
	}
	if err := s.EnsureAuthorPosts(ctx, authors); err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(authors))
	for i, a := range authors {
		cmds[i] = pipe.ZRevRange(ctx, rds.AuthorPosts(a), 0, int64(s.cfg.AuthorPosts-1))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	lists := make([][]int64, 0, len(cmds))
	for _, c := range cmds {
		lists = append(lists, feed.ParseIDs(c.Val()))
	}
	ids := feed.MergeDesc(s.cfg.FeedCap, lists...)
	zs := []redis.Z{{Score: 0, Member: "0"}} // sentinel so an empty feed is still "present"
	for _, id := range ids {
		zs = append(zs, redis.Z{Score: float64(id), Member: idStr(id)})
	}
	pipe2 := s.rdb.Pipeline()
	pipe2.ZAdd(ctx, rds.Feed(uid), zs...)
	pipe2.Expire(ctx, rds.Feed(uid), s.cfg.FeedTTL)
	_, err = pipe2.Exec(ctx)
	return err
}

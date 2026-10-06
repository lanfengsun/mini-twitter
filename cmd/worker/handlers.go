package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"minitwitter/internal/events"
	"minitwitter/internal/feedsvc"
	"minitwitter/internal/idgen"
	"minitwitter/internal/metrics"
	"minitwitter/internal/queue"
	"minitwitter/internal/rds"
	"minitwitter/internal/store"
)

const fanoutPage = 1000

type Worker struct {
	log *slog.Logger
	rdb *redis.Client
	st  *store.Store
	fs  *feedsvc.Service
}

// Handle dispatches one queue event. Every handler is idempotent because the stream
// delivers at least once: inserts use ON CONFLICT DO NOTHING, and counters are only
// bumped when a row was newly inserted.
func (w *Worker) Handle(ctx context.Context, typ string, data []byte) error {
	switch typ {
	case events.TypePost:
		var e events.Post
		if err := json.Unmarshal(data, &e); err != nil {
			return queue.Permanent(err)
		}
		return w.onPost(ctx, e)
	case events.TypeFollow:
		var e events.Follow
		if err := json.Unmarshal(data, &e); err != nil {
			return queue.Permanent(err)
		}
		return w.onFollow(ctx, e)
	case events.TypeLike:
		var e events.Like
		if err := json.Unmarshal(data, &e); err != nil {
			return queue.Permanent(err)
		}
		return w.onLike(ctx, e)
	case events.TypeReply:
		var e events.Reply
		if err := json.Unmarshal(data, &e); err != nil {
			return queue.Permanent(err)
		}
		return w.onReply(ctx, e)
	default:
		return queue.Permanent(fmt.Errorf("unknown event type %q", typ))
	}
}

// onPost persists the post, then fans it out.
//   - normal author: push the post id into the feed of every follower that has a live feed key
//   - celebrity:     skip fan-out; readers merge author_posts:{celebrity} at read time
func (w *Worker) onPost(ctx context.Context, e events.Post) error {
	p := store.Post{ID: e.ID, AuthorID: e.AuthorID, AuthorName: e.AuthorName, Body: e.Body}
	if _, err := w.st.InsertPost(ctx, p); err != nil {
		return err
	}
	_ = w.fs.CachePost(ctx, p) // best effort: first readers hit Redis instead of Postgres
	if err := w.fs.AddAuthorPost(ctx, e.AuthorID, e.ID); err != nil {
		return err
	}
	celeb, err := w.fs.IsCeleb(ctx, e.AuthorID)
	if err != nil {
		return err
	}
	if celeb {
		metrics.FanoutSkippedCelebrity.Inc()
		metrics.FeedFreshness.Observe(time.Since(idgen.Time(e.ID)).Seconds())
		return nil
	}
	if err := w.fs.PushFeed(ctx, []int64{e.AuthorID}, e.ID); err != nil { // author's own feed
		return err
	}
	var after int64
	for {
		followers, err := w.st.ListFollowersPage(ctx, e.AuthorID, after, fanoutPage)
		if err != nil {
			return err
		}
		if len(followers) == 0 {
			break
		}
		if err := w.fs.PushFeed(ctx, followers, e.ID); err != nil {
			return err
		}
		after = followers[len(followers)-1]
		if len(followers) < fanoutPage {
			break
		}
	}
	metrics.FeedFreshness.Observe(time.Since(idgen.Time(e.ID)).Seconds())
	return nil
}

// onFollow writes both directions of the edge (following sharded by follower, followers
// sharded by followee), maintains the follower counter, and backfills the new follower's feed.
func (w *Worker) onFollow(ctx context.Context, e events.Follow) error {
	if _, err := w.st.InsertFollowing(ctx, e.FollowerID, e.FolloweeID); err != nil {
		return err
	}
	inserted, err := w.st.InsertFollower(ctx, e.FolloweeID, e.FollowerID)
	if err != nil {
		return err
	}
	if inserted {
		v, err := rds.ScriptIncrIfExists.Run(ctx, w.rdb, []string{rds.FollowerCount(e.FolloweeID)}).Int64()
		if err != nil && err != redis.Nil {
			return err
		}
		if v >= w.fs.Config().CelebThreshold {
			w.rdb.SAdd(ctx, rds.KeyCelebs, strconv.FormatInt(e.FolloweeID, 10))
		}
	}
	// keep the cached followee set (if loaded) in step with the database
	if n, _ := w.rdb.Exists(ctx, rds.Following(e.FollowerID)).Result(); n == 1 {
		w.rdb.SAdd(ctx, rds.Following(e.FollowerID), strconv.FormatInt(e.FolloweeID, 10))
	}
	return w.fs.BackfillFollow(ctx, e.FollowerID, e.FolloweeID)
}

// Likes and replies update a Redis delta hash instead of the (potentially very hot)
// Postgres row; flushLoop folds the deltas into the row every second.
func (w *Worker) bump(ctx context.Context, postID int64, field string) error {
	pipe := w.rdb.Pipeline()
	pipe.HIncrBy(ctx, rds.Counts(postID), field, 1)
	pipe.SAdd(ctx, rds.KeyDirty, strconv.FormatInt(postID, 10))
	_, err := pipe.Exec(ctx)
	return err
}

func (w *Worker) onLike(ctx context.Context, e events.Like) error {
	inserted, err := w.st.InsertLike(ctx, e.PostID, e.UserID)
	if err != nil || !inserted {
		return err
	}
	return w.bump(ctx, e.PostID, "likes")
}

func (w *Worker) onReply(ctx context.Context, e events.Reply) error {
	inserted, err := w.st.InsertReply(ctx, store.Reply{
		ID: e.ID, PostID: e.PostID, AuthorID: e.AuthorID, AuthorName: e.AuthorName, Body: e.Body,
	})
	if err != nil || !inserted {
		return err
	}
	return w.bump(ctx, e.PostID, "replies")
}

// flushLoop folds unflushed counter deltas into Postgres. A crash between taking a delta
// and writing it loses that delta; counters are approximate by design (a reconciliation
// job that recounts likes/replies would repair drift).
func (w *Worker) flushLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.flushOnce(ctx)
		}
	}
}

func (w *Worker) flushOnce(ctx context.Context) {
	for ctx.Err() == nil {
		ids, err := w.rdb.SPopN(ctx, rds.KeyDirty, 500).Result()
		if err != nil || len(ids) == 0 {
			return
		}
		for _, s := range ids {
			id, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				continue
			}
			res, err := rds.ScriptTakeCounts.Run(ctx, w.rdb, []string{rds.Counts(id)}).Slice()
			if err != nil || len(res) != 2 {
				continue
			}
			likes, _ := strconv.Atoi(fmt.Sprint(res[0]))
			replies, _ := strconv.Atoi(fmt.Sprint(res[1]))
			if likes == 0 && replies == 0 {
				continue
			}
			if err := w.st.AddCounts(ctx, id, likes, replies); err != nil {
				w.log.Warn("counter flush failed, restoring deltas", "post_id", id, "err", err.Error())
				w.restore(ctx, id, likes, replies)
				continue
			}
			w.rdb.Del(ctx, rds.Post(id)) // drop the cached copy: its base counts are now stale
		}
	}
}

func (w *Worker) restore(ctx context.Context, id int64, likes, replies int) {
	pipe := w.rdb.Pipeline()
	if likes != 0 {
		pipe.HIncrBy(ctx, rds.Counts(id), "likes", int64(likes))
	}
	if replies != 0 {
		pipe.HIncrBy(ctx, rds.Counts(id), "replies", int64(replies))
	}
	pipe.SAdd(ctx, rds.KeyDirty, strconv.FormatInt(id, 10))
	_, _ = pipe.Exec(ctx)
}

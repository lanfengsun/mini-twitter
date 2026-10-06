// Package store is the sharded Postgres access layer. Routing lives in internal/shard;
// every method here picks the owning shard (or scatter-gathers across all of them).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lib/pq"

	"minitwitter/internal/idgen"
	"minitwitter/internal/shard"
)

var (
	ErrNotFound   = errors.New("not found")
	ErrUserExists = errors.New("user exists")
)

type User struct {
	ID           int64
	Username     string
	PasswordHash string
}

type Post struct {
	ID         int64
	AuthorID   int64
	AuthorName string
	Body       string
	Likes      int
	Replies    int
}

type Reply struct {
	ID         int64
	PostID     int64
	AuthorID   int64
	AuthorName string
	Body       string
}

type Store struct {
	dbs []*sql.DB
}

// New opens one pool per shard. maxConns is the per-shard pool size for this process.
func New(dsns []string, maxConns int) (*Store, error) {
	if len(dsns) == 0 {
		return nil, errors.New("no shard DSNs configured (SHARD_DSNS)")
	}
	s := &Store{}
	for _, dsn := range dsns {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(maxConns)
		db.SetMaxIdleConns(maxConns)
		db.SetConnMaxLifetime(30 * time.Minute)
		s.dbs = append(s.dbs, db)
	}
	return s, nil
}

func (s *Store) N() int         { return len(s.dbs) }
func (s *Store) DBs() []*sql.DB { return s.dbs }
func (s *Store) Close() {
	for _, db := range s.dbs {
		_ = db.Close()
	}
}

// Ping checks every shard.
func (s *Store) Ping(ctx context.Context) error {
	return s.each(func(i int, db *sql.DB) error {
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
		return nil
	})
}

// each runs fn against every shard in parallel and returns the first error.
func (s *Store) each(fn func(i int, db *sql.DB) error) error {
	errs := make([]error, len(s.dbs))
	var wg sync.WaitGroup
	for i, db := range s.dbs {
		wg.Add(1)
		go func(i int, db *sql.DB) {
			defer wg.Done()
			errs[i] = fn(i, db)
		}(i, db)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) byID(id int64) *sql.DB      { return s.dbs[shard.ForID(id, len(s.dbs))] }
func (s *Store) byName(name string) *sql.DB { return s.dbs[shard.ForString(name, len(s.dbs))] }

func isUnique(err error) bool {
	var pe *pq.Error
	return errors.As(err, &pe) && pe.Code == "23505"
}

// ---------------------------------------------------------------- users

func (s *Store) CreateUser(ctx context.Context, u User) error {
	_, err := s.byName(u.Username).ExecContext(ctx,
		`INSERT INTO users (id, username, password_hash, created_at) VALUES ($1, $2, $3, $4)`,
		u.ID, u.Username, u.PasswordHash, idgen.Time(u.ID))
	if isUnique(err) {
		return ErrUserExists
	}
	return err
}

func (s *Store) GetUserByName(ctx context.Context, username string) (User, error) {
	var u User
	err := s.byName(username).QueryRowContext(ctx,
		`SELECT id, username, password_hash FROM users WHERE username = $1`, username).
		Scan(&u.ID, &u.Username, &u.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// ---------------------------------------------------------------- posts

// InsertPost is idempotent: it reports whether the row was newly inserted.
func (s *Store) InsertPost(ctx context.Context, p Post) (bool, error) {
	res, err := s.byID(p.ID).ExecContext(ctx,
		`INSERT INTO posts (id, author_id, author_name, body, created_at)
		 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
		p.ID, p.AuthorID, p.AuthorName, p.Body, idgen.Time(p.ID))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// PostsByIDs fetches posts, querying only the shards that own the ids, in parallel.
func (s *Store) PostsByIDs(ctx context.Context, ids []int64) (map[int64]Post, error) {
	groups := make([][]int64, len(s.dbs))
	for _, id := range ids {
		i := shard.ForID(id, len(s.dbs))
		groups[i] = append(groups[i], id)
	}
	out := make(map[int64]Post, len(ids))
	var mu sync.Mutex
	err := s.each(func(i int, db *sql.DB) error {
		if len(groups[i]) == 0 {
			return nil
		}
		rows, err := db.QueryContext(ctx,
			`SELECT id, author_id, author_name, body, like_count, reply_count
			   FROM posts WHERE id = ANY($1)`, pq.Array(groups[i]))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var p Post
			if err := rows.Scan(&p.ID, &p.AuthorID, &p.AuthorName, &p.Body, &p.Likes, &p.Replies); err != nil {
				return err
			}
			mu.Lock()
			out[p.ID] = p
			mu.Unlock()
		}
		return rows.Err()
	})
	return out, err
}

// RecentPerAuthor returns the newest k post ids of each author. Posts are sharded by
// post id, so an author's posts are spread over every shard: this is the scatter-gather
// read. It only runs on cache misses (cold feed rebuild, cold author cache).
func (s *Store) RecentPerAuthor(ctx context.Context, authors []int64, k int) (map[int64][]int64, error) {
	out := make(map[int64][]int64, len(authors))
	if len(authors) == 0 {
		return out, nil
	}
	var mu sync.Mutex
	err := s.each(func(i int, db *sql.DB) error {
		rows, err := db.QueryContext(ctx,
			`SELECT a.author_id, p.id
			   FROM unnest($1::bigint[]) AS a(author_id)
			   CROSS JOIN LATERAL (
			       SELECT id FROM posts WHERE author_id = a.author_id ORDER BY id DESC LIMIT $2
			   ) p`, pq.Array(authors), k)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var author, id int64
			if err := rows.Scan(&author, &id); err != nil {
				return err
			}
			mu.Lock()
			out[author] = append(out[author], id)
			mu.Unlock()
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	for a, ids := range out {
		sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
		if len(ids) > k {
			ids = ids[:k]
		}
		out[a] = ids
	}
	return out, nil
}

// AddCounts applies flushed like/reply deltas to a post row.
func (s *Store) AddCounts(ctx context.Context, postID int64, likes, replies int) error {
	_, err := s.byID(postID).ExecContext(ctx,
		`UPDATE posts SET like_count = like_count + $2, reply_count = reply_count + $3 WHERE id = $1`,
		postID, likes, replies)
	return err
}

// ---------------------------------------------------------------- follows

// InsertFollowing writes the "who do I follow" side (shard by follower).
func (s *Store) InsertFollowing(ctx context.Context, follower, followee int64) (bool, error) {
	res, err := s.byID(follower).ExecContext(ctx,
		`INSERT INTO following (follower_id, followee_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		follower, followee)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// InsertFollower writes the "who follows me" side (shard by followee).
func (s *Store) InsertFollower(ctx context.Context, followee, follower int64) (bool, error) {
	res, err := s.byID(followee).ExecContext(ctx,
		`INSERT INTO followers (followee_id, follower_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		followee, follower)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ListFollowing(ctx context.Context, uid int64, limit int) ([]int64, error) {
	return s.ids(ctx, s.byID(uid),
		`SELECT followee_id FROM following WHERE follower_id = $1 LIMIT $2`, uid, limit)
}

// ListFollowersPage pages through a user's followers in id order (keyset pagination).
func (s *Store) ListFollowersPage(ctx context.Context, uid, after int64, limit int) ([]int64, error) {
	return s.ids(ctx, s.byID(uid),
		`SELECT follower_id FROM followers WHERE followee_id = $1 AND follower_id > $2
		  ORDER BY follower_id LIMIT $3`, uid, after, limit)
}

func (s *Store) CountFollowers(ctx context.Context, uid int64) (int64, error) {
	var n int64
	err := s.byID(uid).QueryRowContext(ctx,
		`SELECT count(*) FROM followers WHERE followee_id = $1`, uid).Scan(&n)
	return n, err
}

func (s *Store) ids(ctx context.Context, db *sql.DB, q string, args ...any) ([]int64, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- likes & replies

// InsertLike reports whether the like is new (so the counter is bumped exactly once).
func (s *Store) InsertLike(ctx context.Context, postID, userID int64) (bool, error) {
	res, err := s.byID(postID).ExecContext(ctx,
		`INSERT INTO likes (post_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, postID, userID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) InsertReply(ctx context.Context, r Reply) (bool, error) {
	res, err := s.byID(r.PostID).ExecContext(ctx,
		`INSERT INTO replies (id, post_id, author_id, author_name, body, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`,
		r.ID, r.PostID, r.AuthorID, r.AuthorName, r.Body, idgen.Time(r.ID))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ListReplies returns the newest replies of a post (they live on the post's shard).
func (s *Store) ListReplies(ctx context.Context, postID int64, limit int) ([]Reply, error) {
	rows, err := s.byID(postID).QueryContext(ctx,
		`SELECT id, post_id, author_id, author_name, body FROM replies
		  WHERE post_id = $1 ORDER BY id DESC LIMIT $2`, postID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reply
	for rows.Next() {
		var r Reply
		if err := rows.Scan(&r.ID, &r.PostID, &r.AuthorID, &r.AuthorName, &r.Body); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

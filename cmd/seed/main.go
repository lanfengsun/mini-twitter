// Command seed bulk-loads a realistic dataset straight into the Postgres shards with
// COPY (inserting millions of rows through the API would take hours), and primes Redis:
// the celebrity set, follower counters and pre-made sessions for the load test.
//
// Every seeded user has the password "password". Sessions for users u1..uN
// (LOAD_SESSIONS) exist with the token "loadtoken-<i>" (i = 0..N-1 maps to user u<i+1>),
// plus "loadtoken-celeb1" for the first celebrity account.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"context"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"

	"minitwitter/internal/config"
	"minitwitter/internal/idgen"
	"minitwitter/internal/rds"
	"minitwitter/internal/shard"
)

const seedNode = idgen.MaxNode // node 15 is reserved for the seeder

// copier streams rows into one table of one shard using COPY inside a single transaction.
type copier struct {
	ch   chan []interface{}
	done chan error
}

func newCopier(db *sql.DB, table string, cols ...string) *copier {
	c := &copier{ch: make(chan []interface{}, 8192), done: make(chan error, 1)}
	go func() {
		var firstErr error
		tx, err := db.Begin()
		var stmt *sql.Stmt
		if err == nil {
			stmt, err = tx.Prepare(pq.CopyIn(table, cols...))
		}
		if err != nil {
			firstErr = err
		}
		for row := range c.ch {
			if firstErr != nil {
				continue // keep draining so the producer never blocks
			}
			if _, err := stmt.Exec(row...); err != nil {
				firstErr = err
			}
		}
		if firstErr == nil {
			if _, err := stmt.Exec(); err != nil {
				firstErr = err
			}
			if err := stmt.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			if firstErr == nil {
				firstErr = tx.Commit()
			}
		}
		if firstErr != nil && tx != nil {
			_ = tx.Rollback()
		}
		c.done <- firstErr
	}()
	return c
}

func (c *copier) add(row ...interface{}) { c.ch <- row }

func (c *copier) finish() error {
	close(c.ch)
	return <-c.done
}

// table set: one copier per table per shard
type loaders struct {
	users, posts, following, followers []*copier
}

func newLoaders(dbs []*sql.DB) *loaders {
	l := &loaders{}
	for _, db := range dbs {
		l.users = append(l.users, newCopier(db, "users", "id", "username", "password_hash", "created_at"))
		l.posts = append(l.posts, newCopier(db, "posts", "id", "author_id", "author_name", "body", "like_count", "reply_count", "created_at"))
		l.following = append(l.following, newCopier(db, "following", "follower_id", "followee_id"))
		l.followers = append(l.followers, newCopier(db, "followers", "followee_id", "follower_id"))
	}
	return l
}

func (l *loaders) finish() error {
	var first error
	for _, group := range [][]*copier{l.users, l.posts, l.following, l.followers} {
		for _, c := range group {
			if err := c.finish(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

var words = strings.Fields(`the quick brown fox jumps over lazy dog coffee deploy shipped bug fixed
	finally monday friday weekend launch queue cache shard latency graph metrics coffee tea build green
	red alert pager sleep ship it learning go rust python kubernetes cluster incident review postmortem`)

func body(r *rand.Rand, i int) string {
	n := 6 + r.Intn(14)
	parts := make([]string, 0, n+1)
	for j := 0; j < n; j++ {
		parts = append(parts, words[r.Intn(len(words))])
	}
	return strings.Join(parts, " ") + " #" + strconv.Itoa(i%1000)
}

func main() {
	var (
		users         = config.Int("USERS", 500000)
		posts         = config.Int("POSTS", 2000000)
		avgFollows    = config.Int("AVG_FOLLOWS", 6)
		celebs        = config.Int("CELEBS", 1)
		celebFollows  = config.Int("CELEB_FOLLOWERS", 300000)
		sessions      = config.Int("LOAD_SESSIONS", 10000)
		celebThresh   = int64(config.Int("CELEB_THRESHOLD", 10000))
		bcryptCost    = config.Int("BCRYPT_COST", 10)
		postWindow    = 7 * 24 * time.Hour
		userWindowAgo = 30 * 24 * time.Hour
	)
	if celebFollows > users {
		celebFollows = users
	}
	start := time.Now()

	dbs := make([]*sql.DB, 0)
	for _, dsn := range config.List("SHARD_DSNS") {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			log.Fatal(err)
		}
		db.SetMaxOpenConns(16)
		for i := 0; ; i++ {
			if err = db.Ping(); err == nil {
				break
			}
			if i > 60 {
				log.Fatalf("postgres not ready: %v", err)
			}
			time.Sleep(time.Second)
		}
		dbs = append(dbs, db)
	}
	n := len(dbs)
	rdb := redis.NewClient(&redis.Options{Addr: config.Str("REDIS_ADDR", "redis:6379")})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	log.Printf("resetting data on %d shards and Redis", n)
	for _, db := range dbs {
		if _, err := db.Exec(`TRUNCATE users, posts, following, followers, likes, replies`); err != nil {
			log.Fatal(err)
		}
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcryptCost)
	if err != nil {
		log.Fatal(err)
	}

	now := time.Now()
	userBaseMs := now.Add(-userWindowAgo).UnixMilli()
	postBaseMs := now.Add(-postWindow).UnixMilli()
	stepMs := int64(postWindow/time.Millisecond) / int64(max(posts, 1))
	if stepMs < 1 {
		stepMs = 1
	}
	// user i (1-based) -> deterministic id, so every phase can compute it without a lookup
	userID := func(i int) int64 { return idgen.Make(userBaseMs+int64(i), seedNode, 0) }
	userName := func(i int) string { return "u" + strconv.Itoa(i) }
	celebName := func(j int) string { return "celeb" + strconv.Itoa(j) }
	celebIdx := func(j int) int { return users + j } // celeb j (1-based) sits after the normal users

	r := rand.New(rand.NewSource(42))
	L := newLoaders(dbs)

	// ---- users --------------------------------------------------------------
	log.Printf("users: %d (+%d celebrities)", users, celebs)
	addUser := func(i int, name string) {
		id := userID(i)
		L.users[shard.ForString(name, n)].add(id, name, string(hash), idgen.Time(id))
	}
	for i := 1; i <= users; i++ {
		addUser(i, userName(i))
	}
	for j := 1; j <= celebs; j++ {
		addUser(celebIdx(j), celebName(j))
	}

	// ---- posts: authors follow a Zipf curve (a few accounts post a lot) -----------
	log.Printf("posts: %d", posts)
	zipfAuthor := rand.NewZipf(r, 1.07, 1, uint64(users-1))
	addPost := func(id int64, authorIdx int, name string, i int) {
		L.posts[shard.ForID(id, n)].add(id, userID(authorIdx), name, body(r, i), 0, 0, idgen.Time(id))
	}
	for i := 0; i < posts; i++ {
		id := idgen.Make(postBaseMs+int64(i)*stepMs, seedNode, 0)
		a := 1 + int(zipfAuthor.Uint64())
		addPost(id, a, userName(a), i)
		if (i+1)%500000 == 0 {
			log.Printf("  posts %d/%d", i+1, posts)
		}
	}
	for j := 1; j <= celebs; j++ { // each celebrity has 20 recent posts
		for k := 0; k < 20; k++ {
			id := idgen.Make(now.Add(-time.Duration(k+1)*30*time.Minute).UnixMilli(), seedNode, j)
			addPost(id, celebIdx(j), celebName(j), k)
		}
	}

	// ---- follow edges: followees follow a Zipf curve, so some accounts become popular ----
	log.Printf("follows: ~%d per user, plus %d followers for each of %d celebrities", avgFollows, celebFollows, celebs)
	zipfFollowee := rand.NewZipf(r, 1.07, 1, uint64(users-1))
	followerCount := make([]int32, users+1)
	addEdge := func(follower, followee int) {
		L.following[shard.ForID(userID(follower), n)].add(userID(follower), userID(followee))
		L.followers[shard.ForID(userID(followee), n)].add(userID(followee), userID(follower))
	}
	var edges int64
	picked := make(map[int]struct{}, 16)
	for u := 1; u <= users; u++ {
		k := 1 + r.Intn(2*avgFollows-1) // mean = avgFollows
		clear(picked)
		for tries := 0; len(picked) < k && tries < k*8; tries++ {
			f := 1 + int(zipfFollowee.Uint64())
			if f == u {
				continue
			}
			if _, dup := picked[f]; dup {
				continue
			}
			picked[f] = struct{}{}
			addEdge(u, f)
			followerCount[f]++
			edges++
		}
		if u%100000 == 0 {
			log.Printf("  follows for %d/%d users", u, users)
		}
	}
	for j := 1; j <= celebs; j++ {
		for u := 1; u <= celebFollows; u++ {
			addEdge(u, celebIdx(j))
			edges++
		}
	}

	log.Printf("flushing COPY streams (%d follow edges)", edges)
	if err := L.finish(); err != nil {
		log.Fatalf("copy failed: %v", err)
	}
	for _, db := range dbs {
		if _, err := db.Exec(`ANALYZE`); err != nil {
			log.Printf("analyze: %v", err)
		}
	}

	// ---- Redis: celebrity set, follower counters, load-test sessions -------------------
	pipe := rdb.Pipeline()
	celebCount := 0
	setCeleb := func(idx int, followers int64) {
		pipe.SAdd(ctx, rds.KeyCelebs, strconv.FormatInt(userID(idx), 10))
		pipe.Set(ctx, rds.FollowerCount(userID(idx)), followers, 0)
		celebCount++
	}
	for j := 1; j <= celebs; j++ {
		setCeleb(celebIdx(j), int64(celebFollows))
	}
	for u := 1; u <= users; u++ { // organically popular accounts count as celebrities too
		if int64(followerCount[u]) >= celebThresh {
			setCeleb(u, int64(followerCount[u]))
		}
	}
	for i := 0; i < sessions && i < users; i++ {
		pipe.Set(ctx, rds.Session("loadtoken-"+strconv.Itoa(i)),
			strconv.FormatInt(userID(i+1), 10)+"|"+userName(i+1), 30*24*time.Hour)
	}
	if celebs > 0 {
		pipe.Set(ctx, rds.Session("loadtoken-celeb1"),
			strconv.FormatInt(userID(celebIdx(1)), 10)+"|"+celebName(1), 30*24*time.Hour)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatalf("redis priming: %v", err)
	}

	fmt.Printf("\nseed complete in %s\n", time.Since(start).Round(time.Second))
	fmt.Printf("  users %d (+%d celebrities)  posts %d  follow edges %d (stored twice)\n", users, celebs, posts, edges)
	fmt.Printf("  accounts treated as celebrities (>= %d followers): %d\n", celebThresh, celebCount)
	fmt.Printf("  login: any of u1..u%d / celeb1.. with password \"password\"\n", users)
	fmt.Printf("  load-test tokens: loadtoken-0..loadtoken-%d, loadtoken-celeb1\n", sessions-1)
}

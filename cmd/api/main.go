// Command api is the stateless HTTP API. It is horizontally scalable behind nginx:
// all state lives in Redis (sessions, caches, queue) and the Postgres shards.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"minitwitter/internal/config"
	"minitwitter/internal/feedsvc"
	"minitwitter/internal/idgen"
	"minitwitter/internal/logx"
	"minitwitter/internal/queue"
	"minitwitter/internal/rds"
	"minitwitter/internal/store"
)

// App holds everything the handlers share.
type App struct {
	log        *slog.Logger
	rdb        *redis.Client
	st         *store.Store
	q          *queue.Queue
	fs         *feedsvc.Service
	ids        *idgen.Gen
	depth      atomic.Int64 // queue depth, sampled in the background
	queueMax   int64
	bcryptCost int
	reqTimeout time.Duration
	logSample  float64
	sessionTTL time.Duration
}

func main() {
	log := logx.New("api", config.Str("LOG_LEVEL", "info"))
	slog.SetDefault(log)

	rdb := redis.NewClient(&redis.Options{
		Addr:         config.Str("REDIS_ADDR", "redis:6379"),
		PoolSize:     config.Int("REDIS_POOL_SIZE", 200),
		MinIdleConns: 20,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	st, err := store.New(config.List("SHARD_DSNS"), config.Int("SHARD_MAX_CONNS", 20))
	if err != nil {
		log.Error("store init failed", "err", err.Error())
		os.Exit(1)
	}
	for i, db := range st.DBs() {
		prometheus.MustRegister(collectors.NewDBStatsCollector(db, fmt.Sprintf("shard%d", i)))
	}

	// Wait for dependencies (compose also gates on healthchecks; this is belt and braces).
	waitCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		err := rdb.Ping(waitCtx).Err()
		if err == nil {
			err = st.Ping(waitCtx)
		}
		if err == nil {
			break
		}
		if waitCtx.Err() != nil {
			log.Error("dependencies not ready", "err", err.Error())
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}

	// Every API instance gets its own id-generator node (0..14; 15 is the seeder's).
	n, err := rdb.Incr(waitCtx, rds.KeyNodeSeq).Result()
	if err != nil {
		log.Error("node id allocation failed", "err", err.Error())
		os.Exit(1)
	}
	node := int(n % int64(idgen.MaxNode))

	cfg := feedsvc.DefaultConfig()
	cfg.CelebThreshold = config.Int64("CELEB_THRESHOLD", cfg.CelebThreshold)
	cfg.FeedCap = config.Int("FEED_CAP", cfg.FeedCap)

	a := &App{
		log:        log,
		rdb:        rdb,
		st:         st,
		q:          queue.New(rdb),
		fs:         feedsvc.New(rdb, st, cfg),
		ids:        idgen.New(node),
		queueMax:   config.Int64("QUEUE_MAX_DEPTH", 100000),
		bcryptCost: config.Int("BCRYPT_COST", 10),
		reqTimeout: config.Dur("REQUEST_TIMEOUT", 3*time.Second),
		logSample:  config.Float("LOG_SAMPLE_RATE", 0.01),
		sessionTTL: 24 * time.Hour,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go a.depthLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/signup", a.signup)
	mux.HandleFunc("POST /api/login", a.login)
	mux.HandleFunc("POST /api/logout", a.auth(a.logout))
	mux.HandleFunc("GET /api/me", a.auth(a.me))
	mux.HandleFunc("POST /api/posts", a.auth(a.createPost))
	mux.HandleFunc("GET /api/feed", a.auth(a.getFeed))
	mux.HandleFunc("POST /api/follow/{username}", a.auth(a.follow))
	mux.HandleFunc("POST /api/posts/{id}/like", a.auth(a.like))
	mux.HandleFunc("POST /api/posts/{id}/reply", a.auth(a.reply))
	mux.HandleFunc("GET /api/posts/{id}/replies", a.auth(a.replies))
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           a.middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("api listening", "addr", srv.Addr, "node", node, "queue_max_depth", a.queueMax)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err.Error())
		os.Exit(1)
	}
}

// depthLoop samples the stream length so write requests can be shed (429) without an
// extra Redis round trip per request.
func (a *App) depthLoop(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c, cancel := context.WithTimeout(ctx, time.Second)
			if n, err := a.q.Depth(c); err == nil {
				a.depth.Store(n)
			}
			cancel()
		}
	}
}

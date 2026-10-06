// Command worker drains the Redis Stream: it persists posts, follows, likes and replies
// to the Postgres shards, fans posts out to follower feeds, and flushes like/reply
// counters to Postgres in the background.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"minitwitter/internal/config"
	"minitwitter/internal/feedsvc"
	"minitwitter/internal/logx"
	"minitwitter/internal/queue"
	"minitwitter/internal/store"
)

func main() {
	log := logx.New("worker", config.Str("LOG_LEVEL", "info"))
	slog.SetDefault(log)

	rdb := redis.NewClient(&redis.Options{
		Addr:         config.Str("REDIS_ADDR", "redis:6379"),
		PoolSize:     config.Int("REDIS_POOL_SIZE", 100),
		MinIdleConns: 10,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	st, err := store.New(config.List("SHARD_DSNS"), config.Int("SHARD_MAX_CONNS", 30))
	if err != nil {
		log.Error("store init failed", "err", err.Error())
		os.Exit(1)
	}
	for i, db := range st.DBs() {
		prometheus.MustRegister(collectors.NewDBStatsCollector(db, fmt.Sprintf("shard%d", i)))
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	cancel()

	cfg := feedsvc.DefaultConfig()
	cfg.CelebThreshold = config.Int64("CELEB_THRESHOLD", cfg.CelebThreshold)
	cfg.FeedCap = config.Int("FEED_CAP", cfg.FeedCap)
	w := &Worker{log: log, rdb: rdb, st: st, fs: feedsvc.New(rdb, st, cfg)}

	host, _ := os.Hostname()
	cons := queue.NewConsumer(rdb, host, w.Handle, log)
	cons.Batch = config.Int("WORKER_BATCH", 200)
	cons.Concurrency = config.Int("WORKER_CONCURRENCY", 32)
	cons.MaxDeliveries = config.Int64("MAX_DELIVERIES", 5)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, r *http.Request) { rw.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	go cons.StatsLoop(ctx)
	go w.flushLoop(ctx)

	log.Info("worker started", "consumer", host, "concurrency", cons.Concurrency, "celeb_threshold", cfg.CelebThreshold)
	if err := cons.Run(ctx); err != nil {
		log.Error("consumer stopped", "err", err.Error())
		os.Exit(1)
	}
	sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	_ = srv.Shutdown(sctx)
}

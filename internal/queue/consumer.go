package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"minitwitter/internal/metrics"
	"minitwitter/internal/rds"
)

// Handler processes one event. It must be idempotent: delivery is at-least-once.
type Handler func(ctx context.Context, typ string, data []byte) error

type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks an error as not worth retrying (for example a malformed payload);
// the event goes straight to the dead-letter stream.
func Permanent(err error) error { return permanent{err: err} }

type Consumer struct {
	rdb           *redis.Client
	name          string
	handler       Handler
	log           *slog.Logger
	Batch         int           // entries fetched per XREADGROUP
	Concurrency   int           // events handled in parallel
	MinIdle       time.Duration // a pending entry idle this long is reclaimed
	MaxDeliveries int64         // after this many deliveries an entry goes to the DLQ
}

func NewConsumer(rdb *redis.Client, name string, h Handler, log *slog.Logger) *Consumer {
	return &Consumer{
		rdb: rdb, name: name, handler: h, log: log,
		Batch: 200, Concurrency: 32, MinIdle: 30 * time.Second, MaxDeliveries: 5,
	}
}

// ensureGroup creates the consumer group (and the stream, if absent). Safe to call repeatedly.
func (c *Consumer) ensureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, rds.StreamEvents, rds.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Run blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}
	go c.reclaimLoop(ctx)
	for ctx.Err() == nil {
		streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    rds.Group,
			Consumer: c.name,
			Streams:  []string{rds.StreamEvents, ">"},
			Count:    int64(c.Batch),
			Block:    time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			if strings.Contains(err.Error(), "NOGROUP") {
				// The stream or group vanished (Redis flushed, or restarted without its data).
				// Recreate it from the start so entries added since are not skipped.
				c.log.Warn("consumer group missing, recreating it")
				if gerr := c.ensureGroup(ctx); gerr != nil {
					c.log.Error("could not recreate consumer group", "err", gerr.Error())
					time.Sleep(time.Second)
				}
				continue
			}
			c.log.Error("xreadgroup failed", "err", err.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, s := range streams {
			c.processBatch(ctx, s.Messages)
		}
	}
	return nil
}

// processBatch handles messages with bounded parallelism, then acks the finished ones.
// Failed messages stay pending and are retried by the reclaim loop.
func (c *Consumer) processBatch(ctx context.Context, msgs []redis.XMessage) {
	sem := make(chan struct{}, c.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var done []string
	for _, m := range msgs {
		sem <- struct{}{}
		wg.Add(1)
		go func(m redis.XMessage) {
			defer wg.Done()
			defer func() { <-sem }()
			if c.handle(ctx, m) {
				mu.Lock()
				done = append(done, m.ID)
				mu.Unlock()
			}
		}(m)
	}
	wg.Wait()
	c.ack(done...)
}

// handle returns true when the message is finished (handled, or moved to the DLQ).
func (c *Consumer) handle(ctx context.Context, m redis.XMessage) bool {
	typ, _ := m.Values["t"].(string)
	data, _ := m.Values["d"].(string)
	start := time.Now()
	err := c.safeCall(ctx, typ, []byte(data))
	metrics.EventDuration.WithLabelValues(typ).Observe(time.Since(start).Seconds())
	if err == nil {
		metrics.EventsProcessed.WithLabelValues(typ, "ok").Inc()
		return true
	}
	var p permanent
	if errors.As(err, &p) {
		c.toDLQ(m, err.Error())
		return true
	}
	metrics.EventsProcessed.WithLabelValues(typ, "error").Inc()
	c.log.Warn("event failed, will retry", "type", typ, "id", m.ID, "err", err.Error())
	return false
}

func (c *Consumer) safeCall(ctx context.Context, typ string, data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return c.handler(ctx, typ, data)
}

func (c *Consumer) toDLQ(m redis.XMessage, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	typ, _ := m.Values["t"].(string)
	data, _ := m.Values["d"].(string)
	err := c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: rds.StreamDLQ,
		MaxLen: 10000,
		Approx: true,
		Values: map[string]interface{}{"t": typ, "d": data, "err": reason, "orig_id": m.ID},
	}).Err()
	if err != nil {
		c.log.Error("dlq write failed", "err", err.Error())
		return
	}
	metrics.EventsProcessed.WithLabelValues(typ, "dlq").Inc()
	c.log.Error("event moved to dead-letter stream", "type", typ, "id", m.ID, "reason", reason)
}

func (c *Consumer) ack(ids ...string) {
	if len(ids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pipe := c.rdb.Pipeline()
	pipe.XAck(ctx, rds.StreamEvents, rds.Group, ids...)
	pipe.XDel(ctx, rds.StreamEvents, ids...)
	if _, err := pipe.Exec(ctx); err != nil {
		c.log.Error("ack failed", "err", err.Error())
	}
}

// reclaimLoop takes over entries that were delivered but not acknowledged for MinIdle
// (a handler failed, or a worker died) and retries them; entries delivered more than
// MaxDeliveries times are moved to the dead-letter stream.
func (c *Consumer) reclaimLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		start := "0-0"
		for i := 0; i < 20; i++ {
			msgs, next, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   rds.StreamEvents,
				Group:    rds.Group,
				Consumer: c.name,
				MinIdle:  c.MinIdle,
				Start:    start,
				Count:    100,
			}).Result()
			if err != nil {
				if ctx.Err() == nil {
					c.log.Error("xautoclaim failed", "err", err.Error())
				}
				break
			}
			var retry []redis.XMessage
			var dead []string
			for _, m := range msgs {
				pend, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
					Stream: rds.StreamEvents, Group: rds.Group, Start: m.ID, End: m.ID, Count: 1,
				}).Result()
				if err == nil && len(pend) == 1 && pend[0].RetryCount > c.MaxDeliveries {
					c.toDLQ(m, "max deliveries exceeded")
					dead = append(dead, m.ID)
					continue
				}
				retry = append(retry, m)
			}
			c.ack(dead...)
			if len(retry) > 0 {
				c.processBatch(ctx, retry)
			}
			if next == "0-0" || next == "" {
				break
			}
			start = next
		}
	}
}

// StatsLoop samples queue depth, pending, lag and DLQ size into gauges.
func (c *Consumer) StatsLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if n, err := c.rdb.XLen(sctx, rds.StreamEvents).Result(); err == nil {
			metrics.QueueDepth.Set(float64(n))
		}
		if n, err := c.rdb.XLen(sctx, rds.StreamDLQ).Result(); err == nil {
			metrics.QueueDLQ.Set(float64(n))
		}
		if groups, err := c.rdb.XInfoGroups(sctx, rds.StreamEvents).Result(); err == nil {
			for _, g := range groups {
				if g.Name == rds.Group {
					metrics.QueuePending.Set(float64(g.Pending))
					metrics.QueueLag.Set(float64(g.Lag))
				}
			}
		}
		cancel()
	}
}

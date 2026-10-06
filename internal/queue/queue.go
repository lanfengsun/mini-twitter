// Package queue wraps Redis Streams: API-side enqueue and the worker-side consumer
// (consumer group, bounded parallelism, redelivery, dead-letter stream).
package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"minitwitter/internal/rds"
)

type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue { return &Queue{rdb: rdb} }

// Enqueue appends an event. The entry is durable (Redis AOF, everysec) once XADD returns.
func (q *Queue) Enqueue(ctx context.Context, typ string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: rds.StreamEvents,
		Values: map[string]interface{}{"t": typ, "d": string(b), "ts": time.Now().UnixMilli()},
	}).Err()
}

// Depth is the number of entries still in the stream: waiting plus delivered-but-unacked
// (consumers XDEL entries after acknowledging them).
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	return q.rdb.XLen(ctx, rds.StreamEvents).Result()
}

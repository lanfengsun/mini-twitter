# Design

## Goals and scope
A working Twitter-like backend and web page on one laptop that demonstrates design, operation, observation,
scaling and saturation analysis. In scope: sign up / log in / log out, text posts, follow, reply, like, home
feed, one HTML/JS page, metrics dashboard, searchable logs, load testing. Out of scope: unfollow, unlike,
edit/delete, notifications, search, media.

**Deviation from the original brief:** there is no periodic feed-generation job and no "posts since the last
run" delta query. Fan-out on write plus lazy rebuild plus optimistic rendering covers the same ground
(see "Feed").

## Architecture
nginx (load balancer + static page) → `api` ×N → Redis and 3 Postgres shards; `worker` drains a Redis Stream.
Observability: Prometheus, Grafana, cAdvisor, redis/postgres exporters, Loki + Promtail.

| Decision | Choice | Why |
|---|---|---|
| Language | Go (api, worker, seed) | matches the team's production stack; cheap goroutines for fan-out |
| Queue | Redis Streams + consumer group | Redis is already required; Kafka is too heavy for a laptop. At-least-once, so handlers are idempotent |
| Auth | server-side sessions in Redis, bcrypt | logout is a key delete (instant revocation) |
| Sharding | app-level routing over 3 Postgres containers | transparent and easy to reason about; no extra moving part |
| IDs | 53-bit time-sortable (41 ms + 4 node + 8 seq) | sortable, exact as a float64 Redis score and as a JS number |

## Data model and sharding

| Table | Shard key | Serves |
|---|---|---|
| `users` | hash(username) | login, username → id |
| `posts` | hash(post_id) | even spread: a celebrity's posts do not pile onto one shard |
| `following` | hash(follower_id) | "who do I follow" |
| `followers` | hash(followee_id) | fan-out targets ("who follows me") |
| `likes`, `replies` | hash(post_id) | co-located with the post |

Each follow edge is written to both `following` and `followers` (denormalised on purpose) through the queue,
with idempotent inserts. Sharding posts by post_id makes "recent posts by author X" a parallel scatter-gather
over all shards; it only runs on cache misses because the worker maintains `author_posts:{id}` (newest 50) in Redis.

## Feed
Hybrid fan-out.
- **Normal author (< 10k followers):** the worker inserts the post, then pushes its id into `feed:{follower}`
  (a sorted set, capped at 200, 7-day TTL refreshed on read) for every follower **that has a live feed key**.
  Users who are not active have no key, so fan-out costs nothing for them and Redis memory is bounded by
  active users rather than total users.
- **Celebrity (≥ 10k followers):** not fanned out. A read merges the user's feed with `author_posts:{celeb}`
  for each celebrity they follow (`SINTER following:{uid} celebs`).
- **Cold feed** (key missing: new, returning, or Redis restarted): rebuilt on read from the user's
  non-celebrity followees' recent posts. New follow: the worker backfills the followee's latest posts.
- **Freshness:** eventual. Measured as `feed_freshness_seconds` (post created → landed in feeds). The author
  sees their own post immediately through optimistic rendering in the page.
- Pagination: cursor = id of the last post seen (ids are time-sortable).

## Write path
`POST` → validate → (if queue deeper than `QUEUE_MAX_DEPTH`: `429`) → `XADD` → `202` with the final id.
Worker: handle in parallel → `XACK`+`XDEL`. Failures stay pending and are reclaimed after 30 s; after 5
deliveries (or a malformed payload) the entry moves to `events:dlq`.

## Counters
Likes/replies insert a row (idempotent), then `HINCRBY cnt:{post}` in Redis; a flusher folds deltas into
Postgres every second. Reads show cached base + pending delta. A crash between taking and applying a delta
loses it, so counts are approximate by design (a reconciliation job would repair drift).

## Observability
Metrics (Prometheus): per-route request rate, latency histograms and status codes (success rate = non-5xx;
429 = deliberate shedding), queue depth/pending/lag/DLQ, worker throughput and handling time, feed
freshness, cache hit/miss by cache, fan-out volume, DB pool stats per shard, container CPU/memory
(cAdvisor), Postgres and Redis internals (exporters). Logs: structured JSON with `request_id` (assigned by
nginx, echoed in the `X-Request-Id` header and shown in the page's error toast), shipped by Promtail to Loki.

## Failure behaviour (single host, no replicas)
Redis down: sessions, feeds and the queue are unavailable, so the API returns 503 for everything.
A shard down: requests that touch that shard fail; requests that do not still work. Queue entries survive a
Redis restart thanks to AOF (`everysec`, so up to ~1 s of writes can be lost).

## What I would do with more time
Postgres replicas and Redis Sentinel/Cluster (no more single points of failure); Kafka for the event log;
per-user rate limiting; stampede protection (single-flight) on cold feed rebuilds; counter reconciliation;
online shard rebalancing (consistent hashing / virtual shards instead of `hash % N`); tracing (OpenTelemetry);
autoscaling on queue lag; a proper chaos suite; a real load generator host separate from the system under test.

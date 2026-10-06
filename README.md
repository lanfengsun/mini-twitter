# Mini Twitter

A small but realistic distributed system: a Twitter-style backend and web page that runs on one laptop in
docker-compose, built to show how a system is designed, operated, observed, scaled and pushed to saturation.

```
                  ┌────────────┐      ┌──────────────┐
 browser ───────► │   nginx    │ ───► │ api  ×N (Go) │ ──┐  stateless; scale with `make scale N=4`
                  │ LB + static│      └──────┬───────┘   │
                  └────────────┘             │ XADD      │ reads
                                             ▼           ▼
                                   ┌────────────────────────────┐
                                   │ Redis: sessions · feeds ·  │
                                   │ caches · counters · Stream │
                                   └──────────────┬─────────────┘
                                                  │ XREADGROUP
                                           ┌──────▼───────┐        ┌─────────────────────────┐
                                           │ worker (Go)  │ ─────► │ Postgres shards pg0..pg2│
                                           │ persist+fanout│        │ (app-level sharding)    │
                                           └──────────────┘        └─────────────────────────┘
 Observability: Prometheus · Grafana · cAdvisor · redis/postgres exporters · Loki + Promtail
```

Features: sign up / log in / log out, text posts, follow, reply, like, home feed (newest first), one plain
HTML/JS page. Out of scope: unfollow, unlike, edit/delete, notifications, search, media.
See [docs/DESIGN.md](docs/DESIGN.md) for the design and [docs/WRITEUP.md](docs/WRITEUP.md) for the
load-test findings.

> **Status:** the pure-logic packages (`idgen`, `shard`, `feed`) have unit tests that pass. The rest of the Go
> code was written without being able to download the Redis/Postgres/Prometheus libraries, so it has been
> syntax-checked and type-checked against stubs but **not compiled or run against the real libraries yet**.
> Expect the first `make up` to possibly surface a small compile error or two; fix and re-run.

## Prerequisites

- Docker Desktop with **≥ 6 CPUs and ~10 GB RAM** (Settings → Resources). All images have arm64 builds.
- [k6](https://k6.io) for load tests: `brew install k6`. It runs natively, outside the Docker VM.
- No local Go needed: everything builds inside Docker.

## Quick start

```bash
make tidy        # optional, once: resolves Go deps and writes go.sum (commit it)
make up          # build + start ~15 containers
make seed-small  # 20k users, 100k posts, a 15k-follower celebrity (about a minute)
open http://localhost:8080
```

Log in as `u1` with password `password` (every seeded user has that password), or sign up. Try following
`celeb1`, posting, liking and replying.

| What | URL |
|---|---|
| App | http://localhost:8080 |
| Grafana dashboard (no login) | http://localhost:3000 → *Mini Twitter* |
| Prometheus | http://localhost:9090 |

For the full load-test dataset run `make seed` instead (500k users, 2M posts, ~3M follow edges stored twice,
one celebrity with 300k followers, plus a few organically popular accounts). Scale it with environment
variables, e.g. `USERS=200000 POSTS=800000 make seed`.

## How the pieces behave

- **Writes are queued.** `POST /api/posts` (and follow, like, reply) validates, appends to a Redis Stream and
  returns `202`. The id is final, so the page renders the post immediately. Past `QUEUE_MAX_DEPTH`
  the API sheds load with `429` + `Retry-After`.
- **Worker** consumes with a consumer group, handles events in parallel, is idempotent (at-least-once
  delivery), retries failures, and moves poison events to the `events:dlq` stream.
- **Feed = hybrid fan-out.** Normal authors: the worker pushes the post id into each *active* follower's
  Redis feed (inactive users have no feed key and cost nothing). Authors with ≥ 10k followers are *not*
  fanned out; readers merge `author_posts:{celebrity}` at read time. Cold feeds rebuild lazily from Postgres.
- **Sharding.** 3 Postgres shards, routed in the application: `users` by hash(username); `posts`, `likes`,
  `replies` by hash(post_id); `following` by follower_id; `followers` by followee_id (every follow is stored
  in both orientations so "who do I follow" and "who follows me" are each single-shard reads).
- **Counters.** Likes and reply counts accumulate in Redis and are flushed to Postgres once a second, so a
  viral post never becomes a hot row.
- **Auth.** Server-side sessions in Redis (logout deletes the key), bcrypt passwords.

## Observing it

Grafana's *Mini Twitter* dashboard has: request rate by route, success rate, latency p50/p95/p99 (overall and
per route), 5xx and 429 rates, queue depth / pending / lag / DLQ, worker throughput, feed-freshness lag,
cache hit rates (application and Redis), container CPU and memory per service, DB connection-pool usage and
wait time, Postgres and Redis internals, and error logs.

Searching logs: Grafana → Explore → *Loki*.

```logql
{service="api"} | json | status >= 500                 # all server errors
{service="api"} | json | request_id="<id from the UI toast or X-Request-Id header>"
{service=~"api|worker"} | json | level="ERROR"
{service="worker"} |= "dead-letter"
```

The API logs every 4xx/5xx and every request slower than 250 ms, plus 1% of the rest
(`LOG_SAMPLE_RATE`), so logging does not become the bottleneck under load.

## Load testing

```bash
make load-mixed RATE=1000     # steady mixed workload at a fixed arrival rate
make load-ramp                # climbs in steps until it saturates
make load-burst               # 5k posts/s for 60 s while readers keep reading
make load-auth                # signup/login storm (bcrypt is the first CPU wall)
make scale N=4                # then re-run to compare 1 → 2 → 4 API replicas
```

All scenarios use an **open model** (fixed arrival rate), so a slow server cannot slow the offered load and
hide its own latency (coordinated omission). **Saturation** is defined as the first load step where p99 > 500 ms,
the error rate (5xx / timeouts) > 1%, or achieved throughput stops following offered load.

Honest limits: k6 and the containers share the Mac's 8 cores, scaling out on one host plateaus at the core
count, and "millions of simultaneous posters" is an extrapolation from measured per-instance throughput, not a
literal test.

Failure drills while a test runs: `make drill-shard` (kill a Postgres shard) and `make drill-redis`
(stop Redis); restore with `docker compose start pg1` / `docker compose start redis`.

## Layout

```
cmd/api        HTTP API            cmd/worker   queue consumer + fan-out + counter flush
cmd/seed       COPY-based seeder   internal/    idgen shard feed store rds queue feedsvc metrics ...
db/schema.sql  per-shard schema    deploy/      nginx, prometheus, loki, promtail, grafana (+dashboard)
loadtest/k6    load scenarios      docs/        DESIGN.md, WRITEUP.md
```

## Troubleshooting

- **Build error on first `make up`**: see the status note above; the error names the file and line.
- **cAdvisor shows no per-container data on Docker Desktop**: it needs the volume mounts in
  `docker-compose.yml`; if the `container_label_com_docker_compose_service` label is missing, query by `name`
  instead, or use `docker stats` for CPU/memory during the test.
- **postgres-exporter shows only one shard**: it is given three comma-separated DSNs; if your version rejects
  that, run one exporter per shard.
- **Not enough memory**: lower the seed size, or reduce `shared_buffers` / the Redis `--maxmemory` in the compose file.

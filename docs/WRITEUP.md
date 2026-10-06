# Load-test write-up (fill in after running)

> Nothing below has been measured yet. Every `TBD` is a number to take from your own run; the
> hypotheses are predictions to confirm or refute, not results.

## Setup
- Machine: MacBook Pro M1 Pro, 8 cores, 16 GB. Docker Desktop VM: TBD CPUs / TBD GB.
- Seed: TBD users, TBD posts, TBD follow edges, 1 celebrity with TBD followers (`make seed`).
- Container limits: see `docker-compose.yml` (api 1 CPU each, worker 1.5, each shard 1, Redis 1, nginx 1).
- Caveat: k6 runs on the same 8 cores as the Docker VM, so results are conservative for the system.

## 1. How it is designed
See `docs/DESIGN.md`. One paragraph summary for the interview: queue-buffered writes, hybrid fan-out
feed, sharded Postgres, Redis for sessions/feeds/caches/counters, stateless API behind nginx.

## 2. How it is observed
Screenshot the Grafana dashboard during a run (golden signals, queue, saturation rows). Show one debugging
walkthrough: a failure drill → spike on the 5xx panel → Loki `{service="api"} | json | status >= 500` →
copy a `request_id` → follow that request. Paste the screenshots here.

## 3. Scaling and saturation point
Saturation = first step where p99 > 500 ms, error rate > 1%, or throughput stops following offered load.

| API replicas | Max sustainable RPS (mixed) | p99 at that point | First bottleneck seen |
|---|---|---|---|
| 1 | TBD | TBD | TBD |
| 2 | TBD | TBD | TBD |
| 4 | TBD | TBD | TBD |

Expectation to check: throughput scales roughly linearly from 1 to 2 replicas, then flattens as the VM's
CPUs are shared with Postgres, Redis and the worker (single-host ceiling, not an architectural one).

## 4. What breaks under load
Record observations for each scenario. Predictions to test:

| Scenario | Prediction | Observed |
|---|---|---|
| Signup/login storm (`make load-auth`) | bcrypt CPU saturates the API long before Redis or Postgres | TBD |
| Mixed read-heavy (`load-ramp`) | Redis (single-threaded) or API CPU is the first wall; cold feed rebuilds hurt early on | TBD |
| Write burst (`load-burst`) | queue depth climbs, API latency stays flat, 429s once depth passes the limit, feed freshness lag grows then recovers | TBD |
| Celebrity post | no fan-out spike; reads of celebrity followers stay cheap | TBD |
| Kill a shard | requests touching that shard fail (5xx), the rest keep working; recovery after restart | TBD |
| Stop Redis | everything 503 (sessions, feeds, queue all live there) | TBD |

## 5. What I changed to improve it
Fill in as you go; one row per fix with before/after numbers from the same scenario.

| Change | Why (evidence from dashboard) | Before | After |
|---|---|---|---|
| TBD | TBD | TBD | TBD |

Candidates if the evidence supports them: raise `WORKER_CONCURRENCY` / add worker replicas if queue lag
dominates; raise `SHARD_MAX_CONNS` if pool wait time is non-zero; pipeline or shrink hot Redis calls; cache
more aggressively; lower bcrypt cost only with a security trade-off stated; add single-flight to cold rebuilds.

## 6. With more time
See the end of `docs/DESIGN.md`.

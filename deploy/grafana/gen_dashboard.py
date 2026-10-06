#!/usr/bin/env python3
"""Generates dashboards/mini-twitter.json. Run: python3 deploy/grafana/gen_dashboard.py"""
import json, pathlib

PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
panels, next_id = [], 1
y = 0

def ts(title, targets, x, w, h=8, unit=None, desc=None, ymin=None, ymax=None, stack=False):
    global next_id, y
    p = {
        "id": next_id, "type": "timeseries", "title": title, "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"datasource": PROM, "expr": e, "legendFormat": l, "refId": chr(65 + i)} for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit or "short", "custom": {"fillOpacity": 12, "lineWidth": 1,
                        "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    }
    if ymin is not None: p["fieldConfig"]["defaults"]["min"] = ymin
    if ymax is not None: p["fieldConfig"]["defaults"]["max"] = ymax
    if desc: p["description"] = desc
    next_id += 1
    panels.append(p)

def row(title):
    global next_id, y
    panels.append({"id": next_id, "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    next_id += 1
    y += 1

def newline(h=8):
    global y
    y += h

CS = 'container_label_com_docker_compose_service'

row("Golden signals (API)")
ts("Requests / sec by route", [('sum by (route) (rate(http_requests_total[30s]))', '{{route}}')], 0, 8, stack=True, unit="reqps")
ts("Success rate (non-5xx)", [('1 - (sum(rate(http_requests_total{status=~"5.."}[30s])) / sum(rate(http_requests_total[30s])))', 'success rate')],
   8, 8, unit="percentunit", ymin=0, ymax=1, desc="5xx counts as failure. 429s are deliberate load shedding, shown separately.")
ts("Latency p50 / p95 / p99", [
    ('histogram_quantile(0.50, sum by (le) (rate(http_request_duration_seconds_bucket[30s])))', 'p50'),
    ('histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[30s])))', 'p95'),
    ('histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[30s])))', 'p99')], 16, 8, unit="s",
   desc="Saturation criterion used in the write-up: p99 > 500 ms.")
newline()
ts("p99 latency by route", [('histogram_quantile(0.99, sum by (le, route) (rate(http_request_duration_seconds_bucket[30s])))', '{{route}}')], 0, 8, unit="s")
ts("Errors and load shedding", [
    ('sum(rate(http_requests_total{status=~"5.."}[30s]))', '5xx / s'),
    ('sum(rate(http_requests_total{status="429"}[30s]))', '429 shed / s'),
    ('sum(rate(http_requests_total{status=~"4..",status!="429"}[30s]))', 'other 4xx / s')], 8, 8, unit="reqps")
ts("API replicas up", [('count(up{job="api"} == 1)', 'api replicas')], 16, 8, ymin=0)
newline()

row("Queue and fan-out")
ts("Queue depth", [('queue_depth', 'depth (waiting + unacked)'), ('queue_pending', 'delivered, unacked'),
                   ('queue_consumer_lag', 'consumer lag'), ('queue_dlq_depth', 'dead-letter')], 0, 8,
   desc="Climbs during a write burst and should drain once the burst ends.")
ts("Worker throughput by event type", [('sum by (type) (rate(events_processed_total{result="ok"}[30s]))', '{{type}} ok'),
                                       ('sum by (type) (rate(events_processed_total{result=~"error|dlq"}[30s]))', '{{type}} {{result}}')], 8, 8, unit="ops")
ts("Feed freshness (post created -> in followers' feeds)", [
    ('histogram_quantile(0.50, sum by (le) (rate(feed_freshness_seconds_bucket[30s])))', 'p50'),
    ('histogram_quantile(0.95, sum by (le) (rate(feed_freshness_seconds_bucket[30s])))', 'p95'),
    ('histogram_quantile(0.99, sum by (le) (rate(feed_freshness_seconds_bucket[30s])))', 'p99')], 16, 8, unit="s")
newline()
ts("Fan-out pushes / sec", [('rate(fanout_recipients_total[30s])', 'feed pushes'),
                            ('rate(fanout_skipped_celebrity_total[30s])', 'celebrity posts (not fanned out)'),
                            ('rate(feed_rebuilds_total[30s])', 'cold feed rebuilds')], 0, 8)
ts("Event handling latency p99", [('histogram_quantile(0.99, sum by (le, type) (rate(event_process_seconds_bucket[30s])))', '{{type}}')], 8, 8, unit="s")
newline()

row("Caches")
ts("Cache hit rate (application)", [('sum by (cache) (rate(cache_requests_total{result="hit"}[1m])) / sum by (cache) (rate(cache_requests_total[1m]))', '{{cache}}')],
   0, 12, unit="percentunit", ymin=0, ymax=1)
ts("Redis keyspace hit rate", [('rate(redis_keyspace_hits_total[1m]) / (rate(redis_keyspace_hits_total[1m]) + rate(redis_keyspace_misses_total[1m]))', 'keyspace hit rate')],
   12, 12, unit="percentunit", ymin=0, ymax=1)
newline()

row("Saturation: containers (cAdvisor)")
ts("CPU by service (cores)", [(f'sum by ({CS}) (rate(container_cpu_usage_seconds_total{{{CS}!=""}}[30s]))', '{{' + CS + '}}')], 0, 12, unit="short",
   desc="Compare with the limits in docker-compose.yml: a service pinned at its limit is saturated.")
ts("Memory by service", [(f'sum by ({CS}) (container_memory_working_set_bytes{{{CS}!=""}})', '{{' + CS + '}}')], 12, 12, unit="bytes")
newline()

row("Saturation: databases")
ts("DB pool: connections in use per shard (per process)", [('go_sql_in_use_connections', '{{db_name}} {{instance}}')], 0, 8,
   desc="Pinned at SHARD_MAX_CONNS means requests are queueing for a connection.")
ts("DB pool wait time / sec", [('sum by (db_name) (rate(go_sql_wait_duration_seconds_total[30s]))', '{{db_name}}')], 8, 8, unit="s")
ts("Postgres commits / sec per shard", [('sum by (server) (rate(pg_stat_database_xact_commit{datname="app"}[30s]))', '{{server}}')], 16, 8, unit="ops")
newline()
ts("Postgres backends per shard", [('sum by (server) (pg_stat_database_numbackends{datname="app"})', '{{server}}')], 0, 8)
ts("Postgres locks held (all modes)", [('sum by (server) (pg_locks_count{datname="app"})', '{{server}}')], 8, 8,
   desc="Total locks per shard; a sustained climb under load points at contention.")
ts("Redis: ops/sec, clients, memory", [('rate(redis_commands_processed_total[30s])', 'commands/s'),
                                      ('redis_connected_clients', 'clients'),
                                      ('redis_memory_used_bytes / 1e6', 'memory MB')], 16, 8)
newline()

row("Logs (Loki)")
panels.append({"id": next_id, "type": "timeseries", "title": "Error log lines / sec", "datasource": LOKI,
               "gridPos": {"x": 0, "y": y, "w": 24, "h": 6},
               "targets": [{"datasource": LOKI, "expr": 'sum by (service) (rate({service=~"api|worker"} | json | level="ERROR" [30s]))',
                            "legendFormat": "{{service}}", "refId": "A"}],
               "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []}}); next_id += 1
newline(6)
panels.append({"id": next_id, "type": "logs", "title": "Recent API errors (4xx/5xx and slow requests)", "datasource": LOKI,
               "gridPos": {"x": 0, "y": y, "w": 24, "h": 10},
               "targets": [{"datasource": LOKI, "expr": '{service="api"} | json | status >= 400', "refId": "A"}],
               "options": {"showTime": True, "wrapLogMessage": True, "enableLogDetails": True, "sortOrder": "Descending"}}); next_id += 1

dash = {
    "uid": "mini-twitter", "title": "Mini Twitter", "schemaVersion": 39, "version": 1, "editable": True,
    "refresh": "5s", "time": {"from": "now-15m", "to": "now"}, "timezone": "browser", "tags": ["mini-twitter"],
    "templating": {"list": []}, "annotations": {"list": []}, "panels": panels,
}
out = pathlib.Path(__file__).parent / "dashboards" / "mini-twitter.json"
out.write_text(json.dumps(dash, indent=1))
print(f"wrote {out} ({len(panels)} panels)")

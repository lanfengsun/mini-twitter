// Package metrics declares every Prometheus metric the services emit.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

var (
	// --- API: RED metrics -------------------------------------------------------
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests by route pattern and status code. Success rate = 1 - 5xx/total; 429 is deliberate load shedding.",
	}, []string{"route", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route pattern.",
		Buckets: latencyBuckets,
	}, []string{"route"})

	EnqueueRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "enqueue_rejected_total",
		Help: "Writes rejected with 429 because the queue was deeper than QUEUE_MAX_DEPTH.",
	})

	// --- caches -----------------------------------------------------------------
	CacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_requests_total",
		Help: "Redis cache lookups by cache (post, following, author_posts, feed) and result (hit, miss).",
	}, []string{"cache", "result"})

	FeedRebuilds = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_rebuilds_total",
		Help: "Cold feeds rebuilt from Postgres on read.",
	})

	// --- worker -----------------------------------------------------------------
	EventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_processed_total",
		Help: "Queue events handled by type and result (ok, error, dlq).",
	}, []string{"type", "result"})

	EventDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "event_process_seconds",
		Help:    "Time to handle one queue event.",
		Buckets: latencyBuckets,
	}, []string{"type"})

	FeedFreshness = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "feed_freshness_seconds",
		Help:    "Time from post creation until it has landed in follower feeds (queue wait + fan-out).",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	})

	FanoutRecipients = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fanout_recipients_total",
		Help: "Feed pushes attempted (one per follower per post).",
	})

	FanoutSkippedCelebrity = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fanout_skipped_celebrity_total",
		Help: "Posts by celebrity accounts that skipped fan-out (merged at read time instead).",
	})

	// --- queue (sampled by the worker) -------------------------------------------
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_depth",
		Help: "Entries in the events stream (waiting + delivered-but-unacked).",
	})
	QueuePending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_pending",
		Help: "Entries delivered to a consumer but not yet acknowledged.",
	})
	QueueLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_consumer_lag",
		Help: "Entries not yet delivered to the consumer group.",
	})
	QueueDLQ = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_dlq_depth",
		Help: "Entries in the dead-letter stream.",
	})
)

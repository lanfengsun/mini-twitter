package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"minitwitter/internal/metrics"
	"minitwitter/internal/rds"
)

type ctxKey int

const infoKey ctxKey = 0

// reqInfo is shared between the middleware and the handlers so the access log line
// carries the user and the underlying error of a failed request.
type reqInfo struct {
	ID     string
	UserID int64
	Err    string
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}

func infoFrom(ctx context.Context) *reqInfo {
	if v, ok := ctx.Value(infoKey).(*reqInfo); ok {
		return v
	}
	return &reqInfo{}
}

// middleware assigns a request id (nginx sets X-Request-Id; it is echoed back and logged
// so one request can be followed from the browser to the logs), enforces a per-request
// timeout, records RED metrics keyed by route pattern, and writes a sampled access log:
// every 4xx/5xx and slow request is logged, plus LOG_SAMPLE_RATE of the successes.
func (a *App) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		info := &reqInfo{ID: r.Header.Get("X-Request-Id")}
		if info.ID == "" {
			info.ID = randHex(8)
		}
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), infoKey, info), a.reqTimeout)
		defer cancel()
		r2 := r.WithContext(ctx)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		w.Header().Set("X-Request-Id", info.ID)

		next.ServeHTTP(sw, r2)

		route := r2.Pattern // set by ServeMux on the request it was handed
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(sw.status)).Inc()
		metrics.HTTPDuration.WithLabelValues(route).Observe(elapsed.Seconds())

		if route == "GET /metrics" || route == "GET /healthz" {
			return
		}
		if sw.status >= 400 || elapsed > 250*time.Millisecond || rand.Float64() < a.logSample {
			lvl := a.log.Info
			if sw.status >= 500 {
				lvl = a.log.Error
			} else if sw.status >= 400 {
				lvl = a.log.Warn
			}
			lvl("request",
				"request_id", info.ID, "route", route, "path", r.URL.Path,
				"status", sw.status, "latency_ms", elapsed.Milliseconds(),
				"user_id", info.UserID, "error", info.Err)
		}
	})
}

// ------------------------------------------------------------------ helpers

type Sess struct {
	UID   int64
	Name  string
	Token string
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// fail writes an error response and records the underlying cause for the access log.
func fail(w http.ResponseWriter, r *http.Request, code int, msg string, cause error) {
	if cause != nil {
		infoFrom(r.Context()).Err = cause.Error()
	}
	writeJSON(w, code, map[string]string{"error": msg})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		fail(w, r, http.StatusBadRequest, "invalid JSON body", err)
		return false
	}
	return true
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

// auth resolves the bearer token to a server-side session stored in Redis
// (logout = delete the key, so revocation is immediate).
func (a *App) auth(h func(http.ResponseWriter, *http.Request, Sess)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := bearer(r)
		if tok == "" {
			fail(w, r, http.StatusUnauthorized, "missing token", nil)
			return
		}
		v, err := a.rdb.Get(r.Context(), rds.Session(tok)).Result()
		if err == redis.Nil {
			fail(w, r, http.StatusUnauthorized, "invalid or expired session", nil)
			return
		}
		if err != nil {
			fail(w, r, http.StatusServiceUnavailable, "session store unavailable", err)
			return
		}
		uidStr, name, ok := strings.Cut(v, "|")
		uid, perr := strconv.ParseInt(uidStr, 10, 64)
		if !ok || perr != nil {
			fail(w, r, http.StatusUnauthorized, "corrupt session", nil)
			return
		}
		infoFrom(r.Context()).UserID = uid
		h(w, r, Sess{UID: uid, Name: name, Token: tok})
	}
}

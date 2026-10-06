package main

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"

	"minitwitter/internal/events"
	"minitwitter/internal/feed"
	"minitwitter/internal/idgen"
	"minitwitter/internal/metrics"
	"minitwitter/internal/rds"
	"minitwitter/internal/store"
)

const maxPostRunes = 280

var usernameRe = regexp.MustCompile(`^[a-z0-9_]{3,20}$`)

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type userInfo struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type authResponse struct {
	Token string   `json:"token"`
	User  userInfo `json:"user"`
}

// ------------------------------------------------------------------ auth

func (a *App) newSession(ctx context.Context, uid int64, name string) (string, error) {
	tok := randHex(16)
	err := a.rdb.Set(ctx, rds.Session(tok), strconv.FormatInt(uid, 10)+"|"+name, a.sessionTTL).Err()
	return tok, err
}

func (a *App) signup(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if !readJSON(w, r, &c) {
		return
	}
	name := strings.ToLower(strings.TrimSpace(c.Username))
	if !usernameRe.MatchString(name) {
		fail(w, r, http.StatusBadRequest, "username must be 3-20 characters: a-z, 0-9, _", nil)
		return
	}
	if len(c.Password) < 6 || len(c.Password) > 72 {
		fail(w, r, http.StatusBadRequest, "password must be 6-72 characters", nil)
		return
	}
	// bcrypt is deliberately expensive; under a signup storm it is the first CPU bottleneck.
	hash, err := bcrypt.GenerateFromPassword([]byte(c.Password), a.bcryptCost)
	if err != nil {
		fail(w, r, http.StatusInternalServerError, "could not hash password", err)
		return
	}
	u := store.User{ID: a.ids.Next(), Username: name, PasswordHash: string(hash)}
	if err := a.st.CreateUser(r.Context(), u); err != nil {
		if errors.Is(err, store.ErrUserExists) {
			fail(w, r, http.StatusConflict, "username already taken", nil)
			return
		}
		fail(w, r, http.StatusServiceUnavailable, "could not create user", err)
		return
	}
	tok, err := a.newSession(r.Context(), u.ID, u.Username)
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not create session", err)
		return
	}
	writeJSON(w, http.StatusCreated, authResponse{Token: tok, User: userInfo{ID: u.ID, Username: u.Username}})
}

func (a *App) login(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if !readJSON(w, r, &c) {
		return
	}
	name := strings.ToLower(strings.TrimSpace(c.Username))
	u, err := a.st.GetUserByName(r.Context(), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			fail(w, r, http.StatusUnauthorized, "wrong username or password", nil)
			return
		}
		fail(w, r, http.StatusServiceUnavailable, "login unavailable", err)
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(c.Password)) != nil {
		fail(w, r, http.StatusUnauthorized, "wrong username or password", nil)
		return
	}
	tok, err := a.newSession(r.Context(), u.ID, u.Username)
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not create session", err)
		return
	}
	writeJSON(w, http.StatusOK, authResponse{Token: tok, User: userInfo{ID: u.ID, Username: u.Username}})
}

func (a *App) logout(w http.ResponseWriter, r *http.Request, s Sess) {
	if err := a.rdb.Del(r.Context(), rds.Session(s.Token)).Err(); err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not log out", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) me(w http.ResponseWriter, r *http.Request, s Sess) {
	writeJSON(w, http.StatusOK, userInfo{ID: s.UID, Username: s.Name})
}

// ------------------------------------------------------------------ writes (queued)

// shed rejects a write with 429 when the queue is deeper than QUEUE_MAX_DEPTH, so a spike
// of writes cannot grow the backlog (and Redis memory) without bound.
func (a *App) shed(w http.ResponseWriter) bool {
	if a.depth.Load() > a.queueMax {
		metrics.EnqueueRejected.Inc()
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "system is busy, retry shortly"})
		return true
	}
	return false
}

func cleanText(s string) (string, bool) {
	s = strings.TrimSpace(s)
	n := utf8.RuneCountInString(s)
	return s, n >= 1 && n <= maxPostRunes
}

type textBody struct {
	Text string `json:"text"`
}

func (a *App) createPost(w http.ResponseWriter, r *http.Request, s Sess) {
	var b textBody
	if !readJSON(w, r, &b) {
		return
	}
	text, ok := cleanText(b.Text)
	if !ok {
		fail(w, r, http.StatusBadRequest, "post must be 1-280 characters", nil)
		return
	}
	if a.shed(w) {
		return
	}
	id := a.ids.Next()
	err := a.q.Enqueue(r.Context(), events.TypePost,
		events.Post{ID: id, AuthorID: s.UID, AuthorName: s.Name, Body: text})
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not enqueue post", err)
		return
	}
	// 202: accepted, not yet durable in Postgres. The id is final, so the client can render
	// the post immediately and a retry of the same event is idempotent.
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "created_at": idgen.Time(id)})
}

// userIDByName resolves a username to an id through a Redis cache in front of the
// username-sharded users table.
func (a *App) userIDByName(ctx context.Context, name string) (int64, error) {
	id, err := a.rdb.Get(ctx, rds.UserID(name)).Int64()
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, redis.Nil) {
		return 0, err
	}
	u, err := a.st.GetUserByName(ctx, name)
	if err != nil {
		return 0, err
	}
	a.rdb.Set(ctx, rds.UserID(name), u.ID, time.Hour)
	return u.ID, nil
}

func (a *App) follow(w http.ResponseWriter, r *http.Request, s Sess) {
	name := strings.ToLower(r.PathValue("username"))
	if name == s.Name {
		fail(w, r, http.StatusBadRequest, "you cannot follow yourself", nil)
		return
	}
	target, err := a.userIDByName(r.Context(), name)
	if errors.Is(err, store.ErrNotFound) {
		fail(w, r, http.StatusNotFound, "no such user", nil)
		return
	}
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "lookup failed", err)
		return
	}
	if a.shed(w) {
		return
	}
	err = a.q.Enqueue(r.Context(), events.TypeFollow, events.Follow{FollowerID: s.UID, FolloweeID: target})
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not enqueue follow", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"following": name})
}

func postID(r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id, err == nil && id > 0
}

func (a *App) like(w http.ResponseWriter, r *http.Request, s Sess) {
	id, ok := postID(r)
	if !ok {
		fail(w, r, http.StatusBadRequest, "bad post id", nil)
		return
	}
	if a.shed(w) {
		return
	}
	if err := a.q.Enqueue(r.Context(), events.TypeLike, events.Like{PostID: id, UserID: s.UID}); err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not enqueue like", err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (a *App) reply(w http.ResponseWriter, r *http.Request, s Sess) {
	pid, ok := postID(r)
	if !ok {
		fail(w, r, http.StatusBadRequest, "bad post id", nil)
		return
	}
	var b textBody
	if !readJSON(w, r, &b) {
		return
	}
	text, ok := cleanText(b.Text)
	if !ok {
		fail(w, r, http.StatusBadRequest, "reply must be 1-280 characters", nil)
		return
	}
	if a.shed(w) {
		return
	}
	id := a.ids.Next()
	err := a.q.Enqueue(r.Context(), events.TypeReply,
		events.Reply{ID: id, PostID: pid, AuthorID: s.UID, AuthorName: s.Name, Body: text})
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not enqueue reply", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "created_at": idgen.Time(id)})
}

// ------------------------------------------------------------------ reads

type replyView struct {
	ID        int64     `json:"id"`
	Author    string    `json:"author"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

func (a *App) replies(w http.ResponseWriter, r *http.Request, _ Sess) {
	id, ok := postID(r)
	if !ok {
		fail(w, r, http.StatusBadRequest, "bad post id", nil)
		return
	}
	rs, err := a.st.ListReplies(r.Context(), id, 50)
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not load replies", err)
		return
	}
	out := make([]replyView, 0, len(rs))
	for _, x := range rs {
		out = append(out, replyView{ID: x.ID, Author: x.AuthorName, Text: x.Body, CreatedAt: idgen.Time(x.ID)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"replies": out})
}

func (a *App) getFeed(w http.ResponseWriter, r *http.Request, s Sess) {
	cursor, err := feed.ParseCursor(r.URL.Query().Get("cursor"))
	if err != nil || cursor < 0 {
		fail(w, r, http.StatusBadRequest, "bad cursor", nil)
		return
	}
	limit := 20
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 50 {
		limit = v
	}
	posts, next, err := a.fs.ReadFeed(r.Context(), s.UID, cursor, limit)
	if err != nil {
		fail(w, r, http.StatusServiceUnavailable, "could not load feed", err)
		return
	}
	resp := map[string]any{"posts": posts}
	if next > 0 {
		resp["next_cursor"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *App) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.rdb.Ping(ctx).Err(); err != nil {
		fail(w, r, http.StatusServiceUnavailable, "redis unavailable", err)
		return
	}
	if err := a.st.Ping(ctx); err != nil {
		fail(w, r, http.StatusServiceUnavailable, "postgres unavailable", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

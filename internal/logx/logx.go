// Package logx builds the structured JSON logger every service uses. Promtail ships
// stdout to Loki, where fields (level, request_id, route, status...) are queryable with
// `| json`.
package logx

import (
	"log/slog"
	"os"
	"strings"
)

func New(service, level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})
	host, _ := os.Hostname()
	return slog.New(h).With("service", service, "host", host)
}

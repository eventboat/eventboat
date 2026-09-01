package observability

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/riverpod/riverpod/internal/config"
)

var currentLevel atomic.Int32

func init() {
	currentLevel.Store(int32(slog.LevelInfo))
}

type dynamicLevel struct{}

func (dynamicLevel) Level() slog.Level {
	return slog.Level(currentLevel.Load())
}

func (dynamicLevel) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.Level(currentLevel.Load())
}

func (dynamicLevel) Handle(ctx context.Context, r slog.Record) error {
	return slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: dynamicLevel{}}).Handle(ctx, r)
}

func (dynamicLevel) WithAttrs(attrs []slog.Attr) slog.Handler { return dynamicLevel{} }
func (dynamicLevel) WithGroup(name string) slog.Handler      { return dynamicLevel{} }

// InitLogging configures structured JSON logging with a dynamically adjustable level.
func InitLogging(cfg config.LoggingConfig) {
	level := parseLevel(cfg.Level)
	currentLevel.Store(int32(level))

	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: dynamicLevel{}})
	default:
		handler = &jsonDynamicHandler{w: os.Stderr}
	}
	slog.SetDefault(slog.New(handler))
}

type jsonDynamicHandler struct {
	w io.Writer
}

func (h *jsonDynamicHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return dynamicLevel{}.Enabled(ctx, level)
}

func (h *jsonDynamicHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs()+2)
	attrs["time"] = r.Time.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	attrs["level"] = r.Level.String()
	attrs["msg"] = r.Message
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	b, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	_, err = h.w.Write(append(b, '\n'))
	return err
}

func (h *jsonDynamicHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *jsonDynamicHandler) WithGroup(name string) slog.Handler      { return h }

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (s *Server) handleLogLevel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Level string `json:"level"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	level := parseLevel(body.Level)
	currentLevel.Store(int32(level))
	writeHealth(w, true, map[string]string{"level": body.Level})
}

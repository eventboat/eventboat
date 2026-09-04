// Package admin is the Admin REST surface: an HTTP-JSON subset of the ops
// service plus SSE status streaming and an embedded read-only UI
// (redesign-v3.md §3.4). Tool semantics are identical to MCP — both are
// thin shells over internal/ops; OpenAPI single-source generation is
// deferred to M4 (recorded in README).
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eventboat/eventboat/internal/ops"
)

// Handler builds the admin mux (also serves /metrics when metricsHandler is
// non-nil, and MCP at /mcp when mcpHandler is non-nil).
func Handler(svc *ops.Service, metricsHandler http.Handler, mcpHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	fail := func(w http.ResponseWriter, code int, err error) {
		writeJSON(w, code, map[string]any{"error": err.Error()})
	}
	body := func(w http.ResponseWriter, r *http.Request, dst any) bool {
		if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
			fail(w, http.StatusBadRequest, fmt.Errorf("bad JSON body: %w", err))
			return false
		}
		return true
	}

	// --- read surface ---
	mux.HandleFunc("GET /admin/status.json", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, svc.Status())
	})
	mux.HandleFunc("GET /admin/jobs/{pipeline}", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		runs, err := svc.Jobs(r.PathValue("pipeline"), limit)
		if err != nil {
			fail(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, runs)
	})
	mux.HandleFunc("GET /admin/tail/{node}", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		writeJSON(w, http.StatusOK, svc.Tail(r.PathValue("node"), n))
	})
	mux.HandleFunc("GET /admin/dlq/{pipeline}", func(w http.ResponseWriter, r *http.Request) {
		dls, err := svc.DeadLetterQuery(r.PathValue("pipeline"), r.URL.Query().Get("since"), r.URL.Query().Get("where"), 0)
		if err != nil {
			fail(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, dls)
	})
	mux.HandleFunc("GET /admin/catalog.json", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, svc.Catalog())
	})

	// --- write surface (verify-first inside ops.Deploy) ---
	mux.HandleFunc("POST /admin/deploy", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Config string `json:"config"`
		}
		if !body(w, r, &in) {
			return
		}
		summary, err := svc.Deploy(r.Context(), in.Config)
		if err != nil {
			fail(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, summary)
	})
	mux.HandleFunc("POST /admin/trigger/{pipeline}", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Parameters map[string]any `json:"parameters"`
			Wait       bool           `json:"wait"`
		}
		if !body(w, r, &in) {
			return
		}
		jr, err := svc.Trigger(r.Context(), r.PathValue("pipeline"), in.Parameters, in.Wait)
		if err != nil {
			fail(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, jr)
	})
	mux.HandleFunc("POST /admin/replay", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Pipeline string  `json:"pipeline"`
			Ids      []int64 `json:"ids"`
			At       string  `json:"at"`
		}
		if !body(w, r, &in) {
			return
		}
		n, err := svc.DeadLetterReplay(in.Pipeline, in.Ids, in.At)
		if err != nil {
			fail(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"replayed": n})
	})
	mux.HandleFunc("POST /admin/drain/{pipeline}", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Drain(r.PathValue("pipeline")); err != nil {
			fail(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "drained"})
	})
	mux.HandleFunc("POST /admin/pause/{pipeline}", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Pause(r.PathValue("pipeline")); err != nil {
			fail(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "paused"})
	})
	mux.HandleFunc("POST /admin/resume/{pipeline}", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Resume(context.WithoutCancel(r.Context()), r.PathValue("pipeline")); err != nil {
			fail(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "running"})
	})

	// --- SSE: status snapshots + events ---
	mux.HandleFunc("GET /admin/sse", func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			fail(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: hello\ndata: {}\n\n")
		fl.Flush()
		events, unsub := svc.Subscribe()
		defer unsub()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-events:
				b, _ := json.Marshal(ev.Data)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
				fl.Flush()
			case <-ticker.C:
				b, _ := json.Marshal(svc.Status())
				_, _ = fmt.Fprint(w, "event: status\ndata: "+string(b)+"\n\n")
				fl.Flush()
			}
		}
	})

	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	}
	if mcpHandler != nil {
		mux.Handle("POST /mcp", mcpHandler)
		mux.Handle("GET /mcp", mcpHandler)
		mux.Handle("DELETE /mcp", mcpHandler)
	}

	mux.HandleFunc("GET /admin/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, uiHTML)
	})
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusFound)
	})
	return mux
}

// Server runs the admin listener.
type Server struct{}

// Serve starts the HTTP server (blocks until ctx is done).
func Serve(ctx context.Context, listen string, handler http.Handler) error {
	srv := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

var _ = strings.TrimSpace

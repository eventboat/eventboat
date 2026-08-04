package httpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
)

// defaultMaxBodyBytes caps request bodies to protect against OOM from
// unbounded io.ReadAll; override with config max_body_bytes.
const defaultMaxBodyBytes = 10 << 20 // 10 MiB

func init() {
	registry.RegisterSource("http_server", func(id string, cfg map[string]any) (stage.Source, error) {
		addr := basestage.ConfigString(cfg, "address")
		if addr == "" {
			addr = basestage.ConfigString(cfg, "listen")
		}
		if addr == "" {
			addr = ":8080"
		}
		path := basestage.ConfigString(cfg, "path")
		if path == "" {
			path = "/"
		}
		maxBody := int64(basestage.ConfigInt(cfg, "max_body_bytes", defaultMaxBodyBytes))
		if maxBody <= 0 {
			return nil, fmt.Errorf("http_server source: max_body_bytes must be positive")
		}
		return &Source{
			Base: basestage.Base{IDVal: id, KindVal: stage.KindSource, TypeVal: "http_server"},
			addr: addr,
			path: path,
			maxBodyBytes: maxBody,
		}, nil
	})
}

type Source struct {
	basestage.Base
	addr         string
	path         string
	maxBodyBytes int64

	server *http.Server
	ln     net.Listener
	errCh  chan error
	mu     sync.Mutex
	out    chan<- *message.Message
}

// Init binds the listener synchronously so port-in-use and similar startup
// failures surface immediately instead of being swallowed by a goroutine.
func (s *Source) Init(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.handle)
	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("http_server source: listen %s: %w", s.addr, err)
	}
	s.ln = ln
	s.errCh = make(chan error, 1)
	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.errCh <- err
		}
	}()
	return nil
}

func (s *Source) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *Source) handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodyBytes))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	meta := map[string]any{
		"http.method": r.Method,
		"http.path":   r.URL.Path,
	}
	msg := message.New(body, meta)
	s.mu.Lock()
	out := s.out
	s.mu.Unlock()
	if out == nil {
		http.Error(w, "source not ready", http.StatusServiceUnavailable)
		return
	}
	select {
	case out <- msg:
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	default:
		http.Error(w, "backpressure", http.StatusTooManyRequests)
	}
}

// Consume forwards messages and reports asynchronous server failures
// (e.g. listener errors after a successful Init) instead of hiding them.
func (s *Source) Consume(ctx context.Context, out chan<- *message.Message) error {
	s.mu.Lock()
	s.out = out
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.errCh:
		return fmt.Errorf("http_server source: %w", err)
	}
}

func (s *Source) HealthCheck(ctx context.Context) stage.HealthStatus {
	if s.server == nil {
		return stage.HealthStatus{Healthy: false, Message: "not started", Since: time.Now()}
	}
	return s.Base.HealthCheck(ctx)
}

func (s *Source) String() string {
	return fmt.Sprintf("http_server@%s%s", s.addr, s.path)
}

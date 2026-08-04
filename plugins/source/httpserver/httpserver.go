package httpserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
)

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
		return &Source{
			Base: basestage.Base{IDVal: id, KindVal: stage.KindSource, TypeVal: "http_server"},
			addr: addr,
			path: path,
		}, nil
	})
}

type Source struct {
	basestage.Base
	addr   string
	path   string
	server *http.Server
	mu     sync.Mutex
	out    chan<- *message.Message
}

func (s *Source) Init(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.handle)
	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = s.server.ListenAndServe()
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
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

func (s *Source) Consume(ctx context.Context, out chan<- *message.Message) error {
	s.mu.Lock()
	s.out = out
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
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

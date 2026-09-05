package builtin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

type httpServerSourceConfig struct {
	Listen      string `json:"listen" schema:"desc=host:port to listen on"`
	Path        string `json:"path" schema:"default=/"`
	MaxBodyByte int64  `json:"max_body_bytes" schema:"min=1,default=1048576"`
}

func registerHTTPServerSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "http_server", 1, nil, func(c httpServerSourceConfig) (registry.Source, error) {
		return &httpServerSource{listen: c.Listen, path: c.Path, maxBody: c.MaxBodyByte}, nil
	})
}

// httpServerSource receives events over HTTP POST. There is no offset to
// commit — the spool is the truth (redesign-v3.md §6.2). The HTTP response
// acknowledges only that the event has been accepted for spooling, which the
// engine guarantees before the message becomes visible to the DAG.
type httpServerSource struct {
	listen  string
	path    string
	maxBody int64

	mu     sync.Mutex
	seq    int64
	server *http.Server
}

func (s *httpServerSource) Init(state []byte) error { return nil }

func (s *httpServerSource) Run(ctx context.Context, emit func(registry.Message)) {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, s.maxBody))
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.seq++
		seq := s.seq
		s.mu.Unlock()
		meta := map[string]any{
			"http_remote": r.RemoteAddr,
			"http_path":   r.URL.Path,
		}
		emit(registry.Message{Raw: body, Meta: meta, SrcName: "http_server", SrcSeq: seq})
		w.WriteHeader(http.StatusAccepted)
	})
	s.server = &http.Server{Addr: s.listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutCtx)
	}()
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return
	}
}

func (s *httpServerSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	return nil, nil
}

func (s *httpServerSource) Close() error { return nil }

type httpSinkConfig struct {
	URL       string            `json:"url"`
	TimeoutMS int               `json:"timeout_ms" schema:"min=1,default=10000"`
	Headers   map[string]string `json:"headers" schema:"optional"`
}

func registerHTTPSink(reg *registry.Registry) error {
	return registry.RegisterSinkT(reg, "http", 1, func(c httpSinkConfig) (registry.Sink, error) {
		if !strings.Contains(c.URL, "://") {
			return nil, fmt.Errorf("http sink: url must be absolute")
		}
		headers := c.Headers
		if headers == nil {
			headers = map[string]string{}
		}
		return &httpSink{
			url:     c.URL,
			client:  &http.Client{Timeout: time.Duration(c.TimeoutMS) * time.Millisecond},
			headers: headers,
		}, nil
	})
}

// httpSink POSTs each message separately (per delivery policy retries are
// owned by the engine). Non-2xx responses are errors, which feed the delivery
// retry loop and eventually the dead letter queue.
type httpSink struct {
	url     string
	client  *http.Client
	headers map[string]string
}

func (s *httpSink) Write(ctx context.Context, msgs []registry.Message) error {
	for _, m := range msgs {
		body := encodedBytes(m)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(string(body)))
		if err != nil {
			return fmt.Errorf("http sink: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Eventboat-Message-Id", m.ID)
		for k, v := range s.headers {
			req.Header.Set(k, v)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return fmt.Errorf("http sink: %w", err)
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()              //nolint:errcheck
		if resp.StatusCode >= 300 {
			return &statusError{code: resp.StatusCode}
		}
	}
	return nil
}

func (s *httpSink) Close() error { return nil }

type statusError struct{ code int }

func (e *statusError) Error() string {
	return fmt.Sprintf("http sink: unexpected status %d", e.code)
}

// encodedBytes picks the final form of a message: the engine-encoded payload
// when present, else the original raw bytes.
func encodedBytes(m registry.Message) []byte {
	if len(m.Out) > 0 {
		return m.Out
	}
	return m.Raw
}

package httpsink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
)

func init() {
	registry.RegisterSink("http", func(id string, cfg map[string]any) (stage.Sink, error) {
		url := basestage.ConfigString(cfg, "url")
		if url == "" {
			return nil, fmt.Errorf("http sink: url is required")
		}
		method := basestage.ConfigString(cfg, "method")
		if method == "" {
			method = http.MethodPost
		}
		timeout := 30 * time.Second
		if ts := basestage.ConfigString(cfg, "timeout"); ts != "" {
			parsed, err := time.ParseDuration(ts)
			if err != nil || parsed <= 0 {
				return nil, fmt.Errorf("http sink: invalid timeout %q", ts)
			}
			timeout = parsed
		}
		return &Sink{
			Base:   basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "http"},
			url:    url,
			method: method,
			client: &http.Client{Timeout: timeout},
		}, nil
	})
}

type Sink struct {
	basestage.Base
	url    string
	method string
	client *http.Client
}

// Write posts each message independently: every message in the batch is
// attempted even when earlier ones fail, and the per-message failures are
// aggregated into a single error. Retries are left to the engine
// (writeWithRetry + DLQ); the sink only reports failures faithfully.
func (s *Sink) Write(ctx context.Context, msgs []*message.Message) error {
	var failed int
	var lastErr error
	for _, msg := range msgs {
		if err := s.writeOne(ctx, msg); err != nil {
			failed++
			lastErr = err
		}
	}
	if failed > 0 {
		return fmt.Errorf("http sink: %d/%d messages failed, last: %w", failed, len(msgs), lastErr)
	}
	return nil
}

func (s *Sink) writeOne(ctx context.Context, msg *message.Message) error {
	req, err := http.NewRequestWithContext(ctx, s.method, s.url, bytes.NewReader(msg.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType(msg))
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	// Drain before Close so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func contentType(msg *message.Message) string {
	if msg.ParsedCodec() == "raw" {
		return "application/octet-stream"
	}
	return "application/json"
}

func (s *Sink) Flush(context.Context) error { return nil }

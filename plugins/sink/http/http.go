package httpsink

import (
	"bytes"
	"context"
	"fmt"
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
		return &Sink{
			Base:   basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "http"},
			url:    url,
			method: method,
			client: &http.Client{Timeout: 30 * time.Second},
		}, nil
	})
}

type Sink struct {
	basestage.Base
	url    string
	method string
	client *http.Client
}

func (s *Sink) Write(ctx context.Context, msgs []*message.Message) error {
	for _, msg := range msgs {
		req, err := http.NewRequestWithContext(ctx, s.method, s.url, bytes.NewReader(msg.Payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("http sink: status %d", resp.StatusCode)
		}
	}
	return nil
}

func (s *Sink) Flush(context.Context) error { return nil }

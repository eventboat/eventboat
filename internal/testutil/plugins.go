package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
)

const (
	SourceTypeGenerator = "test_generator"
	SinkTypeCapture     = "test_capture"
)

var (
	capturesMu sync.RWMutex
	captures   = make(map[string]*CaptureSink)
)

// Register installs test-only source and sink plugins on the registry.
func Register(reg *registry.Registry) {
	if reg == nil {
		reg = registry.Default
	}
	reg.RegisterSource(SourceTypeGenerator, newGenerator)
	reg.RegisterSink(SinkTypeCapture, newCapture)
}

type GeneratorSource struct {
	basestage.Base
	messages [][]byte
}

func newGenerator(id string, cfg map[string]any) (stage.Source, error) {
	raw, ok := cfg["messages"].([]any)
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("test_generator: messages is required")
	}
	msgs := make([][]byte, 0, len(raw))
	for _, item := range raw {
		switch v := item.(type) {
		case string:
			msgs = append(msgs, []byte(v))
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("test_generator: marshal message: %w", err)
			}
			msgs = append(msgs, b)
		}
	}
	return &GeneratorSource{
		Base:     basestage.Base{IDVal: id, KindVal: stage.KindSource, TypeVal: SourceTypeGenerator},
		messages: msgs,
	}, nil
}

func (s *GeneratorSource) Consume(ctx context.Context, out chan<- *message.Message) error {
	for _, payload := range s.messages {
		msg := message.New(payload, nil)
		select {
		case out <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type CaptureSink struct {
	basestage.Base
	mu   sync.Mutex
	msgs []*message.Message
	done chan struct{}
}

func newCapture(id string, _ map[string]any) (stage.Sink, error) {
	sink := &CaptureSink{
		Base: basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: SinkTypeCapture},
		done: make(chan struct{}),
	}
	capturesMu.Lock()
	captures[id] = sink
	capturesMu.Unlock()
	return sink, nil
}

func (s *CaptureSink) Write(_ context.Context, msgs []*message.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		cp := m.ShallowCopy()
		s.msgs = append(s.msgs, cp)
	}
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}

func (s *CaptureSink) Flush(context.Context) error { return nil }

func (s *CaptureSink) Messages() []*message.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*message.Message, len(s.msgs))
	copy(out, s.msgs)
	return out
}

func (s *CaptureSink) Wait() <-chan struct{} { return s.done }

func CaptureSinkFor(id string) (*CaptureSink, bool) {
	capturesMu.RLock()
	defer capturesMu.RUnlock()
	s, ok := captures[id]
	return s, ok
}

func ResetCaptures() {
	capturesMu.Lock()
	defer capturesMu.Unlock()
	captures = make(map[string]*CaptureSink)
}

package builtin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/eventboat/eventboat/internal/registry"
)

type cronSourceConfig struct {
	Expression string `json:"expression" schema:"desc=cron expression (5-field, standard)"`
	Payload    string `json:"payload" schema:"default={},desc=raw payload emitted at each tick"`
}

func registerCronSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "cron", 1, nil, func(c cronSourceConfig) (registry.Source, error) {
		if _, err := cron.ParseStandard(c.Expression); err != nil {
			return nil, fmt.Errorf("cron source: invalid expression: %w", err)
		}
		return &cronSource{expr: c.Expression, payload: []byte(c.Payload)}, nil
	})
}

// cronSource emits a fixed payload on a cron schedule. Ticks carry no offset
// to commit: the spool is the truth (redesign-v3.md §6.2).
type cronSource struct {
	expr    string
	payload []byte

	mu     sync.Mutex
	seq    int64
	closed bool
}

func (s *cronSource) Init(state []byte) error { return nil }

func (s *cronSource) Run(ctx context.Context, emit func(registry.Message)) {
	if _, err := cron.ParseStandard(s.expr); err != nil {
		return
	}
	// A cron source has no deterministic replayable offset; we schedule on the
	// wall clock and let the spool provide durability once a tick is emitted.
	sched := cron.New()
	_, _ = sched.AddFunc(s.expr, func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.seq++
		seq := s.seq
		s.mu.Unlock()
		meta := map[string]any{"scheduled_time": time.Now().UTC().Format(time.RFC3339Nano)}
		emit(registry.Message{Raw: s.payload, Meta: meta, SrcName: "cron", SrcSeq: seq})
	})
	go sched.Run()
	<-ctx.Done()
	sched.Stop()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *cronSource) Settled(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	return nil, nil
}

func (s *cronSource) Close() error { return nil }

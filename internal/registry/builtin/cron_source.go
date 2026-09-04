package builtin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/eventboat/eventboat/internal/registry"
)

const cronSourceSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["expression"],
  "properties": {
    "expression": { "type": "string", "description": "cron expression (5-field, standard)" },
    "payload":    { "type": "string", "description": "raw payload emitted at each tick", "default": "{}" }
  },
  "additionalProperties": false
}`

func registerCronSource(reg *registry.Registry) error {
	return reg.RegisterSource("cron", 1, cronSourceSchema, nil, func(cfg map[string]any) (registry.Source, error) {
		expr, _ := cfg["expression"].(string)
		if _, err := cron.ParseStandard(expr); err != nil {
			return nil, fmt.Errorf("cron source: invalid expression: %w", err)
		}
		payload, _ := cfg["payload"].(string)
		if payload == "" {
			payload = "{}"
		}
		return &cronSource{expr: expr, payload: []byte(payload)}, nil
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

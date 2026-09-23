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

	mu      sync.Mutex
	seq     int64
	closed  bool
	emitErr error // first refusal, reported as a failed source

	stopClosed bool
	stop       chan struct{}
}

func (s *cronSource) Init(state []byte) error { return nil }

func (s *cronSource) Run(ctx context.Context, emit func(registry.Message) error) error {
	if _, err := cron.ParseStandard(s.expr); err != nil {
		return fmt.Errorf("cron source: invalid expression: %w", err)
	}
	s.mu.Lock()
	s.stop = make(chan struct{})
	s.stopClosed = false
	s.mu.Unlock()
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
		if err := emit(registry.Message{Raw: s.payload, Meta: meta, SrcName: "cron", SrcSeq: seq}); err != nil {
			// Refusal: stop scheduling and report the source failed. The
			// callback runs on the scheduler goroutine, so the error is
			// stashed and Run is woken through stop.
			s.mu.Lock()
			if s.emitErr == nil {
				s.emitErr = err
			}
			first := !s.stopClosed
			s.stopClosed = true
			s.mu.Unlock()
			if first {
				close(s.stop)
			}
		}
	})
	go sched.Run()
	select {
	case <-ctx.Done():
		sched.Stop()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		return nil // cancelled: a voluntary stop, not a failure
	case <-s.stop:
		sched.Stop()
		s.mu.Lock()
		s.closed = true
		err := s.emitErr
		s.mu.Unlock()
		if ctx.Err() != nil {
			return nil // engine shutdown under the emit: voluntary stop
		}
		return err
	}
}

func (s *cronSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	return nil, nil
}

func (s *cronSource) Close() error { return nil }

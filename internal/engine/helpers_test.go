package engine

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// memSink is an in-memory sink with fault/block injection. Block gates hang
// until released; they intentionally ignore ctx so tests can freeze a write
// in place (simulating a crash mid-delivery).
type memSink struct {
	mu       sync.Mutex
	id       string
	writes   int
	out      []registry.Message
	fail     func(attempt int) error
	block    func(attempt int) (<-chan struct{}, bool)
	failures int
}

func (s *memSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.mu.Lock()
	s.writes++
	n := s.writes
	s.mu.Unlock()
	if s.block != nil {
		if ch, ok := s.block(n); ok {
			<-ch // deliberate: not cancellable, models a wedged writer
		}
	}
	if s.fail != nil {
		if err := s.fail(n); err != nil {
			s.mu.Lock()
			s.failures++
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Lock()
	s.out = append(s.out, msgs...)
	s.mu.Unlock()
	return nil
}

func (s *memSink) Close() error { return nil }

func (s *memSink) snapshot() (delivered []registry.Message, writes int, failures int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]registry.Message(nil), s.out...), s.writes, s.failures
}

// harness wires test plugins (manual sources, mem sinks) into a fresh
// registry and builds pipelines from YAML.
type harness struct {
	t       testing.TB
	reg     *registry.Registry
	mu      sync.Mutex
	sources map[string]*testkit.ManualSource
	sinks   map[string]*memSink
}

const manualSchema = `{"type":"object","properties":{"id":{"type":"string"}},"additionalProperties":false}`
const memSchema = manualSchema

func newHarness(t testing.TB) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		reg:     registry.New(),
		sources: map[string]*testkit.ManualSource{},
		sinks:   map[string]*memSink{},
	}
	if err := builtin.RegisterAll(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSource("manual", 1, manualSchema, nil, func(cfg map[string]any) (registry.Source, error) {
		id, _ := cfg["id"].(string)
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.sources[id]; ok {
			return s, nil
		}
		s := testkit.NewManualSource()
		s.Name = id
		h.sources[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSink("mem", 1, memSchema, func(cfg map[string]any) (registry.Sink, error) {
		id, _ := cfg["id"].(string)
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.sinks[id]; ok {
			return s, nil
		}
		s := &memSink{id: id}
		h.sinks[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) source(id string) *testkit.ManualSource {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sources[id]
}

func (h *harness) sink(id string) *memSink {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sinks[id]
}

func (h *harness) build(yamlText string) *ir.Pipeline {
	h.t.Helper()
	lr := config.LoadBytes("test.yaml", []byte(yamlText))
	if lr.HasErrors() {
		h.t.Fatalf("config errors:\n%+v", lr.Diagnostics)
	}
	pip, diags := ir.Build(lr.Pipeline, h.reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		h.t.Fatalf("ir build errors:\n%+v", diags)
	}
	return pip
}

func fastOptions() Options {
	o := DefaultOptions()
	o.Clock = testkit.FixedClock(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC))
	o.NewID = testkit.CounterID()
	o.BackoffBase = time.Millisecond
	o.DLBackoff = time.Millisecond
	o.BatchFlush = 10 * time.Millisecond
	o.DefaultTimeout = 2 * time.Second
	return o
}

// runEngine starts an engine and registers cleanup. The returned stop
// function cancels and waits for shutdown; stop old engines before starting a
// new one on the same harness (test plugin instances are shared).
func runEngine(t testing.TB, pip *ir.Pipeline, st store.Store, reg *registry.Registry, opts Options) (*Engine, func()) {
	t.Helper()
	if opts.SinkWrapper == nil && opts.HighWatermark == 0 {
		opts = fastOptions()
	}
	eng, err := New(pip, st, reg, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		if err := eng.Run(ctx); err != nil {
			t.Errorf("engine %q Run: %v", pip.Config.Name, err)
		}
		done <- nil
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				// Deliberately tolerated: tests with wedged (non-cancellable)
				// sinks model crash-style shutdowns and drain slowly.
			}
		})
	}
	t.Cleanup(stop)
	waitReady(t, eng)
	return eng, stop
}

func waitReady(t testing.TB, eng *Engine) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if eng.Ready() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("engine not ready")
}

func waitCommit(t *testing.T, eng *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.WaitCommit(ctx); err != nil {
		t.Fatal(err)
	}
}

func decodeJSON(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("bad json %q: %v", raw, err)
	}
	return m
}

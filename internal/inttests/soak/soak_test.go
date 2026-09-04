// Package soak is the long-run stability gate (redesign-v3-review-beta.md
// R-B8): mixed pipelines under load with random fault injection for a
// configurable duration, asserting the reliability invariants and no
// goroutine leaks at the end. Env-gated: EVENTBOAT_SOAK_TEST=1 (the CI soak
// workflow sets it; EVENTBOAT_SOAK_DURATION bounds the run, default 25m).
package soak

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// recordingSink is a local capture sink (the engine test harness's memSink
// shape, kept self-contained here).
type recordingSink struct {
	mu sync.Mutex
	n  int64
}

func (s *recordingSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.mu.Lock()
	s.n += int64(len(msgs))
	s.mu.Unlock()
	return nil
}
func (s *recordingSink) Close() error { return nil }

func registerTestPlugins(t *testing.T, reg *registry.Registry) {
	t.Helper()
	const schema = `{"type":"object","properties":{"id":{"type":"string"}},"additionalProperties":false}`
	if err := reg.RegisterSource("manual", 1, schema, nil, func(cfg map[string]any) (registry.Source, error) {
		s := testkit.NewManualSource()
		if id, ok := cfg["id"].(string); ok {
			s.Name = id
		}
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterSink("memsink", 1, schema, func(cfg map[string]any) (registry.Sink, error) {
		return &recordingSink{}, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func buildPipeline(t *testing.T, reg *registry.Registry, name, yamlText string) *ir.Pipeline {
	t.Helper()
	lr := config.LoadBytes(name+".yaml", []byte(yamlText))
	if lr.HasErrors() {
		t.Fatal(lr.Diagnostics)
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatal(diags)
	}
	return pip
}

// TestSoakMixedLoadWithFaults drives two pipelines for the configured
// duration: a continuous fan-out under transient spool/DLQ store faults and
// a script-failure pipeline feeding the dead-letter path. At the end every
// injected message must have settled exactly once, the checkpoint must
// cover the full prefix, and no goroutines may remain.
func TestSoakMixedLoadWithFaults(t *testing.T) {
	if os.Getenv("EVENTBOAT_SOAK_TEST") != "1" {
		t.Skip("set EVENTBOAT_SOAK_TEST=1 (CI soak workflow; minutes-class run)")
	}
	duration := 25 * time.Minute
	if d := os.Getenv("EVENTBOAT_SOAK_DURATION"); d != "" {
		parsed, err := time.ParseDuration(d)
		if err != nil || parsed < time.Minute {
			t.Fatalf("bad EVENTBOAT_SOAK_DURATION %q", d)
		}
		duration = parsed
	}
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	registerTestPlugins(t, reg)

	opts := engine.DefaultOptions()
	opts.BatchFlush = 50 * time.Millisecond
	opts.BackoffBase = 5 * time.Millisecond
	opts.DLBackoff = 10 * time.Millisecond
	opts.DrainTimeout = 5 * time.Second

	// Pipeline 1: continuous fan-out; the store wrapper injects transient
	// spool-append faults (the retry path must absorb them; the injection is
	// refused and the driver retries — never lost).
	var spoolFaults, dlFaults atomic.Int64
	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory("soak-fan")}
	wrapped.AppendHook = func(m registry.Message) error {
		if rand.IntN(200) == 0 {
			spoolFaults.Add(1)
			return errors.New("disk hiccup")
		}
		return nil
	}
	eng1, stop1 := mustRun(t, reg, wrapped, opts, buildPipeline(t, reg, "fan", `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: soak-fan }
edge_defaults:
  delivery: { retries: 2, backoff: exponential }
limits: { max_in_flight: 512, drain_timeout: 5s }
sources:
  in:
    decoder: json
    manual: { id: soak-fan }
transforms:
  enrich:
    from: [in]
    script: |
      payload.seen = True
sinks:
  out:
    from: [enrich]
    encoder: json
    memsink: { id: soak-out }
`))

	// Pipeline 2: every 10th message fails its script → dead letter against
	// the real SQLite store, wrapped with transient DLQ-write faults (the
	// settle must block and retry — never settle through a failed write).
	st2raw, err := store.OpenSQLite(t.TempDir() + "/soak-dlq.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2raw.Close() }()
	st2 := &testkit.StoreWrapper{Inner: st2raw}
	st2.DeadLetterHook = func(dl store.DeadLetter) error {
		if rand.IntN(4) == 0 {
			dlFaults.Add(1)
			return errors.New("dlq busy")
		}
		return nil
	}
	eng2, stop2 := mustRun(t, reg, st2, opts, buildPipeline(t, reg, "dlq", `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: soak-dlq }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: soak-dlq }
transforms:
  bomb:
    from: [in]
    script: |
      if payload.i % 10 == 9:
          fail("boom %d" % payload.i)
sinks:
  out:
    from: [bomb]
    encoder: json
    memsink: { id: soak-dlq-out }
`))

	baseline := runtime.NumGoroutine()
	deadline := time.Now().Add(duration)
	var injected1, injected2 atomic.Int64
	drive := func(eng *engine.Engine, counter *atomic.Int64, every time.Duration) (done chan struct{}) {
		done = make(chan struct{})
		go func() {
			defer close(done)
			i := 0
			for time.Now().Before(deadline) {
				if _, err := eng.InjectAt("in", []byte(fmt.Sprintf(`{"i":%d}`, i)), nil); err == nil {
					counter.Add(1)
				}
				i++
				time.Sleep(every)
			}
		}()
		return done
	}
	d1 := drive(eng1, &injected1, 2*time.Millisecond)
	d2 := drive(eng2, &injected2, 5*time.Millisecond)
	<-d1
	<-d2

	settleCtx, cancelSettle := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelSettle()
	if err := eng1.WaitSettled(settleCtx); err != nil {
		t.Fatalf("soak-fan did not settle: %v", err)
	}
	if err := eng2.WaitSettled(settleCtx); err != nil {
		t.Fatalf("soak-dlq did not settle: %v", err)
	}
	stop1()
	stop2()

	// At-least-once, exactly-once-per-injection: both engines settle every
	// accepted message (spool-faulted injections are refused and retried by
	// the driver, counted only on success).
	if got, want := eng1.Metrics.SettledCount.Load(), injected1.Load(); got != want {
		t.Errorf("soak-fan settled %d, injected %d", got, want)
	}
	if got, want := eng2.Metrics.SettledCount.Load(), injected2.Load(); got != want {
		t.Errorf("soak-dlq settled %d, injected %d", got, want)
	}
	if _, through, _ := eng1.SettleSnapshot(); through != injected1.Load() {
		t.Errorf("soak-fan checkpoint %d, want %d", through, injected1.Load())
	}
	if dl := eng2.Metrics.DeadLettered.Load(); dl != injected2.Load()/10 {
		t.Errorf("soak-dlq dead letters %d, want %d", dl, injected2.Load()/10)
	}
	t.Logf("soak done (%s): injected %d + %d, spoolFaults=%d dlqFaults=%d, goroutines %d -> %d",
		duration, injected1.Load(), injected2.Load(), spoolFaults.Load(), dlFaults.Load(),
		baseline, runtime.NumGoroutine())

	// No goroutine leak (workers, drivers and sources all exited).
	time.Sleep(2 * time.Second)
	if delta := runtime.NumGoroutine() - baseline; delta > 20 {
		t.Errorf("goroutine leak: baseline %d, now %d (+%d)", baseline, runtime.NumGoroutine(), delta)
	}
}

func mustRun(t *testing.T, reg *registry.Registry, st store.Store, opts engine.Options, pip *ir.Pipeline) (*engine.Engine, func()) {
	t.Helper()
	eng, err := engine.New(pip, st, reg, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = eng.Run(ctx); close(done) }()
	for i := 0; i < 500 && !eng.Ready(); i++ {
		time.Sleep(2 * time.Millisecond)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
		}
	}
	t.Cleanup(stop)
	return eng, stop
}

package engine

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// Third-party transform plugins ride the same generic path as the builtins:
// registry instantiation, delivery retries per the incoming edge, and the
// 1→0 filter contract (commit-as-filtered + NoMatch, like a zero-match edge).
func TestEnginePluginTransformFilters(t *testing.T) {
	h := newHarness(t)
	if err := testkit.RegisterFakeTransform(h.reg, "sieve", func(msg *registry.Message) ([]*registry.Message, error) {
		if m, ok := msg.Decoded.(map[string]any); ok && m["keep"] == true {
			return []*registry.Message{msg}, nil
		}
		return nil, nil // 1→0: filtered
	}); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: sieve }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    from: [in]
    sieve: {}
sinks:
  out: { from: [t], mem: { id: out } }
`)
	st := store.NewMemory("sieve")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"keep": false}`), "")
	h.source("in").Emit([]byte(`{"keep": true}`), "")
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 1 {
		t.Fatalf("delivered = %d messages, want 1 (the other filtered)", len(delivered))
	}
	if got := decodeJSON(t, delivered[0].Out)["keep"]; got != true {
		t.Errorf("delivered payload = %v", delivered[0])
	}
	if got := eng.Metrics.NoMatch.Load(); got != 1 {
		t.Errorf("NoMatch = %d, want 1", got)
	}
}

// A plugin returning N outputs gets the split contract: children share the
// parent's identity and the commit accounting expands for the extra branches.
func TestEnginePluginTransformExpands(t *testing.T) {
	h := newHarness(t)
	if err := testkit.RegisterFakeTransform(h.reg, "triple", func(msg *registry.Message) ([]*registry.Message, error) {
		out := make([]*registry.Message, 3)
		for i := range out {
			child := *msg
			child.Decoded = map[string]any{"i": i}
			out[i] = &child
		}
		return out, nil
	}); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: triple }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    from: [in]
    triple: {}
sinks:
  out: { from: [t], mem: { id: out } }
`)
	st := store.NewMemory("triple")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"x":1}`), "")
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 3 {
		t.Fatalf("delivered = %d messages, want 3", len(delivered))
	}
	ids := map[string]bool{}
	for _, m := range delivered {
		ids[m.ID] = true
	}
	if len(ids) != 1 {
		t.Errorf("children must share the parent's message_id, got %v", ids)
	}
}

func TestEnginePluginTransformErrorRetriesThenDeadLetters(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	if err := testkit.RegisterFakeTransform(h.reg, "flaky", func(msg *registry.Message) ([]*registry.Message, error) {
		calls.Add(1)
		return nil, &registry.TransformError{Err: errString("boom"), Flavor: "flaky"}
	}); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: flaky }
edge_defaults:
  delivery: { retries: 1, backoff: constant }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    from: [in]
    flaky: {}
sinks:
  out: { from: [t], mem: { id: out } }
`)
	st := store.NewMemory("flaky")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"a":1}`), "")
	waitCommit(t, eng)

	dls, _ := st.DeadLetters("flaky")
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d", len(dls))
	}
	if !strings.Contains(dls[0].Reason, "flaky: boom") {
		t.Errorf("reason = %q, want plugin-name prefix", dls[0].Reason)
	}
	if got := calls.Load(); got != 2 { // 1 attempt + 1 retry
		t.Errorf("apply calls = %d, want 2", got)
	}
	if got := eng.Metrics.Retries.Load(); got != 1 {
		t.Errorf("retries = %d, want 1", got)
	}
}

// cloneFailTransform implements TransformCloner with an always-failing Clone.
type cloneFailTransform struct{ applies *atomic.Int32 }

func (c *cloneFailTransform) Init(env *registry.TransformEnv) error { return nil }
func (c *cloneFailTransform) Apply(msg *registry.Message) ([]*registry.Message, error) {
	c.applies.Add(1)
	return []*registry.Message{msg}, nil
}
func (c *cloneFailTransform) Close() error                       { return nil }
func (c *cloneFailTransform) Clone() (registry.Transform, error) { return nil, errString("clone boom") }

// A failed Clone is worker-fatal: the plugin implemented TransformCloner
// precisely because its instance is not goroutine-safe (wasm module
// instances), so the engine must shut down with the error instead of
// silently racing `workers` goroutines on the shared master.
func TestEngineTransformCloneFailureFailsPipeline(t *testing.T) {
	h := newHarness(t)
	var applies atomic.Int32
	fake := &cloneFailTransform{applies: &applies}
	if err := h.reg.RegisterTransform("cloner", 1,
		`{"type": ["object", "array", "string", "integer", "number", "boolean", "null"]}`, nil,
		func(cfg any, dir string) (registry.Transform, error) { return fake, nil }); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: cloner }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    from: [in]
    workers: 3
    cloner: {}
sinks:
  out: { from: [t], mem: { id: out } }
`)
	st := store.NewMemory("cloner")
	eng, err := New(pip, st, h.reg, fastOptions())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "clone boom") || !strings.Contains(err.Error(), `"t"`) {
			t.Fatalf("Run = %v, want node-annotated clone failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("engine kept running after a failed transform clone")
	}
	if got := applies.Load(); got != 0 {
		t.Errorf("Apply ran %d times on the shared master instance, want 0", got)
	}
}

// errString is declared in engine_test.go and shared here.

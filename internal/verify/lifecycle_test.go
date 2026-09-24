package verify

import (
	"sync/atomic"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/wasmhost"
)

// lifecycleTransform counts Init/Close so the composition's instance
// lifecycle is observable (candidate 09).
type lifecycleTransform struct {
	init  *atomic.Int32
	close *atomic.Int32
}

func (t *lifecycleTransform) Init(*registry.TransformEnv) error { t.init.Add(1); return nil }
func (t *lifecycleTransform) Apply(m *registry.Message) ([]*registry.Message, error) {
	return []*registry.Message{m}, nil
}
func (t *lifecycleTransform) Close() error { t.close.Add(1); return nil }

const lifecyclePipeline = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: verify-lifecycle }
sources:
  in: { file: { path: input.jsonl, on_eof: stop } }
transforms:
  t: { depends_on: [in], counter: {} }
sinks:
  out: { depends_on: [t], debug: {} }
`

// lifecycleRegistry registers one explain-safe counting transform.
func lifecycleRegistry(t *testing.T) (*registry.Registry, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	reg := testRegistry(t)
	var init, close atomic.Int32
	if err := reg.RegisterTransform("counter", 1, `{"type":"object","additionalProperties":false}`, []string{"explain-safe"},
		func(cfg any, dir string) (registry.Transform, error) {
			return &lifecycleTransform{init: &init, close: &close}, nil
		}); err != nil {
		t.Fatal(err)
	}
	return reg, &init, &close
}

// Verify-only retains nothing: the instance is closed before Result returns,
// and the pipeline Close is a no-op.
func TestVerifyOnlyRetainsNoInstances(t *testing.T) {
	reg, initCount, closeCount := lifecycleRegistry(t)
	res := Bytes("p.yaml", []byte(lifecyclePipeline), "", reg, Options{})
	if res.Pipeline == nil {
		t.Fatalf("verify: %+v", res.Diagnostics)
	}
	if initCount.Load() != 1 || closeCount.Load() != 1 {
		t.Fatalf("Init=%d Close=%d, want 1/1", initCount.Load(), closeCount.Load())
	}
	if res.Pipeline.Nodes["t"].Transform != nil {
		t.Fatal("verify-only Result must not retain the instance")
	}
	if err := res.Pipeline.Close(); err != nil {
		t.Fatal(err)
	}
	if closeCount.Load() != 1 {
		t.Fatalf("Close on a verify-only pipeline must be a no-op, got %d", closeCount.Load())
	}
}

// The ForExplain lifecycle retains the explain-safe instance and the caller
// closes it; a failing ForExplain verify leaks nothing.
func TestForExplainRetainsAndCloses(t *testing.T) {
	reg, initCount, closeCount := lifecycleRegistry(t)
	res := Bytes("p.yaml", []byte(lifecyclePipeline), "", reg, Options{ForExplain: true})
	if res.Pipeline == nil {
		t.Fatalf("verify: %+v", res.Diagnostics)
	}
	if initCount.Load() != 1 || closeCount.Load() != 0 {
		t.Fatalf("Init=%d Close=%d, want 1/0 before Close", initCount.Load(), closeCount.Load())
	}
	if res.Pipeline.Nodes["t"].Transform == nil {
		t.Fatal("ForExplain verify must retain the explain-safe instance")
	}
	if err := res.Pipeline.Close(); err != nil {
		t.Fatal(err)
	}
	if closeCount.Load() != 1 {
		t.Fatalf("Close called %d times, want 1", closeCount.Load())
	}

	bad := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: verify-lifecycle-bad }
sources:
  in: { file: { path: input.jsonl, on_eof: stop } }
transforms:
  t: { depends_on: [in], counter: {} }
sinks:
  out: { depends_on: [t], encoder: nosuchcodec, debug: {} }
`
	reg2, init2, close2 := lifecycleRegistry(t)
	res2 := Bytes("p.yaml", []byte(bad), "", reg2, Options{ForExplain: true})
	if res2.Pipeline != nil {
		t.Fatal("verify with an unknown encoder must fail")
	}
	if init2.Load() != 1 {
		t.Fatalf("Init=%d, want 1", init2.Load())
	}
	if got := close2.Load(); got != 1 {
		t.Fatalf("failed ForExplain verify leaked the instance: Close=%d, want 1", got)
	}
}

// The LSP keystroke case: a repeated verify of the same document reuses the
// compiled wasm module (only the first verify compiles).
func TestSecondVerifyHitsWasmCache(t *testing.T) {
	reg := testRegistry(t)
	pipeline := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: wasm-cache }
sources:
  in: { file: { path: input.jsonl, on_eof: stop } }
transforms:
  t: { depends_on: [in], wasm: { module: "../wasmhost/testdata/aggregate.wasm" } }
sinks:
  out: { depends_on: [t], debug: {} }
`
	before := wasmhost.CacheStats()
	if res := Bytes("p.yaml", []byte(pipeline), "", reg, Options{}); res.Pipeline == nil {
		t.Fatalf("first verify: %+v", res.Diagnostics)
	}
	afterFirst := wasmhost.CacheStats()
	if got := afterFirst.Misses - before.Misses; got != 1 {
		t.Fatalf("first verify compiled %d modules, want exactly 1", got)
	}
	if res := Bytes("p.yaml", []byte(pipeline), "", reg, Options{}); res.Pipeline == nil {
		t.Fatalf("second verify: %+v", res.Diagnostics)
	}
	afterSecond := wasmhost.CacheStats()
	if afterSecond.Misses != afterFirst.Misses {
		t.Fatalf("second verify recompiled (%d -> %d misses)", afterFirst.Misses, afterSecond.Misses)
	}
	if afterSecond.Hits <= afterFirst.Hits {
		t.Fatalf("second verify did not hit the cache (%d -> %d hits)", afterFirst.Hits, afterSecond.Hits)
	}
}

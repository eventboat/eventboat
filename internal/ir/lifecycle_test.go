package ir

import (
	"sync/atomic"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
)

// lifecycleTransform counts Init/Close so the tests can pin the instance
// lifecycle (candidate 09).
type lifecycleTransform struct {
	init  *atomic.Int32
	close *atomic.Int32
}

func (t *lifecycleTransform) Init(*registry.TransformEnv) error { t.init.Add(1); return nil }
func (t *lifecycleTransform) Apply(m *registry.Message) ([]*registry.Message, error) {
	return []*registry.Message{m}, nil
}
func (t *lifecycleTransform) Close() error { t.close.Add(1); return nil }

const lifecycleSchema = `{"type":"object","additionalProperties":false}`

// lifecycleRegistry registers one explain-safe counting transform.
func lifecycleRegistry(t *testing.T) (*registry.Registry, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	reg := testReg(t)
	var init, close atomic.Int32
	if err := reg.RegisterTransform("counter", 1, lifecycleSchema, []string{"explain-safe"},
		func(cfg any, dir string) (registry.Transform, error) {
			return &lifecycleTransform{init: &init, close: &close}, nil
		}); err != nil {
		t.Fatal(err)
	}
	return reg, &init, &close
}

func loadLifecycleConfig(t *testing.T, yamlText string) *config.Pipeline {
	t.Helper()
	lr := config.LoadBytes("lifecycle.yaml", []byte(yamlText))
	if lr.Diagnostics.HasErrors() {
		t.Fatalf("load: %+v", lr.Diagnostics)
	}
	return lr.Pipeline
}

const lifecyclePipeline = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lifecycle }
sources:
  in: { file: { path: in.jsonl } }
transforms:
  t: { depends_on: [in], counter: {} }
sinks:
  out: { depends_on: [t], debug: {} }
`

// Verify-only: the instance is validated (Init) and closed immediately; the
// pipeline retains nothing.
func TestBuildVerifyOnlyClosesImmediately(t *testing.T) {
	reg, initCount, closeCount := lifecycleRegistry(t)
	pip, diags := Build(loadLifecycleConfig(t, lifecyclePipeline), reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("build: %+v", diags)
	}
	if got := initCount.Load(); got != 1 {
		t.Errorf("Init called %d times, want 1", got)
	}
	if got := closeCount.Load(); got != 1 {
		t.Errorf("Close called %d times, want 1 (verify-only must close immediately)", got)
	}
	if pip.Nodes["t"].Transform != nil {
		t.Error("verify-only pipeline must not retain the instance")
	}
	if err := pip.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := closeCount.Load(); got != 1 {
		t.Errorf("Close on a verify-only pipeline must be a no-op, got %d", got)
	}
}

// The retaining mode keeps the instance for explain; Pipeline.Close releases
// it exactly once.
func TestBuildForExplainRetainsUntilClose(t *testing.T) {
	reg, initCount, closeCount := lifecycleRegistry(t)
	pip, diags := BuildForExplain(loadLifecycleConfig(t, lifecyclePipeline), reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("build: %+v", diags)
	}
	if initCount.Load() != 1 || closeCount.Load() != 0 {
		t.Fatalf("Init=%d Close=%d, want 1/0 before Pipeline.Close", initCount.Load(), closeCount.Load())
	}
	if pip.Nodes["t"].Transform == nil {
		t.Fatal("retaining build must keep the explain-safe instance")
	}
	if err := pip.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := closeCount.Load(); got != 1 {
		t.Errorf("Close called %d times, want 1", got)
	}
	if pip.Nodes["t"].Transform != nil {
		t.Error("Close must drop the retained instance")
	}
}

// A retaining build that fails returns nil and closes what it had retained:
// the error path leaks nothing. A verify-only failure path is safe by
// construction (instances close as soon as they are validated) and pinned
// here too.
func TestBuildForExplainFailureClosesRetained(t *testing.T) {
	reg, initCount, closeCount := lifecycleRegistry(t)
	bad := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lifecycle-bad }
sources:
  in: { file: { path: in.jsonl } }
transforms:
  t: { depends_on: [in], counter: {} }
sinks:
  out: { depends_on: [t], encoder: nosuchcodec, debug: {} }
`
	pip, diags := BuildForExplain(loadLifecycleConfig(t, bad), reg, starhost.DefaultOptions(), nil)
	if pip != nil {
		t.Fatal("build with an unknown encoder must fail")
	}
	if !hasCode(diags, "codec_unknown") {
		t.Fatalf("want codec_unknown, got %+v", diags)
	}
	if initCount.Load() != 1 {
		t.Errorf("Init called %d times, want 1", initCount.Load())
	}
	if got := closeCount.Load(); got != 1 {
		t.Errorf("failed retaining build leaked the instance: Close called %d times, want 1", got)
	}

	reg2, init2, close2 := lifecycleRegistry(t)
	if pip, _ := Build(loadLifecycleConfig(t, bad), reg2, starhost.DefaultOptions(), nil); pip != nil {
		t.Fatal("verify-only build with an unknown encoder must fail")
	}
	if init2.Load() != 1 || close2.Load() != 1 {
		t.Errorf("verify-only failure path: Init=%d Close=%d, want 1/1", init2.Load(), close2.Load())
	}
}

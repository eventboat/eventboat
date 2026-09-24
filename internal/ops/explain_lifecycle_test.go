package ops

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

// explainLifecycleTransform counts Init/Close for the explain entry's
// instance lifecycle.
type explainLifecycleTransform struct {
	init  *atomic.Int32
	close *atomic.Int32
}

func (t *explainLifecycleTransform) Init(*registry.TransformEnv) error { t.init.Add(1); return nil }
func (t *explainLifecycleTransform) Apply(m *registry.Message) ([]*registry.Message, error) {
	return []*registry.Message{m}, nil
}
func (t *explainLifecycleTransform) Close() error { t.close.Add(1); return nil }

// Candidate 09 acceptance 3 (explain half): the content explain entry builds
// in the retaining lifecycle and closes the pipeline it built.
func TestExplainContentClosesRetainedInstances(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	var init, close atomic.Int32
	if err := reg.RegisterTransform("counter", 1, `{"type":"object","additionalProperties":false}`, []string{"explain-safe"},
		func(cfg any, dir string) (registry.Transform, error) {
			return &explainLifecycleTransform{init: &init, close: &close}, nil
		}); err != nil {
		t.Fatal(err)
	}

	out, err := ExplainContent(reg, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: explain-lifecycle }
sources:
  in: { file: { path: input.jsonl } }
transforms:
  t: { depends_on: [in], counter: {} }
sinks:
  out: { depends_on: [t], debug: {} }
`, "", ExplainRequest{Message: []byte(`{"x": 1}`)})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !strings.Contains(out, "t: transform.counter") {
		t.Fatalf("explain did not dry-run the explain-safe transform:\n%s", out)
	}
	if got := init.Load(); got != 1 {
		t.Fatalf("Init=%d, want 1", got)
	}
	if got := close.Load(); got != 1 {
		t.Fatalf("ExplainContent must close the pipeline it built: Close=%d, want 1", got)
	}
}

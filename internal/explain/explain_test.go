package explain

import (
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/testkit"
)

func buildPipeline(t *testing.T, yamlText string) *ir.Pipeline {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(reg); err != nil {
		t.Fatal(err)
	}
	lr := config.LoadBytes("p.yaml", []byte(yamlText))
	if lr.HasErrors() {
		t.Fatalf("config: %+v", lr.Diagnostics)
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("ir: %+v", diags)
	}
	return pip
}

const branchingYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: eb }
constants:
  vip_threshold: 100
sources:
  ingest:
    decoder: json
    file: { path: in.jsonl }
transforms:
  enrich:
    from: [ingest]
    script: |
      payload.total = payload.price * payload.qty
      if payload.total > constants.vip_threshold:
          meta.tier = "vip"
      else:
          meta.tier = "basic"
sinks:
  eu-out:
    from: { enrich: { when: 'meta.tier == "vip"' } }
    file: { path: eu.jsonl }
  us-out:
    from: [enrich]
    file: { path: us.jsonl }
`

// Message-level explain: the script dry-runs (the sandbox is deterministic),
// and downstream CEL sees the TRANSFORMED payload — the vip message matches
// the tier condition the script itself assigned.
func TestTraceMessageLevel(t *testing.T) {
	pip := buildPipeline(t, branchingYAML)
	out, err := Trace(pip, Options{Message: []byte(`{"price": 60, "qty": 3}`)})
	if err != nil && out == "" {
		t.Fatal(err)
	}
	if !strings.Contains(out, `enters at node "ingest"`) {
		t.Errorf("trace header missing:\n%s", out)
	}
	if !strings.Contains(out, "enrich: transform.script") || !strings.Contains(out, "✓") {
		t.Errorf("script dry-run not reported:\n%s", out)
	}
	if !strings.Contains(out, `enrich → eu-out  when meta.tier == "vip"`) || !strings.Contains(out, "✓ MATCH") {
		t.Errorf("vip branch not matched (script output must feed the CEL eval):\n%s", out)
	}
	if !strings.Contains(out, "us-out") || !strings.Contains(out, "always") {
		t.Errorf("unconditional branch not marked always:\n%s", out)
	}

	// Below threshold: eu-out does not match; us-out (unconditional) still takes it.
	out, _ = Trace(pip, Options{Message: []byte(`{"price": 10, "qty": 2}`)})
	if !strings.Contains(out, "✗ no match") {
		t.Errorf("non-vip branch should not match:\n%s", out)
	}
}

// A failing script IS the explanation: the trace shows the backtrace and
// stops (exactly what production would do with the same input).
func TestTraceScriptFailureShown(t *testing.T) {
	pip := buildPipeline(t, `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: boom }
sources:
  in:
    decoder: json
    file: { path: a }
transforms:
  t:
    from: [in]
    script: |
      payload.x = 1
      fail("kaboom")
sinks:
  out: { from: [t], file: { path: o } }
`)
	out, _ := Trace(pip, Options{Message: []byte(`{}`)})
	if !strings.Contains(out, "✗ script failed") || !strings.Contains(out, "kaboom") {
		t.Errorf("script failure not rendered:\n%s", out)
	}
	if !strings.Contains(out, "dead-letter") {
		t.Errorf("dead-letter consequence missing:\n%s", out)
	}
}

// Symbolic mode never executes anything: script summary + condition texts.
func TestTraceSymbolic(t *testing.T) {
	pip := buildPipeline(t, branchingYAML)
	out, err := Trace(pip, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "symbolic trace") || !strings.Contains(out, "statements, budget=") {
		t.Errorf("symbolic summary missing:\n%s", out)
	}
	if strings.Contains(out, "MATCH") {
		t.Errorf("symbolic mode must not evaluate conditions:\n%s", out)
	}
}

func TestTopology(t *testing.T) {
	pip := buildPipeline(t, branchingYAML)
	mermaid := TopologyMermaid(pip)
	if !strings.Contains(mermaid, "flowchart LR") ||
		!strings.Contains(mermaid, "enrich -->|meta.tier == 'vip'| eu-out") {
		t.Errorf("mermaid wrong:\n%s", mermaid)
	}
	ascii := TopologyASCII(pip)
	if !strings.Contains(ascii, "ingest → enrich") || !strings.Contains(ascii, "enrich → eu-out") {
		t.Errorf("ascii edges missing:\n%s", ascii)
	}
}

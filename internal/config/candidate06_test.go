package config_test

import (
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

// Candidate 06 acceptance 2: Pipeline.Order is the YAML document order,
// shuffled sections included — jobs' cursor binding, explain's entry node and
// diagnostics iterate it.
func TestDeclarationOrderFollowsDocument(t *testing.T) {
	res := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: order }
sinks:
  out: { depends_on: [t], debug: {} }
sources:
  zeta:  { file: { path: a.jsonl } }
  alpha: { file: { path: b.jsonl } }
transforms:
  t: { depends_on: [zeta, alpha], script: "payload.x = 1" }
`))
	if res.Diagnostics.HasErrors() {
		t.Fatalf("load: %+v", res.Diagnostics)
	}
	want := []string{"out", "zeta", "alpha", "t"}
	got := res.Pipeline.Order
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Order = %v, want document order %v", got, want)
	}
	// The shuffled order is stable across loads (map iteration is gone).
	for i := 0; i < 20; i++ {
		again := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: order }
sinks:
  out: { depends_on: [t], debug: {} }
sources:
  zeta:  { file: { path: a.jsonl } }
  alpha: { file: { path: b.jsonl } }
transforms:
  t: { depends_on: [zeta, alpha], script: "payload.x = 1" }
`))
		if strings.Join(again.Pipeline.Order, ",") != strings.Join(want, ",") {
			t.Fatalf("Order drifted on load %d: %v", i, again.Pipeline.Order)
		}
	}
}

// Candidate 06 acceptance 3: decoder/encoder/workers/edge defaults are
// materialized into the typed config at load, and the IR reads them without
// re-defaulting.
func TestDefaultsMaterializedAtLoad(t *testing.T) {
	res := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: defaults }
sources:
  in: { file: { path: a.jsonl } }
transforms:
  t: { depends_on: [in], script: "payload.x = 1" }
sinks:
  out: { depends_on: [t], debug: {} }
`))
	if res.Diagnostics.HasErrors() {
		t.Fatalf("load: %+v", res.Diagnostics)
	}
	p := res.Pipeline
	if got := p.Sources["in"].Decoder; got != "json" {
		t.Errorf("source decoder = %q, want materialized json", got)
	}
	if got := p.Sinks["out"].Encoder; got != "json" {
		t.Errorf("sink encoder = %q, want materialized json", got)
	}
	if got := p.Transforms["t"].Workers; got != 1 {
		t.Errorf("transform workers = %d, want 1", got)
	}
	if p.EdgeDefaults.Delivery == nil || p.EdgeDefaults.Delivery.Retries != 3 || p.EdgeDefaults.Delivery.Backoff != "exponential" {
		t.Errorf("edge delivery defaults not materialized: %+v", p.EdgeDefaults.Delivery)
	}
	if p.EdgeDefaults.Required == nil || !*p.EdgeDefaults.Required {
		t.Errorf("edge required default not materialized: %+v", p.EdgeDefaults.Required)
	}
	if p.EdgeDefaults.Buffer == nil || p.EdgeDefaults.Buffer.MaxEvents != 128 {
		t.Errorf("edge buffer default not materialized: %+v", p.EdgeDefaults.Buffer)
	}

	pip, diags := ir.Build(p, testRegistry(t), starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("build: %+v", diags)
	}
	edge := pip.Nodes["out"].In[0]
	if edge.Retries != 3 || edge.Backoff != "exponential" || !edge.Required || edge.BufferMax != 128 {
		t.Fatalf("IR edge did not inherit the materialized defaults: %+v", edge)
	}
	// An explicit edge block still wins field-for-field.
	res = config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: defaults2 }
sources:
  in: { file: { path: a.jsonl } }
sinks:
  out:
    depends_on:
      in: { delivery: { retries: 9, backoff: constant }, required: false, buffer: { max_events: 7 } }
    debug: {}
`))
	if res.Diagnostics.HasErrors() {
		t.Fatalf("load: %+v", res.Diagnostics)
	}
	pip, diags = ir.Build(res.Pipeline, testRegistry(t), starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("build: %+v", diags)
	}
	edge = pip.Nodes["out"].In[0]
	if edge.Retries != 9 || edge.Backoff != "constant" || edge.Required || edge.BufferMax != 7 {
		t.Fatalf("explicit edge attributes lost: %+v", edge)
	}
}

// Candidate 06 acceptance 4: edge_defaults rejects when/route with a
// diagnostic; delivery/required/buffer are unchanged.
func TestEdgeDefaultsRejectsWhenRoute(t *testing.T) {
	res := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: edges }
edge_defaults:
  when: "payload.x == 1"
  route: vip
  delivery: { retries: 5 }
  required: false
  buffer: { max_events: 16 }
sources:
  in: { file: { path: a.jsonl } }
sinks:
  out: { depends_on: [in], debug: {} }
`))
	var codes []string
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_edge_defaults_field" {
			codes = append(codes, d.Message)
		}
	}
	if len(codes) != 2 {
		t.Fatalf("want 2 cfg_edge_defaults_field diagnostics (when, route), got %+v", res.Diagnostics)
	}
	for _, msg := range codes {
		if !strings.Contains(msg, "when") && !strings.Contains(msg, "route") {
			t.Errorf("unexpected message: %s", msg)
		}
	}
	// The legal attributes still parse and materialize.
	if res.Pipeline.EdgeDefaults.Delivery == nil || res.Pipeline.EdgeDefaults.Delivery.Retries != 5 {
		t.Fatalf("delivery not parsed: %+v", res.Pipeline.EdgeDefaults.Delivery)
	}
	if res.Pipeline.EdgeDefaults.Required == nil || *res.Pipeline.EdgeDefaults.Required {
		t.Fatalf("required not parsed: %+v", res.Pipeline.EdgeDefaults.Required)
	}
	if res.Pipeline.EdgeDefaults.Buffer == nil || res.Pipeline.EdgeDefaults.Buffer.MaxEvents != 16 {
		t.Fatalf("buffer not parsed: %+v", res.Pipeline.EdgeDefaults.Buffer)
	}
}

// Adversarial review 2026-09-24: an explicit empty `edge_defaults:` (YAML
// null) is an empty declaration, not a type error; only a non-null
// non-mapping value is rejected.
func TestEdgeDefaultsEmptyIsAccepted(t *testing.T) {
	res := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: edges-empty }
edge_defaults:
sources:
  in: { file: { path: a.jsonl } }
sinks:
  out: { depends_on: [in], debug: {} }
`))
	if res.Diagnostics.HasErrors() {
		t.Fatalf("empty edge_defaults rejected: %+v", res.Diagnostics)
	}
	if res.Pipeline.EdgeDefaults.Delivery == nil || res.Pipeline.EdgeDefaults.Delivery.Retries != 3 {
		t.Fatalf("defaults not materialized for an empty edge_defaults: %+v", res.Pipeline.EdgeDefaults)
	}
}

// Candidate 06 acceptance 5a: sinks.workers is rejected (the field was
// accepted and ignored); transforms.workers still works.
func TestSinkWorkersRejected(t *testing.T) {
	res := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: sink-workers }
sources:
  in: { file: { path: a.jsonl } }
sinks:
  out: { depends_on: [in], workers: 4, debug: {} }
`))
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_sink_workers" {
			found = true
		}
		if d.Code == "cfg_multiple_plugins" {
			t.Errorf("workers must be a dead-knob diagnostic, not a plugin clash: %+v", d)
		}
	}
	if !found {
		t.Fatalf("want cfg_sink_workers, got %+v", res.Diagnostics)
	}

	res = config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: tf-workers }
sources:
  in: { file: { path: a.jsonl } }
transforms:
  t: { depends_on: [in], workers: 4, script: "payload.x = 1" }
sinks:
  out: { depends_on: [t], debug: {} }
`))
	if res.Diagnostics.HasErrors() {
		t.Fatalf("transforms.workers rejected: %+v", res.Diagnostics)
	}
	if res.Pipeline.Transforms["t"].Workers != 4 {
		t.Fatalf("transform workers = %d, want 4", res.Pipeline.Transforms["t"].Workers)
	}
}

// Candidate 06 acceptance 6a: metadata is strictly checked.
func TestMetadataStrict(t *testing.T) {
	res := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: meta, labels: { a: b } }
sources:
  in: { file: { path: a.jsonl } }
sinks:
  out: { depends_on: [in], debug: {} }
`))
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_unknown_field" && strings.Contains(d.Message, "metadata") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unknown metadata key not diagnosed: %+v", res.Diagnostics)
	}

	res = config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: just-a-string
sources:
  in: { file: { path: a.jsonl } }
sinks:
  out: { depends_on: [in], debug: {} }
`))
	found = false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_metadata_type" {
			found = true
		}
	}
	if !found {
		t.Fatalf("non-mapping metadata not diagnosed: %+v", res.Diagnostics)
	}
}

package explain

import (
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
)

// Candidate 09 acceptance 1: explain renders resolved semantics — no
// re-derivation by plugin name.

// wasm without timeout_ms runs fast mode in production; the symbolic trace
// must say so instead of printing a fictional 1000 ms budget.
func TestSymbolicWasmResolvedMode(t *testing.T) {
	fast := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: wasm-fast }
sources:
  in: { file: { path: in.jsonl } }
transforms:
  heavy:
    depends_on: [in]
    wasm: { module: ../wasmhost/testdata/aggregate.wasm }
sinks:
  out: { depends_on: [heavy], file: { path: out.jsonl } }
`)
	out, err := Trace(fast, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "fast mode (no per-invoke kill switch)") {
		t.Errorf("fast mode not shown for an unset timeout_ms:\n%s", out)
	}
	if strings.Contains(out, "1000") {
		t.Errorf("fictional 1000ms budget still rendered:\n%s", out)
	}

	budgeted := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: wasm-budget }
sources:
  in: { file: { path: in.jsonl } }
transforms:
  heavy:
    depends_on: [in]
    wasm: { module: ../wasmhost/testdata/aggregate.wasm, timeout_ms: 1500 }
sinks:
  out: { depends_on: [heavy], file: { path: out.jsonl } }
`)
	out, err = Trace(budgeted, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "budget 1500ms") {
		t.Errorf("resolved budget not shown:\n%s", out)
	}
}

// Every inbound edge's policy is rendered, plus the engine's mixed-batch
// aggregation rule and the optional-edge drop semantics.
func TestDeliveryRendersEveryEdgeAndAggregation(t *testing.T) {
	pip := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: delivery }
sources:
  a: { file: { path: a.jsonl } }
  b: { file: { path: b.jsonl } }
transforms:
  t:
    depends_on: [a, b]
    script: "payload.x = 1"
sinks:
  out:
    depends_on:
      t: { delivery: { retries: 5, backoff: constant, timeout_ms: 250 } }
    file: { path: out.jsonl }
  side:
    depends_on:
      a: { required: false, delivery: { retries: 0 } }
    file: { path: side.jsonl }
`)
	out, err := Trace(pip, Options{Message: []byte(`{"x": 1}`)})
	if err != nil && out == "" {
		t.Fatal(err)
	}
	// Both edges into `out` are shown (the old rendering showed only the
	// first), with their own policies.
	if !strings.Contains(out, "t: retries=5 backoff=constant timeout=250ms required=true") {
		t.Errorf("per-edge policy of the first edge missing:\n%s", out)
	}
	if !strings.Contains(out, "a: retries=0 backoff=exponential timeout=default required=false") {
		t.Errorf("per-edge policy of the second edge missing:\n%s", out)
	}
	if !strings.Contains(out, "batch rule: a mixed batch takes the strictest retries and explicit timeout") {
		t.Errorf("batch aggregation rule missing:\n%s", out)
	}
	if !strings.Contains(out, "exhausted → dropped (required: false") {
		t.Errorf("optional-edge drop semantics missing:\n%s", out)
	}
	if !strings.Contains(out, "exhausted → dead letter") {
		t.Errorf("required-edge dead-letter semantics missing:\n%s", out)
	}
}

// The sample message is decoded with the source's declared decoder, not
// always JSON: a raw source accepts a non-JSON sample, and a declared codec
// decodes its own format (the header's `decoder %s` claim is true).
func TestSampleDecodedWithDeclaredDecoder(t *testing.T) {
	raw := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: raw-sample }
sources:
  in: { decoder: raw, file: { path: in.txt } }
sinks:
  out: { depends_on: [in], file: { path: out.txt } }
`)
	out, err := Trace(raw, Options{Message: []byte(`not json at all`)})
	if err != nil && out == "" {
		t.Fatalf("raw decoder sample rejected: %v\n%s", err, out)
	}
	if !strings.Contains(out, `(source file / decoder raw)`) {
		t.Errorf("decoder claim missing:\n%s", out)
	}

	csv := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: csv-sample }
codecs:
  rows:
    type: csv
    columns:
      - {name: a, type: int}
      - {name: b, type: int}
sources:
  in: { decoder: rows, file: { path: in.csv } }
sinks:
  out: { depends_on: [in], file: { path: out.csv } }
`)
	out, err = Trace(csv, Options{Message: []byte("1,2")})
	if err != nil && out == "" {
		t.Fatalf("csv sample rejected: %v\n%s", err, out)
	}
	if !strings.Contains(out, `(source file / decoder rows)`) {
		t.Errorf("declared decoder claim missing:\n%s", out)
	}

	// A json source still rejects a non-JSON sample, and the error names the
	// declared decoder.
	js := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: json-sample }
sources:
  in: { decoder: json, file: { path: in.json } }
sinks:
  out: { depends_on: [in], file: { path: out.json } }
`)
	if _, err := Trace(js, Options{Message: []byte(`nope`)}); err == nil || !strings.Contains(err.Error(), "not json") {
		t.Fatalf("json decoder must reject a non-JSON sample, got %v", err)
	}
}

// countingWhen wraps a predicate and counts evaluations.
type countingWhen struct {
	inner ir.WhenPredicate
	n     *int
}

func (c countingWhen) Lang() string { return c.inner.Lang() }

func (c countingWhen) Eval(payload, meta any) (bool, error) {
	*c.n++
	return c.inner.Eval(payload, meta)
}

// Each edge's predicate is evaluated once per walk, not twice.
func TestEdgePredicateEvaluatedOnce(t *testing.T) {
	pip := buildPipeline(t, `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: once }
sources:
  in: { file: { path: in.jsonl } }
sinks:
  out:
    depends_on: { in: { when: 'payload.kind == "keep"' } }
    file: { path: out.jsonl }
`)
	count := 0
	pip.Nodes["in"].Out[0].When = countingWhen{inner: pip.Nodes["in"].Out[0].When, n: &count}
	out, err := Trace(pip, Options{Message: []byte(`{"kind": "keep"}`)})
	if err != nil && out == "" {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("edge predicate evaluated %d times, want exactly 1:\n%s", count, out)
	}
	if !strings.Contains(out, "✓ MATCH") {
		t.Errorf("matched edge not rendered:\n%s", out)
	}
}

// An explain-safe transform that a verify-only build closed is disclosed as
// exactly that — not mislabelled as "not explain-safe".
func TestVerifyOnlyDisclosure(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	lr := config.LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: verify-only }
sources:
  in: { file: { path: in.jsonl } }
transforms:
  t: { depends_on: [in], script: "payload.x = 1" }
sinks:
  out: { depends_on: [t], file: { path: out.jsonl } }
`))
	if lr.Diagnostics.HasErrors() {
		t.Fatalf("load: %+v", lr.Diagnostics)
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("build: %+v", diags)
	}
	out, err := Trace(pip, Options{Message: []byte(`{}`)})
	if err != nil && out == "" {
		t.Fatal(err)
	}
	if !strings.Contains(out, "explain-safe instance not retained (verify-only build)") {
		t.Errorf("verify-only disclosure missing:\n%s", out)
	}
}

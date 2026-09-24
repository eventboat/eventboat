package engine

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// assertOnlyDLClass asserts exactly one dead letter with the wanted class and
// returns it.
func assertOnlyDLClass(t *testing.T, st store.Store, pipeline, want string) store.DeadLetter {
	t.Helper()
	dls, err := st.DeadLetters(pipeline)
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters = %d (%v), want 1", len(dls), err)
	}
	if dls[0].Class != want {
		t.Fatalf("dead-letter class = %q, want %q (reason %q)", dls[0].Class, want, dls[0].Reason)
	}
	return dls[0]
}

func scrapeMetrics(t *testing.T, o *obs.Obs) string {
	t.Helper()
	h := o.Handler()
	if h == nil {
		t.Fatal("prometheus handler is nil")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

// simpleSinkYAML is the minimal source → sink pipeline the class subtests
// rewire.
const simpleSinkYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: %s }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    depends_on: [in]
    mem: { id: out }
`

// Candidate 07 acceptance 4: the dead-letter class is recorded where the
// failure is produced — never re-derived from the reason text. Every engine
// production path is pinned here.
func TestDeadLetterClasses(t *testing.T) {
	t.Run("decode", func(t *testing.T) {
		h := newHarness(t)
		pip := h.build(pipelineYAML("class-decode"))
		st := store.NewMemory()
		eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

		h.source("in").Emit([]byte(`{not json`), "")
		waitCommit(t, eng)
		assertOnlyDLClass(t, st, "class-decode", store.DLClassDecode)
	})

	t.Run("codec", func(t *testing.T) {
		h := newHarness(t)
		pip := h.build(pipelineYAML("class-codec"))
		st := store.NewMemory()
		eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

		// An injected message can carry a codec the registry does not know.
		if _, err := eng.InjectAt("in", registry.Message{ID: "m1", Codec: "nope", Raw: []byte(`{}`), Meta: map[string]any{}}); err != nil {
			t.Fatal(err)
		}
		waitCommit(t, eng)
		assertOnlyDLClass(t, st, "class-codec", store.DLClassCodec)
	})

	t.Run("encode", func(t *testing.T) {
		o, err := obs.Setup(context.Background(), obs.Config{Prometheus: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = o.Shutdown(context.Background()) }()

		h := newHarness(t)
		if err := h.reg.RegisterCodec("badenc", 1, `{"type": "object"}`,
			func(cfg map[string]any, dir string) (registry.Codec, error) { return badEncoder{}, nil }); err != nil {
			t.Fatal(err)
		}
		pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: class-encode }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    depends_on: [in]
    encoder: badenc
    mem: { id: out }
`)
		st := store.NewMemory()
		opts := fastOptions()
		opts.Obs = o
		eng, _ := runEngine(t, pip, st, h.reg, opts)

		h.source("in").Emit([]byte(`{"a":1}`), "")
		waitCommit(t, eng)
		assertOnlyDLClass(t, st, "class-encode", store.DLClassEncode)

		// "encode:" used to fall through to reason_class="other" (the prefix
		// list checked "encoder", which "encode:" never matched).
		body := scrapeMetrics(t, o)
		if !strings.Contains(body, `reason_class="encode"`) {
			t.Errorf("exposition lacks reason_class=\"encode\":\n%s", body)
		}
		if strings.Contains(body, `reason_class="other"`) {
			t.Errorf("encode failure landed in reason_class=\"other\":\n%s", body)
		}
	})

	t.Run("delivery", func(t *testing.T) {
		h := newHarness(t)
		pip := h.build(pipelineYAML("class-delivery"))
		st := store.NewMemory()
		eng, _ := runEngine(t, pip, st, h.reg, fastOptions())
		h.sink("out").fail = func(int) error { return errors.New("sink down") }

		h.source("in").Emit([]byte(`{"a":1}`), "")
		waitCommit(t, eng)
		assertOnlyDLClass(t, st, "class-delivery", store.DLClassDelivery)
	})

	t.Run("transform-runtime", func(t *testing.T) {
		h := newHarness(t)
		pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: class-runtime }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    depends_on: [in]
    script: |
      fail("kaboom")
sinks:
  out: { depends_on: [t], mem: { id: out } }
`)
		st := store.NewMemory()
		eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

		h.source("in").Emit([]byte(`{"a":1}`), "")
		waitCommit(t, eng)
		dl := assertOnlyDLClass(t, st, "class-runtime", string(registry.FailureRuntime))
		if !strings.Contains(dl.Backtrace, "script:") {
			t.Errorf("backtrace lost: %q", dl.Backtrace)
		}
	})

	t.Run("transform-steps", func(t *testing.T) {
		h := newHarness(t)
		pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: class-steps }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    depends_on: [in]
    script: |
      for i in range(1000000):
          payload.x = i
sinks:
  out: { depends_on: [t], mem: { id: out } }
`)
		st := store.NewMemory()
		eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

		h.source("in").Emit([]byte(`{"a":1}`), "")
		waitCommit(t, eng)
		assertOnlyDLClass(t, st, "class-steps", string(registry.FailureSteps))
	})

	t.Run("transform-plain-error", func(t *testing.T) {
		h := newHarness(t)
		if err := testkit.RegisterFakeTransform(h.reg, "plain", func(msg *registry.Message) ([]*registry.Message, error) {
			return nil, errors.New("untyped boom")
		}); err != nil {
			t.Fatal(err)
		}
		pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: class-plain }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    depends_on: [in]
    plain: {}
sinks:
  out: { depends_on: [t], mem: { id: out } }
`)
		st := store.NewMemory()
		eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

		h.source("in").Emit([]byte(`{"a":1}`), "")
		waitCommit(t, eng)
		assertOnlyDLClass(t, st, "class-plain", string(registry.FailureOther))
	})
}

// Candidate 07 acceptance 3: flavor is an instance property read through
// TransformFlavor only. A flavor the engine does not know takes the generic
// branch — the typed kind still rides the dead-letter class (the generic
// channel every flavor shares), but no script/wasm instrument fires: the kind
// alone never routes a counter.
func TestUnknownFlavorRecordedGenerically(t *testing.T) {
	o, err := obs.Setup(context.Background(), obs.Config{Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()

	h := newHarness(t)
	if err := h.reg.RegisterTransform("flavored", 1,
		`{"type": ["object", "array", "string", "integer", "number", "boolean", "null"]}`, nil,
		func(cfg any, dir string) (registry.Transform, error) { return flavoredTransform{}, nil }); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: flavored-kind }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    depends_on: [in]
    flavored: {}
sinks:
  out: { depends_on: [t], mem: { id: out } }
`)
	st := store.NewMemory()
	opts := fastOptions()
	opts.Obs = o
	eng, _ := runEngine(t, pip, st, h.reg, opts)

	h.source("in").Emit([]byte(`{"a":1}`), "")
	waitCommit(t, eng)

	// The failure is recorded generically: the typed kind is the dead-letter
	// class, and the engine's generic run accounting counted the execution.
	assertOnlyDLClass(t, st, "flavored-kind", string(registry.FailureSteps))
	if got := eng.Metrics.TransformRuns.Load(); got != 1 {
		t.Errorf("transform runs = %d, want 1 (generic engine accounting)", got)
	}
	body := scrapeMetrics(t, o)
	if !strings.Contains(body, `reason_class="steps"`) {
		t.Errorf("exposition lacks reason_class=\"steps\":\n%s", body)
	}
	// No per-flavor instrument exists for an unknown flavor: the kind alone
	// must not feed the script/wasm counters.
	if strings.Contains(body, "eventboat_script_step_budget_exhausted_total{") {
		t.Errorf("unknown flavor fed the script budget counter:\n%s", body)
	}
	if strings.Contains(body, "eventboat_wasm_timeouts_total{") {
		t.Errorf("unknown flavor fed the wasm timeout counter:\n%s", body)
	}
}

// The counterpart: a KNOWN flavor routes its kind to the per-flavor counter
// (script + FailureSteps → the step-budget counter).
func TestKnownFlavorKindFeedsItsCounter(t *testing.T) {
	o, err := obs.Setup(context.Background(), obs.Config{Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()

	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: script-kind }
sources:
  in: { decoder: json, manual: { id: in } }
transforms:
  t:
    depends_on: [in]
    script: |
      for i in range(1000000):
          payload.x = i
sinks:
  out: { depends_on: [t], mem: { id: out } }
`)
	st := store.NewMemory()
	opts := fastOptions()
	opts.Obs = o
	eng, _ := runEngine(t, pip, st, h.reg, opts)

	h.source("in").Emit([]byte(`{"a":1}`), "")
	waitCommit(t, eng)

	assertOnlyDLClass(t, st, "script-kind", string(registry.FailureSteps))
	body := scrapeMetrics(t, o)
	if !strings.Contains(body, "eventboat_script_step_budget_exhausted_total{") {
		t.Errorf("script step budget counter missing:\n%s", body)
	}
	if !strings.Contains(body, `reason_class="steps"`) {
		t.Errorf("exposition lacks reason_class=\"steps\":\n%s", body)
	}
}

// flavoredTransform is a third-party transform with a flavor the engine does
// not know, failing with a typed kind.
type flavoredTransform struct{}

func (flavoredTransform) Init(*registry.TransformEnv) error { return nil }
func (flavoredTransform) Apply(*registry.Message) ([]*registry.Message, error) {
	return nil, &registry.TransformError{Err: errors.New("boom"), Kind: registry.FailureSteps}
}
func (flavoredTransform) Close() error   { return nil }
func (flavoredTransform) Flavor() string { return "flavored" }

// badEncoder is a codec whose Encode always fails (the encode dead-letter
// path needs a run-time encoder failure).
type badEncoder struct{}

func (badEncoder) Decode([]byte) (any, error) { return nil, errors.New("badenc decode") }
func (badEncoder) Encode(any) ([]byte, error) { return nil, errors.New("badenc encode") }

func pipelineYAML(name string) string {
	return strings.Replace(simpleSinkYAML, "%s", name, 1)
}

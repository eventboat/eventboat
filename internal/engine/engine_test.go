package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/store"
)

const linearYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: linear }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  enrich:
    from: [in]
    script: |
      payload.total = payload.price * payload.qty
      meta.label = "order-%s" % payload.id
sinks:
  out:
    from: [enrich]
    encoder: json
    mem: { id: out }
`

func TestEngineLinearFlow(t *testing.T) {
	h := newHarness(t)
	pip := h.build(linearYAML)
	st := store.NewMemory("linear")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"id":"A1","price":20,"qty":6}`), "")
	waitSettled(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 1 {
		t.Fatalf("delivered %d messages", len(delivered))
	}
	payload := decodeJSON(t, delivered[0].Out)
	if payload["total"] != 120.0 {
		t.Errorf("total = %v", payload["total"])
	}
	if delivered[0].Meta["label"] != "order-A1" {
		t.Errorf("meta.label = %v", delivered[0].Meta["label"])
	}
	if delivered[0].Meta["message_id"] == "" || delivered[0].Meta["ingest_time"] == "" {
		t.Errorf("engine stamps missing: %+v", delivered[0].Meta)
	}
	if cp, _ := st.Checkpoint("linear"); cp != 1 {
		t.Errorf("checkpoint = %d, want 1", cp)
	}
}

func TestEngineFanIn(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: fanin }
sources:
  orders:
    decoder: json
    manual: { id: orders }
  refunds:
    decoder: json
    manual: { id: refunds }
transforms:
  stamp:
    from: [orders, refunds]
    script: |
      meta.stream = meta.source
sinks:
  audit:
    from: [stamp]
    mem: { id: audit }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("fanin"), h.reg, fastOptions())

	h.source("orders").Emit([]byte(`{"kind":"order"}`), "")
	h.source("refunds").Emit([]byte(`{"kind":"refund"}`), "")
	waitSettled(t, eng)

	delivered, _, _ := h.sink("audit").snapshot()
	if len(delivered) != 2 {
		t.Fatalf("delivered %d, want 2", len(delivered))
	}
	streams := map[string]bool{}
	for _, m := range delivered {
		streams[m.Meta["stream"].(string)] = true
	}
	if !streams["orders"] || !streams["refunds"] {
		t.Errorf("streams = %v", streams)
	}
}

func TestEngineSplit(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: split }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  explode:
    from: [in]
    split: {}
sinks:
  out:
    from: [explode]
    mem: { id: out }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("split"), h.reg, fastOptions())

	h.source("in").Emit([]byte(`[{"i":1},{"i":2},{"i":3}]`), "")
	waitSettled(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 3 {
		t.Fatalf("delivered %d, want 3", len(delivered))
	}
	if delivered[0].Meta["message_id"] != delivered[2].Meta["message_id"] {
		t.Errorf("split children must share the parent message_id")
	}
}

func TestEngineConditionalRouting(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: branch }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  enrich:
    from: [in]
    script: |
      payload.total = payload.price * payload.qty
sinks:
  eu:
    from: { enrich: { when: 'payload.region == "eu"' } }
    mem: { id: eu }
  us:
    from: { enrich: { when: 'payload.region == "us"' } }
    mem: { id: us }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("branch"), h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"region":"eu","price":1,"qty":1}`), "")
	h.source("in").Emit([]byte(`{"region":"us","price":2,"qty":2}`), "")
	h.source("in").Emit([]byte(`{"region":"apac","price":3,"qty":3}`), "")
	waitSettled(t, eng)

	eu, _, _ := h.sink("eu").snapshot()
	us, _, _ := h.sink("us").snapshot()
	if len(eu) != 1 || len(us) != 1 {
		t.Fatalf("eu=%d us=%d, want 1/1", len(eu), len(us))
	}
	if eng.Metrics.NoMatch.Load() != 1 {
		t.Errorf("no-match counter = %d, want 1 (apac filtered)", eng.Metrics.NoMatch.Load())
	}
}

func TestEngineDecodeErrorGoesToDeadLetter(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: dlq }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`)
	st := store.NewMemory("dlq")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{not json`), "")
	waitSettled(t, eng)

	if delivered, _, _ := h.sink("out").snapshot(); len(delivered) != 0 {
		t.Fatalf("malformed message leaked to sink")
	}
	dls, _ := st.DeadLetters("dlq")
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(dls))
	}
	if !strings.Contains(dls[0].Reason, "decode") {
		t.Errorf("reason = %q", dls[0].Reason)
	}
}

func TestEngineScriptErrorRetriesThenDeadLetters(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: scriptfail }
edge_defaults:
  delivery: { retries: 2, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  t:
    from: [in]
    script: |
      fail("kaboom")
sinks:
  out:
    from: [t]
    mem: { id: out }
`)
	st := store.NewMemory("scriptfail")
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"a":1}`), "")
	waitSettled(t, eng)

	dls, _ := st.DeadLetters("scriptfail")
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d", len(dls))
	}
	if !strings.Contains(dls[0].Reason, "kaboom") {
		t.Errorf("reason = %q", dls[0].Reason)
	}
	if !strings.Contains(dls[0].Backtrace, "transforms.t.script") {
		t.Errorf("backtrace should name the script: %q", dls[0].Backtrace)
	}
	// 2 retries => 2 retry attempts recorded.
	if got := eng.Metrics.Retries.Load(); got != 2 {
		t.Errorf("retries = %d, want 2", got)
	}
}

func TestEngineSinkRetryThenSuccess(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: retry }
edge_defaults:
  delivery: { retries: 3, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("retry"), h.reg, fastOptions())
	h.sink("out").fail = func(attempt int) error {
		if attempt <= 2 {
			return errString("transient")
		}
		return nil
	}

	h.source("in").Emit([]byte(`{"a":1}`), "")
	waitSettled(t, eng)

	delivered, writes, _ := h.sink("out").snapshot()
	if len(delivered) != 1 || writes != 3 {
		t.Fatalf("delivered=%d writes=%d, want 1/3", len(delivered), writes)
	}
}

func TestEngineBackpressurePausesSource(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: bp }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`)
	gate := make(chan struct{})
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		if attempt == 1 {
			return gate, true
		}
		return nil, false
	}
	opts := fastOptions()
	opts.HighWatermark = 1
	eng, _ := runEngine(t, pip, store.NewMemory("bp"), h.reg, opts)

	// First message wedges the sink; the admission gate fills up.
	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 1
	})

	// Second emission must block on the high watermark.
	blocked := make(chan struct{})
	go func() {
		close(blocked)
		h.source("in").Emit([]byte(`{"i":2}`), "")
	}()
	<-blocked
	select {
	case <-time.After(150 * time.Millisecond):
		// still blocked: good
	case <-gate:
		t.Fatal("second message admitted while first unsettled")
	}
	close(gate)
	waitSettled(t, eng)
	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 2 {
		t.Fatalf("delivered = %d, want 2 after release", len(delivered))
	}
}

func TestEngineCELWrongTypeCountsAsNotPassed(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: celerr }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  a:
    from: { in: { when: 'payload.total > 10' } }
    mem: { id: a }
  b:
    from: { in: { when: 'payload.label == "x"' } }
    mem: { id: b }
`)
	eng, _ := runEngine(t, pip, store.NewMemory("celerr"), h.reg, fastOptions())

	// payload.total is a string (type error) and payload.label is absent
	// (unknown map key): both predicates fail at evaluation, count as
	// not-passed, and the message settles as filtered.
	h.source("in").Emit([]byte(`{"total":"NaN"}`), "")
	waitSettled(t, eng)

	if got := eng.Metrics.CelEvalErrors.Load(); got != 2 {
		t.Errorf("cel eval errors = %d, want 2", got)
	}
	if got := eng.Metrics.NoMatch.Load(); got != 1 {
		t.Errorf("no-match = %d, want 1", got)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// Options.WithLimits maps the limits section onto engine options; nil limits
// preserve the defaults. drain() itself uses DrainTimeout instead of the old
// hardcoded 10s (M1 debt).
func TestOptionsWithLimits(t *testing.T) {
	base := DefaultOptions()
	keep := base.WithLimits(nil)
	if keep.HighWatermark != base.HighWatermark || keep.DrainTimeout != base.DrainTimeout {
		t.Error("nil limits must not alter options")
	}
	got := base.WithLimits(&config.Limits{MaxInFlight: 42, DrainTimeout: 3 * time.Second})
	if got.HighWatermark != 42 {
		t.Errorf("HighWatermark = %d, want 42", got.HighWatermark)
	}
	if got.DrainTimeout != 3*time.Second {
		t.Errorf("DrainTimeout = %v, want 3s", got.DrainTimeout)
	}
	// Zero values keep the base option (limits validation guarantees >= 1,
	// but WithLimits stays defensive).
	got = base.WithLimits(&config.Limits{})
	if got.HighWatermark != base.HighWatermark || got.DrainTimeout != base.DrainTimeout {
		t.Error("empty limits must not alter options")
	}
}

// A sink write that outlasts DrainTimeout must not wedge shutdown past the
// bound: drain hard-cancels and Run returns once the (uncancellable) writer
// eventually finishes, instead of the pre-M2 fixed 10-second wait plus a
// wedged wait group.
func TestEngineDrainBoundedByDrainTimeout(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: drainlim }
limits: { drain_timeout: 50ms }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`)
	// The write sleeps 150ms and ignores ctx, longer than the 50ms drain
	// bound but far below the old 10s default.
	h.sink("out").block = func(attempt int) (<-chan struct{}, bool) {
		ch := make(chan struct{})
		time.AfterFunc(150*time.Millisecond, func() { close(ch) })
		return ch, true
	}

	opts := fastOptions().WithLimits(pip.Config.Limits)
	if opts.DrainTimeout != 50*time.Millisecond {
		t.Fatalf("drain timeout not applied: %v", opts.DrainTimeout)
	}
	st := store.NewMemory("drainlim")
	eng, stop := runEngine(t, pip, st, h.reg, opts)
	h.source("in").Emit([]byte(`{"i":1}`), "")

	start := time.Now()
	stop() // cancels ctx; drain waits DrainTimeout then hard-cancels
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("shutdown took %v; drain bound not honored", elapsed)
	}
	_ = eng
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not reached in time")
}

package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// Candidate 02 acceptance: one run outcome for every runner. These tests pin
// the classification matrix, the end-to-end statuses (completed / partial /
// failed / interrupted), the bounded Abandon contract, the Quiesced guards
// and the source-failure self-stop.

// startEngine launches Run and hands back the live ctx and the result
// channel: the Wait tests own the terminal wait (runEngine's cleanup-only
// stop cannot expose runDone).
func startEngine(t *testing.T, pip *ir.Pipeline, st store.Store, reg *registry.Registry, opts Options) (*Engine, context.Context, chan error, context.CancelFunc) {
	t.Helper()
	eng, err := New(pip, st, reg, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()
	t.Cleanup(cancel)
	waitReady(t, eng)
	return eng, ctx, done, cancel
}

// Acceptance 1: the four statuses are constructed directly and asserted
// against the documented priority (worker-fatal → source errors → caller
// cancellation → dead letters > 0 → completed). An engine self-stop is never
// misreported as interrupted.
func TestOutcomeClassificationMatrix(t *testing.T) {
	cases := []struct {
		name           string
		workerFatal    error
		sourceErrors   map[string]error
		callerCanceled bool
		deadLettered   int64
		want           RunStatus
	}{
		{"completed", nil, nil, false, 0, RunCompleted},
		{"partial", nil, nil, false, 3, RunPartial},
		{"failed-worker-fatal", errString("clone boom"), nil, false, 0, RunFailed},
		{"failed-source", nil, map[string]error{"in": errString("pull boom")}, false, 0, RunFailed},
		{"interrupted", nil, nil, true, 0, RunInterrupted},
		// The two self-stop classes beat a concurrent caller cancellation:
		// an engine that died is failed, never "interrupted".
		{"source-beats-cancel", nil, map[string]error{"in": errString("pull boom")}, true, 0, RunFailed},
		{"fatal-beats-cancel", errString("clone boom"), nil, true, 5, RunFailed},
	}
	for _, tc := range cases {
		got := classifyOutcome(tc.workerFatal, tc.sourceErrors, tc.callerCanceled, tc.deadLettered)
		if got != tc.want {
			t.Errorf("%s: classifyOutcome = %s, want %s", tc.name, got, tc.want)
		}
	}
}

// WaitQuiesced stays the low-level primitive (Wait builds on the same loop):
// a quiesced pipeline returns nil, a canceled caller gets ctx.Err().
func TestWaitQuiescedPrimitive(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	opts := fastOptions()
	opts.DisableSources = true
	eng, _, done, _ := startEngine(t, pip, store.NewMemory(), h.reg, opts)
	if err := eng.WaitQuiesced(context.Background(), done); err != nil {
		t.Fatalf("WaitQuiesced on a quiesced engine = %v, want nil", err)
	}

	h2 := newHarness(t)
	pip2 := h2.build(invYAML)
	eng2, ctx2, done2, cancel2 := startEngine(t, pip2, store.NewMemory(), h2.reg, fastOptions())
	cancel2()
	if err := eng2.WaitQuiesced(ctx2, done2); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitQuiesced after cancellation = %v, want context.Canceled", err)
	}
}

// Acceptance 1, end to end: a clean quiesce is completed with the engine's
// counts.
func TestWaitCompletedOutcome(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	st := store.NewMemory()
	opts := fastOptions()
	opts.DisableSources = true
	eng, _, done, _ := startEngine(t, pip, st, h.reg, opts)

	for i := 0; i < 3; i++ {
		if _, err := eng.InjectAt("in", registry.Message{Raw: []byte(`{"i":1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	waitCommit(t, eng)

	outcome := eng.Wait(context.Background(), done, WaitOptions{})
	if outcome.Status != RunCompleted {
		t.Fatalf("status = %s, want completed (fatal=%v sources=%v)", outcome.Status, outcome.WorkerFatal, outcome.SourceErrors)
	}
	if outcome.Committed != 3 {
		t.Errorf("committed = %d, want 3", outcome.Committed)
	}
	if outcome.DeadLettered != 0 || outcome.WorkerFatal != nil || len(outcome.SourceErrors) != 0 || outcome.AbandonError != nil {
		t.Errorf("clean outcome carries failure state: %+v", outcome)
	}
}

// Acceptance 1, end to end: a run that quiesces with dead letters is partial.
func TestWaitPartialOutcome(t *testing.T) {
	h := newHarness(t)
	const yamlText = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: partialout }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
transforms:
  t:
    depends_on: [in]
    script: |
      fail("boom")
sinks:
  out:
    depends_on: [t]
    mem: { id: out }
`
	pip := h.build(yamlText)
	st := store.NewMemory()
	opts := fastOptions()
	opts.DisableSources = true
	eng, _, done, _ := startEngine(t, pip, st, h.reg, opts)

	// Injection into the failing transform: the script dead-letters it, the
	// run quiesces cleanly otherwise.
	if _, err := eng.InjectAt("t", registry.Message{Raw: []byte(`{"i":1}`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eng.Metrics.DeadLettered.Load() == 1 })

	outcome := eng.Wait(context.Background(), done, WaitOptions{})
	if outcome.Status != RunPartial {
		t.Fatalf("status = %s, want partial (fatal=%v sources=%v)", outcome.Status, outcome.WorkerFatal, outcome.SourceErrors)
	}
	if outcome.DeadLettered != 1 || outcome.Committed != 1 {
		t.Errorf("counts: committed=%d dead=%d, want 1/1", outcome.Committed, outcome.DeadLettered)
	}
	if dls, _ := st.DeadLetters("partialout"); len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(dls))
	}
}

// Acceptance 3 (engine half): a genuine source failure stops the engine in
// every mode — no caller cancellation, no worker-fatal, but Run returns and
// the outcome is failed with the source error.
func TestWaitFailedOnSourceFailureStopsEngine(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory()}
	wrapped.AppendHook = func(registry.Message) error { return errString("disk full") }
	eng, _, done, _ := startEngine(t, pip, wrapped, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"i":1}`), "") // refused: the source fails
	outcome := eng.Wait(context.Background(), done, WaitOptions{})

	if outcome.Status != RunFailed {
		t.Fatalf("status = %s, want failed", outcome.Status)
	}
	if outcome.SourceErrors["in"] == nil {
		t.Fatalf("source error missing: %v", outcome.SourceErrors)
	}
	if outcome.WorkerFatal != nil {
		t.Errorf("source failure surfaced as worker-fatal: %v", outcome.WorkerFatal)
	}
	if eng.ctx.Err() == nil {
		t.Fatal("engine context still live after a source failure")
	}
	// The refused message never became durable or visible.
	if out, _, _ := eng.CommitSnapshot(); out != 0 {
		t.Fatalf("outstanding = %d after a refusal, want 0", out)
	}
}

// Acceptance 1/4, end to end: caller cancellation is interrupted, and with
// AbandonOnCancel the outstanding set is dead-lettered before the engine
// stops (the R2 contract).
func TestWaitInterruptedAbandonsOnCancel(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	st := store.NewMemory()
	opts := fastOptions()
	opts.DrainTimeout = 200 * time.Millisecond
	eng, ctx, done, cancel := startEngine(t, pip, st, h.reg, opts)

	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	h.sink("out").block = func(int) (<-chan struct{}, bool) { return gate, true }

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitFor(t, func() bool { _, writes, _ := h.sink("out").snapshot(); return writes >= 1 })

	cancel() // caller cancellation while the message is wedged mid-delivery
	outcome := eng.Wait(ctx, done, WaitOptions{
		AbandonOnCancel: true,
		AbandonReason:   "test canceled",
		AbandonTimeout:  500 * time.Millisecond,
		DrainTimeout:    500 * time.Millisecond,
	})
	if outcome.Status != RunInterrupted {
		t.Fatalf("status = %s, want interrupted", outcome.Status)
	}
	if outcome.Abandoned != 1 || outcome.AbandonError != nil {
		t.Fatalf("abandon = (%d, %v), want (1, nil)", outcome.Abandoned, outcome.AbandonError)
	}
	dls, err := st.DeadLetters("inv")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Reason != "test canceled" {
		t.Fatalf("abandoned dead letters = %+v", dls)
	}
	if dls[0].Class != store.DLClassCanceled {
		t.Fatalf("abandoned dead-letter class = %q, want %q", dls[0].Class, store.DLClassCanceled)
	}
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint = %d after abandon, want 1 (the durable record landed first)", cp)
	}
}

// gatedSink blocks writes until the gate closes or the write ctx is
// cancelled — the cancellable counterpart of memSink's crash-style wedge.
type gatedSink struct {
	gate chan struct{}
	mu   sync.Mutex
	got  []registry.Message
}

func (s *gatedSink) Write(ctx context.Context, msgs []registry.Message) error {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	s.got = append(s.got, msgs...)
	s.mu.Unlock()
	return nil
}

func (s *gatedSink) Close() error { return nil }

func (s *gatedSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

// Acceptance 4: a failing WriteDeadLetter stops the bounded Abandon, leaves
// the message uncommitted (no force-terminate, no slot release, no checkpoint
// advance) and the NEXT run replays it — never loss.
func TestAbandonBoundedLeavesMessagesForReplay(t *testing.T) {
	h := newHarness(t)
	const yamlText = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: bounded }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    depends_on: [in]
    gated: {}
`
	gated := &gatedSink{gate: make(chan struct{})}
	if err := h.reg.RegisterSink("gated", 1, memSchema, func(map[string]any) (registry.Sink, error) { return gated, nil }); err != nil {
		t.Fatal(err)
	}
	pip := h.build(yamlText)
	st := store.NewMemory()

	down := &testkit.StoreWrapper{Inner: st}
	down.DeadLetterHook = func(store.DeadLetter) error { return errString("dlq down") }
	eng, _, done, cancel := startEngine(t, pip, down, h.reg, fastOptions())

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitFor(t, func() bool { return len(eng.admit.gate) == 1 }) // wedged in the sink: one slot held

	actx, acancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	abandoned, aerr := eng.Abandon(actx, "test canceled")
	acancel()
	if aerr == nil || !strings.Contains(aerr.Error(), "engine: abandon:") {
		t.Fatalf("Abandon err = %v, want an engine: abandon wrapper", aerr)
	}
	if abandoned != 0 {
		t.Fatalf("abandoned = %d, want 0", abandoned)
	}
	if !eng.commit.isOutstanding(1) {
		t.Fatal("Abandon force-terminated a message whose record never landed")
	}
	if n := len(eng.admit.gate); n != 1 {
		t.Fatalf("Abandon released the slot of an unrecorded message (%d held)", n)
	}
	if cp, _ := st.Checkpoint("bounded"); cp != 0 {
		t.Fatalf("checkpoint = %d after a failed abandon, want 0", cp)
	}
	if dls, _ := st.DeadLetters("bounded"); len(dls) != 0 {
		t.Fatalf("dead letters written despite the failing store: %+v", dls)
	}

	// Stop the wedged engine: the message stays uncommitted (the failed
	// abandon did not force-terminate it).
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not stop")
	}

	// The next run replays the row and commits it.
	close(gated.gate)
	eng2, _, _, _ := startEngine(t, pip, st, h.reg, fastOptions())
	waitCommit(t, eng2)
	if cp, _ := st.Checkpoint("bounded"); cp != 1 {
		t.Fatalf("checkpoint after replay = %d, want 1", cp)
	}
	if gated.count() != 1 {
		t.Fatalf("replayed deliveries = %d, want 1", gated.count())
	}
}

// Acceptance 4, success path: the dead-letter record is written BEFORE the
// tracker clears the message (force-terminate only after a successful write).
func TestAbandonWritesRecordBeforeForceTerminate(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	st := store.NewMemory()

	var eng *Engine
	wrapped := &testkit.StoreWrapper{Inner: st}
	wrapped.DeadLetterHook = func(store.DeadLetter) error {
		if out, _, _ := eng.CommitSnapshot(); out != 1 {
			t.Errorf("record written with outstanding = %d, want 1 (force-terminate ran before the write)", out)
		}
		return nil
	}
	eng, _, _, _ = startEngine(t, pip, wrapped, h.reg, fastOptions())

	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	h.sink("out").block = func(int) (<-chan struct{}, bool) { return gate, true }

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitFor(t, func() bool { return len(eng.admit.gate) == 1 })

	n, err := eng.Abandon(context.Background(), "test canceled")
	if err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if n != 1 {
		t.Fatalf("abandoned = %d, want 1", n)
	}
	if eng.commit.isOutstanding(1) {
		t.Fatal("message still outstanding after a successful abandon")
	}
	if got := len(eng.admit.gate); got != 0 {
		t.Fatalf("Abandon left %d admission slot(s) held", got)
	}
	if cp, _ := st.Checkpoint("inv"); cp != 1 {
		t.Fatalf("checkpoint = %d after abandon, want 1", cp)
	}
}

// Acceptance 7: the Quiesced guards. Quiescence must be unknowable before Run
// counted the sources, must stay false while the crash-replay scan is in
// flight, and must stay false while an admission is between gate acquisition
// and registration (a message in that window looks like no work).
func TestQuiescedGuards(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	eng, err := New(pip, store.NewMemory(), h.reg, fastOptions())
	if err != nil {
		t.Fatal(err)
	}
	if eng.Quiesced() {
		t.Fatal("Quiesced before Run counted the sources")
	}

	eng.srcErrMu.Lock()
	eng.srcTotal = 0
	eng.srcStart.Store(true)
	eng.srcErrMu.Unlock()
	eng.replayDone.Store(false)
	if eng.Quiesced() {
		t.Fatal("Quiesced while the crash-replay scan is in flight")
	}
	eng.replayDone.Store(true)
	if !eng.Quiesced() {
		t.Fatal("not Quiesced with replay done and no work")
	}

	eng.admit.admitting.Add(1)
	if eng.Quiesced() {
		t.Fatal("Quiesced while an admission is in flight")
	}
	eng.admit.admitting.Add(-1)
	if !eng.Quiesced() {
		t.Fatal("not Quiesced after the admission finished")
	}
}

// Acceptance 7, injection scenario: with sources disabled and nothing
// outstanding the engine is quiesced; an injection blocked on a full
// admission gate must clear it, and quiescence must return once everything
// committed.
func TestQuiescedInjectionScenario(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	opts := fastOptions()
	opts.DisableSources = true
	opts.HighWatermark = 1
	eng, _, _, _ := startEngine(t, pip, store.NewMemory(), h.reg, opts)

	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	h.sink("out").block = func(int) (<-chan struct{}, bool) { return gate, true }

	if _, err := eng.InjectAt("in", registry.Message{Raw: []byte(`{"i":1}`)}); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		_, _ = eng.InjectAt("in", registry.Message{Raw: []byte(`{"i":2}`)})
	}()
	waitFor(t, func() bool { return eng.admit.admitting.Load() > 0 })
	if eng.Quiesced() {
		t.Fatal("Quiesced with an injection blocked on the full gate")
	}

	safeClose(gate)
	<-blocked
	waitCommit(t, eng)
	if !eng.Quiesced() {
		t.Fatal("not Quiesced after the injections committed")
	}
}

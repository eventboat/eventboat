package engine

import (
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// Candidate 01 acceptance: the emit contract. A refused emission (spool
// append failed) reaches the source through emit's error and the builtin
// default reports it as a failed source; engine shutdown under the emit is a
// voluntary stop (the source returns nil, never a failure).

func TestRefusalContractFailingAppendFailsSource(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory()}
	wrapped.AppendHook = func(registry.Message) error { return errString("disk full") }
	eng, stop := runEngine(t, pip, wrapped, h.reg, fastOptions())
	defer stop()

	h.source("in").Emit([]byte(`{"i":1}`), "")
	waitFor(t, func() bool { return len(eng.SourceErrors()) > 0 })

	if err := eng.SourceErrors()["in"]; err == nil {
		t.Fatal("refusal not recorded as a source failure")
	}
	if !eng.SourcesDone() {
		t.Fatal("failed source not marked done")
	}
	// The refused message is not durable, never visible, and its admission
	// slot was returned.
	if delivered, _, _ := h.sink("out").snapshot(); len(delivered) != 0 {
		t.Fatalf("refused message became visible: %d delivered", len(delivered))
	}
	if out, _, _ := eng.CommitSnapshot(); out != 0 {
		t.Fatalf("outstanding = %d after a refusal, want 0", out)
	}
	if n := len(eng.admit.gate); n != 0 {
		t.Fatalf("refused emission leaked %d admission slot(s)", n)
	}
}

func TestRefusalContractCancellationIsVoluntary(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())

	stop() // ctx cancellation: the source returns nil (v1.24)
	if !eng.SourcesDone() {
		t.Fatal("source did not stop on cancellation")
	}
	if errs := eng.SourceErrors(); len(errs) != 0 {
		t.Fatalf("ctx cancellation recorded as a source failure: %v", errs)
	}
}

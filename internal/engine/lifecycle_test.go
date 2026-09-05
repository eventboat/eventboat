package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Regression (review-2026-09): Run must reject a second invocation on the
// same Engine — it would replay the spool and start duplicate workers.
func TestRunTwice(t *testing.T) {
	h := newHarness(t)
	pip := h.build(linearYAML)
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	defer stop()

	err := eng.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Run called twice") {
		t.Fatalf("second Run err = %v, want \"engine: Run called twice\"", err)
	}
}

// failingReplayStore models a store fault surfacing only in Abandon's paging
// loop; everything else delegates.
type failingReplayStore struct {
	store.Store
	err error
}

func (s *failingReplayStore) ReplayPage(pipeline string, afterSeq int64, limit int, fn func(int64, registry.Message, time.Time) error) (int64, bool, error) {
	return 0, false, s.err
}

// Regression (review-2026-09): a store failure during Abandon must reach the
// caller — silently reporting zero abandoned conflates "no residue" with
// "storage down".
func TestAbandonStoreError(t *testing.T) {
	h := newHarness(t)
	pip := h.build(linearYAML)
	inner := store.NewMemory()
	eng, err := New(pip, &failingReplayStore{Store: inner, err: errors.New("store down")}, h.reg, fastOptions())
	if err != nil {
		t.Fatal(err)
	}
	spoolInjected(t, inner, "linear", "in", "x1", `{"id":"A1"}`)

	n, aerr := eng.Abandon("test")
	if aerr == nil || !strings.Contains(aerr.Error(), "engine: abandon:") {
		t.Fatalf("Abandon err = %v, want \"engine: abandon: ...\" wrapper", aerr)
	}
	if n != 0 {
		t.Fatalf("abandoned = %d, want 0", n)
	}
}

// Abandon on a healthy store dead-letters the outstanding message and returns
// its count with a nil error.
func TestAbandonDeadLettersOutstanding(t *testing.T) {
	h := newHarness(t)
	const yamlText = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: abandons }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out:
    from: [in]
    mem: { id: out }
`
	pip := h.build(yamlText)
	gate := make(chan struct{})
	t.Cleanup(func() { safeClose(gate) })
	h.sink("out").block = func(int) (<-chan struct{}, bool) { return gate, true }
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	defer stop()

	h.source("in").Emit([]byte(`{"id":"A1"}`), "")
	waitFor(t, func() bool {
		_, writes, _ := h.sink("out").snapshot()
		return writes >= 1 // wedged mid-delivery: one outstanding message
	})

	n, err := eng.Abandon("test canceled")
	if err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if n != 1 {
		t.Fatalf("abandoned = %d, want 1", n)
	}
	dls, err := eng.Store.DeadLetters("abandons")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(dls))
	}
}

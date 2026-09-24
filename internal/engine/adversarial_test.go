package engine

import (
	"sync"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Adversarial review 2026-09-24, finding 1: an injected message has no
// operator-configured edge. The synthetic edge used to be a zero value
// (Required=false), so a reinjection that failed again at its sink took the
// optional-drop path — and `replay --delete` removes the original record once
// the reinjection commits, so the message was silently lost. The synthetic
// edge is required: a re-failure must produce a fresh durable record.
func TestInjectedSinkFailureDeadLettersInsteadOfDrop(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	h.sink("out").fail = func(int) error { return errString("still broken") }
	st := store.NewMemory()
	eng, stop := runEngine(t, pip, st, h.reg, fastOptions())
	defer stop()

	if _, err := eng.InjectAt("out", registry.Message{Raw: []byte(`{"i":1}`), Codec: "json"}); err != nil {
		t.Fatal(err)
	}
	waitCommit(t, eng)

	if n := eng.Metrics.DeadLettered.Load(); n != 1 {
		t.Fatalf("deadLettered = %d, want 1 (a reinjection failure must re-dead-letter)", n)
	}
	if n := eng.Metrics.OptionalDrops.Load(); n != 0 {
		t.Fatalf("optionalDrops = %d, want 0 (an injection has no optional edge)", n)
	}
	dls, err := st.DeadLetters(pip.Config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Node != "out" || dls[0].Class != store.DLClassDelivery {
		t.Fatalf("dead letters = %+v, want one delivery-class record at out", dls)
	}
}

// fakeCodec is a config-less codec for the concurrency test below.
type fakeCodec struct{}

func (fakeCodec) Decode(raw []byte) (any, error) { return string(raw), nil }
func (fakeCodec) Encode(any) ([]byte, error)     { return nil, nil }

// Adversarial review 2026-09-24, finding 2: the lazy codec cache was an
// unguarded map written from source goroutines (entry decode), sink workers
// (encode) and operator replays/injections; two concurrent resolutions of an
// un-cached name were a concurrent-map-write panic. This runs the resolution
// concurrently under -race, using codec names the pipeline does not declare.
func TestCodecCacheConcurrentResolution(t *testing.T) {
	h := newHarness(t)
	pip := h.build(invYAML)
	eng, err := New(pip, store.NewMemory(), h.reg, fastOptions())
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"adv-a", "adv-b"} // not declared by invYAML: un-cached
	for _, name := range names {
		if err := registry.RegisterCodecT[struct{}](h.reg, name, 1,
			func(struct{}, string) (registry.Codec, error) { return fakeCodec{}, nil }); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := eng.codec(names[(i+j)%len(names)], h.reg); err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent codec resolution: %v", err)
	}
}

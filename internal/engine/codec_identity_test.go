package engine

import (
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 01 acceptance: codec identity. The injection API carries the
// message's codec, so a csv dead letter replays as csv (the old injectAt
// hardcoded json and would dead-letter the raw CSV as a decode error), and an
// internal injection can carry a non-json codec.

const codecIdentityYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: codecident }
codecs:
  events-csv:
    type: csv
    columns:
      - {name: order_no, type: string}
      - {name: amount, type: float}
sources:
  in:
    decoder: events-csv
    manual: { id: in }
sinks:
  out:
    depends_on: [in]
    mem: { id: out }
`

func TestInjectReplayCarriesCodecIdentity(t *testing.T) {
	h := newHarness(t)
	pip := h.build(codecIdentityYAML)
	st := store.NewMemory()
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	// Exactly the shape of a dead letter produced at the source's decode
	// step: raw CSV bytes plus the codec marker.
	if _, err := eng.InjectReplay("in", registry.Message{
		ID:    "dl-1",
		Codec: "events-csv",
		Raw:   []byte("ORD-1,10.5"),
		Meta:  map[string]any{"job_run_id": "r1"},
	}); err != nil {
		t.Fatal(err)
	}
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(delivered))
	}
	msg := delivered[0]
	if msg.Codec != "events-csv" {
		t.Errorf("codec = %q, want events-csv (identity lost)", msg.Codec)
	}
	if msg.ID != "dl-1" {
		t.Errorf("message_id = %q, want the preserved dl-1", msg.ID)
	}
	if v, ok := msg.Meta["is_replay"].(bool); !ok || !v {
		t.Errorf("is_replay stamp missing: %+v", msg.Meta)
	}
	payload := decodeJSON(t, msg.Out)
	if payload["order_no"] != "ORD-1" || payload["amount"] != 10.5 {
		t.Fatalf("csv payload not decoded by identity: %s", msg.Out)
	}
	if n := eng.Metrics.DecodeErrors.Load(); n != 0 {
		t.Errorf("decode errors = %d, want 0 (csv was not decoded as json)", n)
	}
	if dls, _ := st.DeadLetters("codecident"); len(dls) != 0 {
		t.Fatalf("replay dead-lettered despite a valid codec: %+v", dls)
	}
}

func TestInjectAtInternalCarriesNonJSONCodec(t *testing.T) {
	h := newHarness(t)
	pip := h.build(codecIdentityYAML)
	st := store.NewMemory()
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	if _, err := eng.InjectAt("out", registry.Message{Codec: "events-csv", Raw: []byte("ORD-2,3.5")}); err != nil {
		t.Fatal(err)
	}
	waitCommit(t, eng)

	delivered, _, _ := h.sink("out").snapshot()
	if len(delivered) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(delivered))
	}
	if delivered[0].Codec != "events-csv" {
		t.Errorf("codec = %q, want events-csv", delivered[0].Codec)
	}
	if payload := decodeJSON(t, delivered[0].Out); payload["order_no"] != "ORD-2" || payload["amount"] != 3.5 {
		t.Fatalf("payload not decoded with the message codec: %s", delivered[0].Out)
	}
	if n := eng.Metrics.DecodeErrors.Load(); n != 0 {
		t.Errorf("decode errors = %d, want 0", n)
	}
}

package engine

import (
	"testing"

	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 06: DefaultOptions and New share ONE runtime normalization path.
// A hand-built Options{} must normalize to exactly the documented production
// defaults (previously the same eight if-blocks were copied in both places and
// could drift).
func TestOneOptionNormalizationPath(t *testing.T) {
	h := newHarness(t)
	pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: options }
sources:
  in: { manual: { id: opts } }
sinks:
  out: { depends_on: [in], mem: { id: opts } }
`)
	eng, err := New(pip, store.NewMemory(), h.reg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	d := DefaultOptions()
	got := eng.Opts
	if got.Clock == nil || got.NewID == nil {
		t.Fatal("identity functions not normalized")
	}
	if got.BackoffBase != d.BackoffBase || got.DLBackoff != d.DLBackoff ||
		got.HighWatermark != d.HighWatermark || got.ChannelSize != d.ChannelSize ||
		got.BatchFlush != d.BatchFlush || got.DefaultTimeout != d.DefaultTimeout ||
		got.DrainTimeout != d.DrainTimeout || got.SpoolRetention != d.SpoolRetention ||
		got.WasmSlowCallWarnMs != d.WasmSlowCallWarnMs || got.StarOptions != d.StarOptions {
		t.Fatalf("zero Options normalized to %+v, want the DefaultOptions values %+v", got, d)
	}
	// A negative WasmSlowCallWarnMs explicitly disables the watchdog.
	eng2, err := New(pip, store.NewMemory(), h.reg, Options{WasmSlowCallWarnMs: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	if eng2.Opts.WasmSlowCallWarnMs != -1 {
		t.Fatalf("explicit negative watchdog value overwritten: %d", eng2.Opts.WasmSlowCallWarnMs)
	}
}

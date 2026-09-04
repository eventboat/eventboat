package wasmhost

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/lang/starhost"
)

// The §4.6 "heavy script" benchmark: identical multi-pass aggregation over a
// numeric array, Starlark (interpreter) vs the Go WASM guest (near-native).
// Numbers go into the README; reproduce with:
//
//	go test ./internal/wasmhost/ -bench BenchmarkHeavyTransform -benchtime 2s
const heavyStarlark = `
def agg(values):
    total = 0.0
    mn = values[0]
    mx = values[0]
    for p in range(20):
        for v in values:
            total = total + v + p * 0.000000001
            if v < mn:
                mn = v
            if v > mx:
                mx = v
    n = len(values)
    return {"count": n, "sum": total, "mean": total / n, "min": mn, "max": mx}

payload.stats = agg(payload.samples)
`

const lightStarlark = `
payload.processed = "yes"
`

func benchPayload(b *testing.B, n int) map[string]any {
	values := make([]float64, n)
	for i := range values {
		values[i] = float64(i%997) + 0.5
	}
	raw, err := json.Marshal(map[string]any{"samples": values})
	if err != nil {
		b.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		b.Fatal(err)
	}
	return m
}

func BenchmarkHeavyTransform(b *testing.B) {
	payload := benchPayload(b, 2000)

	// Heavy workloads legitimately raise the step budget (§4.6: the budget
	// is both safety valve and per-message CPU bound — a heavy script that
	// needs more pays for it explicitly).
	opts := starhost.DefaultOptions()
	opts.MaxSteps = 20_000_000
	prog, err := starhost.Compile("bench.heavy", heavyStarlark, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("starlark_heavy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ps := starhost.NewMsgState("payload", payload)
			ms := starhost.NewMsgState("meta", map[string]any{})
			if serr := prog.RunWithParams(ps, ms, nil, nil); serr != nil {
				b.Fatal(serr.Msg)
			}
		}
	})

	light, err := starhost.Compile("bench.light", lightStarlark, starhost.DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	b.Run("starlark_light", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ps := starhost.NewMsgState("payload", payload)
			ms := starhost.NewMsgState("meta", map[string]any{})
			if serr := light.RunWithParams(ps, ms, nil, nil); serr != nil {
				b.Fatal(serr.Msg)
			}
		}
	})

	path := os.Getenv("EVENTBOAT_WASM_GUEST")
	if path == "" {
		path = guestBenchPath(b)
	}
	ctx := context.Background()
	// Two wasm modes (M3-audit J2): fast is the DEFAULT (no kill switch);
	// protected (positive timeout_ms, wazero's ctx-close kill switch costs
	// ~5x on loop-heavy guests) is the explicitly-named comparison. Both
	// belong in the README table.
	fastCompiled, err := Compile(ctx, path, &config.WasmConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer fastCompiled.Close(ctx)
	fastInv := fastCompiled.NewInvoker(&config.WasmConfig{}, nil, 0)
	defer fastInv.Close()

	protectedCompiled, err := Compile(ctx, path, &config.WasmConfig{TimeoutMs: 1000})
	if err != nil {
		b.Fatal(err)
	}
	defer protectedCompiled.Close(ctx)
	protectedInv := protectedCompiled.NewInvoker(&config.WasmConfig{TimeoutMs: 1000}, nil, 0)
	defer protectedInv.Close()

	b.Run("wasm_heavy", func(b *testing.B) { // default: fast mode
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// The engine path: JSON-encode the decoded payload, cross into the
			// guest, get JSON back. Same work as the Starlark run above.
			in, err := json.Marshal(payload)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := fastInv.Invoke(ctx, in); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("wasm_heavy_protected", func(b *testing.B) { // opt-in: kill switch armed
		b.ReportAllocs()
		in, _ := json.Marshal(payload)
		for i := 0; i < b.N; i++ {
			if _, err := protectedInv.Invoke(ctx, in); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("wasm_light", func(b *testing.B) {
		small, _ := json.Marshal(map[string]any{"samples": []float64{1, 2, 3}})
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := fastInv.Invoke(ctx, small); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func guestBenchPath(b *testing.B) string {
	const p = "testdata/aggregate.wasm"
	if _, err := os.Stat(p); err != nil {
		b.Skipf("guest not built (%v)", err)
	}
	return p
}

package wasmhost

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
)

const guestPath = "testdata/aggregate.wasm"

func skipIfGuestMissing(t *testing.T) string {
	t.Helper()
	// EVENTBOAT_WASM_GUEST lets CI point tests at a freshly rebuilt guest.
	if p := os.Getenv("EVENTBOAT_WASM_GUEST"); p != "" {
		return p
	}
	if _, err := os.Stat(guestPath); err != nil {
		t.Skipf("guest not built (%v); run: GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o testdata/aggregate.wasm ./testdata/guest", err)
	}
	return guestPath
}

func TestInvokeRoundTrip(t *testing.T) {
	path := skipIfGuestMissing(t)
	ctx := context.Background()
	compiled, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer compiled.Close(ctx)
	inv := compiled.NewInvoker(nil, nil)
	defer inv.Close()

	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i)
	}
	in, _ := json.Marshal(map[string]any{"samples": values})
	out, err := inv.Invoke(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Count int     `json:"count"`
		Sum   float64 `json:"sum"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("output not json: %s", out)
	}
	if res.Count != 100 || res.Min != 0 || res.Max != 99 {
		t.Fatalf("unexpected stats: %+v", res)
	}
	// Repeated invocations reuse the instance.
	for i := 0; i < 3; i++ {
		if _, err := inv.Invoke(ctx, in); err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
	}
}

func TestInvokeGuestError(t *testing.T) {
	path := skipIfGuestMissing(t)
	ctx := context.Background()
	compiled, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer compiled.Close(ctx)
	inv := compiled.NewInvoker(&config.WasmConfig{}, nil)
	defer inv.Close()

	// Empty values: the guest reports a domain error via eb_last_error.
	if _, err := inv.Invoke(ctx, []byte(`{"samples":[]}`)); err == nil || !strings.Contains(err.Error(), "samples must not be empty") {
		t.Fatalf("want guest error, got %v", err)
	}
	// Bad JSON: guest error too, and the invoker recovers on the next call.
	if _, err := inv.Invoke(ctx, []byte(`not json`)); err == nil {
		t.Fatal("want error for invalid json")
	}
	if _, err := inv.Invoke(ctx, []byte(`{"samples":[1,2,3]}`)); err != nil {
		t.Fatalf("invoker did not recover after guest error: %v", err)
	}
}

func TestInvokeTimeoutKillsAndRecovers(t *testing.T) {
	path := skipIfGuestMissing(t)
	ctx := context.Background()
	compiled, err := Compile(ctx, path, &config.WasmConfig{TimeoutMs: 200, MaxMemoryPages: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer compiled.Close(ctx)
	inv := compiled.NewInvoker(&config.WasmConfig{TimeoutMs: 200, MaxMemoryPages: 1024}, nil)
	defer inv.Close()

	// Huge input with many passes: the guest cannot finish in 200ms.
	values := make([]float64, 200_000)
	in, _ := json.Marshal(map[string]any{"samples": values, "passes": 50})
	start := time.Now()
	_, err = inv.Invoke(ctx, in)
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("want timeout error, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("kill was not prompt: %v", time.Since(start))
	}
	// The instance died; the next invoke must re-instantiate and succeed.
	if _, err := inv.Invoke(ctx, []byte(`{"samples":[1,2,3]}`)); err != nil {
		t.Fatalf("no recovery after timeout: %v", err)
	}
}

func TestCompileRejectsNonWasm(t *testing.T) {
	ctx := context.Background()
	notWasm := filepath.Join(t.TempDir(), "fake.wasm")
	if err := os.WriteFile(notWasm, []byte("ELF, honest"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(ctx, notWasm, nil); err == nil {
		t.Fatal("non-wasm module accepted")
	}
	if _, err := Compile(ctx, filepath.Join(t.TempDir(), "missing.wasm"), nil); err == nil {
		t.Fatal("missing module accepted")
	}
}

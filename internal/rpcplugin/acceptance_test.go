// Package rpcplugin_test holds the third-party plugin acceptance gate
// (redesign-v3.md §7.4 M3): examples/plugins/ticker-source is a separate Go
// module that depends only on the generated protocol code — this test proves
// the full verify -> run chain works with nothing but the protocol contract.
package rpcplugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
)

const tickerDir = "../../examples/plugins/ticker-source"

// buildTicker compiles the example plugin binary like an outside user would.
func buildTicker(t *testing.T) (bin, manifest string) {
	t.Helper()
	bin = filepath.Join(t.TempDir(), "ticker-plugin")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = tickerDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build ticker-source: %v\n%s", err, out)
	}
	return bin, filepath.Join(tickerDir, "manifest.json")
}

func acceptanceYAML(bin, manifest, outFile string) string {
	return fmt.Sprintf(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: ticker-acceptance }
sources:
  prices:
    version: 1
    grpc:
      command: [%q]
      schema: %q
    ticker:
      symbol: USD/EUR
      events: 5
      interval_ms: 10
sinks:
  out:
    from: [prices]
    file: { path: %q }
`, filepath.ToSlash(bin), filepath.ToSlash(manifest), filepath.ToSlash(outFile))
}

func buildIR(t *testing.T, reg *registry.Registry, yamlText string) (*ir.Pipeline, []config.Diagnostic) {
	t.Helper()
	lr := config.LoadBytes("acceptance.yaml", []byte(yamlText))
	if lr.HasErrors() {
		return nil, lr.Diagnostics
	}
	return ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
}

func hasDiag(diags []config.Diagnostic, code string) bool {
	for _, d := range diags {
		if d.Code == code {
			return true
		}
	}
	return false
}

// Gate 1 acceptance: the external plugin verifies clean against its manifest,
// and the static gates (version pin, strict schema, builtin conflict) fire.
func TestTickerPluginVerify(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the plugin binary")
	}
	bin, manifest := buildTicker(t)
	outFile := filepath.Join(t.TempDir(), "out.ndjson")
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	pip, diags := buildIR(t, reg, acceptanceYAML(bin, manifest, outFile))
	if pip == nil || anyError(diags) {
		t.Fatalf("verify errors:\n%+v", diags)
	}

	// Version pin mismatch is a verify error (§6.5).
	_, diags = buildIR(t, reg, strings.Replace(acceptanceYAML(bin, manifest, outFile), "version: 1", "version: 2", 1))
	if !hasDiag(diags, "plugin_version_mismatch") {
		t.Errorf("version mismatch not diagnosed; got %+v", diags)
	}

	// Strict schema: an unknown field inside the plugin block is an error.
	bad := strings.Replace(acceptanceYAML(bin, manifest, outFile), "interval_ms: 10", "interval_ms: 10\n      nonexistent: true", 1)
	_, diags = buildIR(t, reg, bad)
	if !hasDiag(diags, "plugin_schema") {
		t.Errorf("unknown plugin field not diagnosed; got %+v", diags)
	}

	// A grpc block on a compiled-in plugin name is rejected.
	conflict := strings.Replace(acceptanceYAML(bin, manifest, outFile), "ticker:", "kafka:", 1)
	_, diags = buildIR(t, reg, conflict)
	if !hasDiag(diags, "grpc_builtin_conflict") {
		t.Errorf("grpc on builtin not diagnosed; got %+v", diags)
	}
}

func anyError(diags []config.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

// Full-chain acceptance: the real engine spawns the real plugin process,
// pulls five events and lands them in the file sink, at-least-once.
func TestTickerPluginRun(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and spawns the plugin binary")
	}
	bin, manifest := buildTicker(t)
	outFile := filepath.Join(t.TempDir(), "out.ndjson")
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	pip, diags := buildIR(t, reg, acceptanceYAML(bin, manifest, outFile))
	if anyError(diags) {
		t.Fatalf("verify errors:\n%+v", diags)
	}

	opts := engine.DefaultOptions()
	opts.Logf = t.Logf
	eng, err := engine.New(pip, store.NewMemory("ticker-acceptance"), reg, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- eng.Run(ctx) }()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if eng.SourcesDone() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !eng.SourcesDone() {
		for name, err := range eng.SourceErrors() {
			t.Logf("source %q error: %v", name, err)
		}
		t.Logf("messagesIn=%d committed=%d deadlettered=%d", eng.Metrics.MessagesIn.Load(), eng.Metrics.CommittedCount.Load(), eng.Metrics.DeadLettered.Load())
		t.Fatal("ticker plugin did not signal exhaustion")
	}
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := eng.WaitCommit(sctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not stop")
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read sink output: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 5 {
		t.Fatalf("sink got %d lines, want 5:\n%s", len(lines), data)
	}
	for i, line := range lines {
		var ev struct {
			Symbol string  `json:"symbol"`
			Seq    int64   `json:"seq"`
			Price  float64 `json:"price"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if ev.Symbol != "USD/EUR" || ev.Seq != int64(i+1) || ev.Price <= 0 {
			t.Fatalf("line %d: unexpected event %+v", i, ev)
		}
	}
}

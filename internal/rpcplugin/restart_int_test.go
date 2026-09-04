package rpcplugin

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/registry"
)

// The restart policy (grpc.restart: restart — the M3 fast-fail trim,
// closed by the beta round): a crashed plugin process is respawned with
// backoff, its config re-delivered, and the pull continues — duplicates are
// the at-least-once contract. The default (no restart field) keeps M3
// fast-fail semantics: a dead process surfaces as an error, no respawn.
// tickerDir mirrors the external acceptance test's plugin directory (the
// external package's const is not visible here).
const tickerDir = "../../examples/plugins/ticker-source"

func buildTickerBin(t *testing.T) (string, *config.PluginManifest) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "ticker-plugin")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = tickerDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build ticker: %v\n%s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(tickerDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest config.PluginManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return bin, &manifest
}

func TestSupervisorRestartsCrashedPlugin(t *testing.T) {
	bin, manifest := buildTickerBin(t)
	var restarts atomic.Int64
	cfg := &config.GrpcConfig{
		Command: []string{bin},
		Restart: "restart",
	}
	src, err := SpawnSource(context.Background(), cfg, manifest, map[string]any{
		"symbol": "RESTART", "events": 2, "interval_ms": 5,
	}, func(f string, a ...any) { t.Logf(f, a...) },
		WithRestartCounter(func(plugin string) {
			if plugin != "ticker" {
				t.Errorf("restart counter plugin = %q, want ticker", plugin)
			}
			restarts.Add(1)
		}))
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer func() { _ = src.Close() }()
	s := src.(*source)
	if s.sup == nil {
		t.Fatal("restart policy must install a supervisor")
	}

	// First pull: 2 events, clean exhaustion.
	ps := src.(registry.PullSource)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n := 0
	if err := ps.Pull(ctx, func(registry.Message) { n++ }); err != nil {
		t.Fatalf("first pull: %v", err)
	}
	if n != 2 {
		t.Fatalf("first pull events = %d, want 2", n)
	}

	// Crash the process behind the adapter's back.
	s.sup.mu.Lock()
	old := s.sup.proc
	s.sup.mu.Unlock()
	old.kill()
	deadline := time.Now().Add(5 * time.Second)
	for old.alive() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// Second pull: the supervisor respawns (backoff starts at 250ms),
	// re-delivers config, and the pull succeeds again.
	n = 0
	if err := ps.Pull(ctx, func(registry.Message) { n++ }); err != nil {
		t.Fatalf("pull after crash: %v", err)
	}
	if n != 2 {
		t.Fatalf("pull after crash events = %d, want 2", n)
	}
	if got := s.sup.count(); got != 1 {
		t.Fatalf("restarts = %d, want 1", got)
	}
	if got := restarts.Load(); got != 1 {
		t.Fatalf("restart counter callback fired %d times, want 1", got)
	}
	s.sup.mu.Lock()
	newP := s.sup.proc
	s.sup.mu.Unlock()
	if newP == old {
		t.Fatal("process was not replaced")
	}
}

func TestFastFailDefaultKeepsM3Semantics(t *testing.T) {
	bin, manifest := buildTickerBin(t)
	cfg := &config.GrpcConfig{Command: []string{bin}} // no restart field
	src, err := SpawnSource(context.Background(), cfg, manifest, map[string]any{
		"symbol": "FASTFAIL", "events": 1, "interval_ms": 5,
	}, nil)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer func() { _ = src.Close() }()
	s := src.(*source)
	if s.sup != nil {
		t.Fatal("default policy must stay fast-fail (no supervisor)")
	}

	// Crash; the next pull surfaces the transport error, no respawn.
	s.plug.kill()
	ps := src.(registry.PullSource)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ps.Pull(ctx, func(registry.Message) {}); err == nil {
		t.Fatal("fast-fail: pull after crash must surface an error")
	}
}

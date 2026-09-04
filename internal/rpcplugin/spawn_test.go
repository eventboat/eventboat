package rpcplugin_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/rpcplugin"
	"github.com/eventboat/eventboat/internal/registry"
)

// TestSpawnSourceDirect drives the plugin process without the engine:
// spawn -> handshake -> Init -> Pull to exhaustion. It isolates the protocol
// layer from engine scheduling.
func TestSpawnSourceDirect(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "ticker-plugin")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = tickerDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	manifestData, err := os.ReadFile(filepath.Join(tickerDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest config.PluginManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}

	cfg := &config.GrpcConfig{Command: []string{bin}}
	src, err := rpcplugin.SpawnSource(context.Background(), cfg, &manifest, map[string]any{
		"symbol": "TEST", "events": 3, "interval_ms": 5,
	}, func(f string, a ...any) { t.Logf(f, a...) })
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer src.Close()

	if err := src.Init(nil); err != nil {
		t.Fatalf("init: %v", err)
	}

	ps, ok := src.(registry.PullSource)
	if !ok {
		t.Fatalf("spawned source (%T) does not implement PullSource", src)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var events []registry.Message
	if err := ps.Pull(ctx, func(m registry.Message) {
		events = append(events, m)
		t.Logf("event: %s", m.Raw)
	}); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[0].Cursor != "1" || events[2].Cursor != "3" {
		t.Fatalf("cursors: %+v", events)
	}
}

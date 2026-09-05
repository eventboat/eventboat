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
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/rpcplugin"
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
	defer func() { _ = src.Close() }()

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

// The plugin transport's message cap is explicit and above grpc-go's 4 MiB
// default (rpcplugin.MaxMessageSize): a >4 MiB Event crosses the wire
// cleanly — with the default limits it would fail as ResourceExhausted. The
// 5 MiB payload also stays deliberately far under the 64 MiB cap.
func TestSpawnSourceLargeMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the plugin binary")
	}
	bin, manifestPath := buildTicker(t)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest config.PluginManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}

	cfg := &config.GrpcConfig{Command: []string{bin}}
	src, err := rpcplugin.SpawnSource(context.Background(), cfg, &manifest, map[string]any{
		"symbol": "BIG", "events": 1, "interval_ms": 1, "pad_bytes": 5 << 20,
	}, func(f string, a ...any) { t.Logf(f, a...) })
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer func() { _ = src.Close() }()
	if err := src.Init(nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	ps, ok := src.(registry.PullSource)
	if !ok {
		t.Fatalf("spawned source (%T) does not implement PullSource", src)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var events []registry.Message
	if err := ps.Pull(ctx, func(m registry.Message) { events = append(events, m) }); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if got := len(events[0].Raw); got <= 4<<20 || got > rpcplugin.MaxMessageSize {
		t.Fatalf("payload size %d, want between 4 MiB and the %d-byte transport cap", got, rpcplugin.MaxMessageSize)
	}
}

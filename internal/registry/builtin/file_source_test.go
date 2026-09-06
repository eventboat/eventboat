package builtin

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

func newFileSource(t *testing.T, dir, content, onEOF string) registry.Source {
	t.Helper()
	if content != "" {
		path := filepath.Join(dir, "data.jsonl")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reg := registry.New()
	if err := registerFileSource(reg); err != nil {
		t.Fatal(err)
	}
	src, err := reg.NewSource("file", map[string]any{
		"path":          filepath.ToSlash(filepath.Join(dir, "data.jsonl")),
		"poll_every_ms": 10,
		"on_eof":        onEOF,
	})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// on_eof: stop reads the whole file, emits one message per line, then Run
// returns nil (exhausted) — the batch/job completion signal.
func TestFileSourceStopExhausts(t *testing.T) {
	src := newFileSource(t, t.TempDir(), "{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n", "stop")
	var got []registry.Message
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- src.Run(ctx, func(m registry.Message) { got = append(got, m) }) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop mode never returned")
	}
	_ = src.Close()
	if len(got) != 3 {
		t.Fatalf("emitted %d messages, want 3", len(got))
	}
}

// stop mode fails loudly on a missing file instead of sitting silent.
func TestFileSourceStopMissingFileErrors(t *testing.T) {
	src := newFileSource(t, t.TempDir(), "", "stop")
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- src.Run(ctx, func(registry.Message) {}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing file under stop mode returned nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned")
	}
	_ = src.Close()
}

// tail mode (the default) keeps polling for a missing file — Run returns
// only on cancellation, and cancellation is a voluntary stop (nil).
func TestFileSourceTailMissingFileWaits(t *testing.T) {
	src := newFileSource(t, t.TempDir(), "", "tail")
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- src.Run(ctx, func(registry.Message) {}) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancellation must be a voluntary stop (nil), got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tail mode never returned after cancel")
	}
	_ = src.Close()
}

// A resumed stop-mode source whose committed offset is already at EOF
// finishes immediately without re-emitting (re-trigger does not re-read).
func TestFileSourceStopResumeAtEOF(t *testing.T) {
	dir := t.TempDir()
	src := newFileSource(t, dir, "{\"i\":1}\n", "stop")
	var state []byte
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	emissions := 0
	_ = src.Run(ctx, func(m registry.Message) { emissions++ })
	state, _ = src.Commit(context.Background(), 1)
	_ = src.Close()

	src2 := newFileSource(t, dir, "", "stop")
	if err := src2.Init(state); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	emissions2 := 0
	go func() { done <- src2.Run(ctx, func(m registry.Message) { emissions2++ }) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resumed Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed stop mode never returned")
	}
	_ = src2.Close()
	if emissions != 1 || emissions2 != 0 {
		t.Fatalf("emissions = %d/%d, want 1/0", emissions, emissions2)
	}
}

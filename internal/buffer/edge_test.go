package buffer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/message"
)

func TestDiskWALAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewDiskWAL(DiskOptions{Dir: dir, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	msg := message.New([]byte("hello"), map[string]any{"x": 1})
	msg.ID = "m1"
	if err := wal.Append(msg); err != nil {
		t.Fatal(err)
	}
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := NewDiskWAL(DiskOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()
	got, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "m1" {
		t.Fatalf("got %#v", got)
	}
}

func TestEdgeInboundOverflowSpill(t *testing.T) {
	dir := t.TempDir()
	eb, err := NewEdgeInbound(EdgeOptions{
		Pipeline: "p",
		From:     "a",
		To:       "b",
		Config: config.EdgeBufferConfig{
			Type:     "overflow",
			Size:     1,
			Strategy: "block",
			DiskPath: dir,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	eb.Start(ctx)

	got := make(chan string, 2)
	go func() {
		for i := 0; i < 2; i++ {
			m := <-eb.Out()
			got <- string(m.Payload)
		}
	}()

	m1 := message.New([]byte("1"), nil)
	m2 := message.New([]byte("2"), nil)
	if _, _, err := eb.Enqueue(ctx, m1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eb.Enqueue(ctx, m2); err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case p := <-got:
			seen[p]++
		case <-timeout:
			t.Fatalf("timeout waiting for messages, seen=%v", seen)
		}
	}
	if seen["1"] != 1 || seen["2"] != 1 {
		t.Fatalf("payloads %v", seen)
	}

	_ = eb.Close()

	walDir := filepath.Join(dir, "p", "a__b")
	if _, err := os.Stat(walDir); err != nil {
		t.Fatalf("expected wal dir: %v", err)
	}
}

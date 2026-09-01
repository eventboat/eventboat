package buffer

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverpod/riverpod/internal/config"
	"github.com/riverpod/riverpod/internal/message"
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

func TestEdgeInboundEnqueueCloseRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		eb, err := NewEdgeInbound(EdgeOptions{
			Pipeline: "p",
			From:     "a",
			To:       "b",
			Config: config.EdgeBufferConfig{
				Type:     "memory",
				Size:     1,
				Strategy: "block",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		eb.Start(ctx)

		// nobody drains Out(), so producers quickly block on a full mem
		var wg sync.WaitGroup
		for p := 0; p < 4; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for pctx := ctx; pctx.Err() == nil; {
					_, _, _ = eb.Enqueue(pctx, message.New([]byte("x"), nil))
				}
			}()
		}
		time.Sleep(2 * time.Millisecond)
		// Close races with producers blocked on a full mem channel
		done := make(chan error, 1)
		go func() { done <- eb.Close() }()
		time.Sleep(time.Millisecond)
		cancel() // releases blocked producers and the drain loop
		wg.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestEdgeInboundNacksResidualMessagesOnShutdown(t *testing.T) {
	eb, err := NewEdgeInbound(EdgeOptions{
		Pipeline: "p",
		From:     "a",
		To:       "b",
		Config: config.EdgeBufferConfig{
			Type:     "memory",
			Size:     4,
			Strategy: "block",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	eb.Start(ctx)

	// nobody drains Out(): it fills, the drain loop blocks in emit, and the
	// rest piles up in mem
	const total = 8
	var acked atomic.Int64
	var ackErr atomic.Int64
	for i := 0; i < total; i++ {
		m := message.New([]byte("x"), nil)
		m.SetAckFn(func(err error) {
			acked.Add(1)
			if err != nil {
				ackErr.Add(1)
			}
		})
		if _, _, err := eb.Enqueue(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	if err := eb.Close(); err != nil {
		t.Fatal(err)
	}
	// messages that made it into Out are the engine's responsibility; every
	// other message must have received a terminal (error) ack
	inOut := 0
	for range eb.Out() {
		inOut++
	}
	if got := int(acked.Load()); got+inOut != total {
		t.Fatalf("acked %d + out %d != %d: residual messages never acked", got, inOut, total)
	}
	if ackErr.Load() != acked.Load() {
		t.Fatalf("residual acks must be nacks, ackErr=%d acked=%d", ackErr.Load(), acked.Load())
	}
}

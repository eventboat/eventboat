package ops

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// Rethink follow-up 2026-09-24 (supplement B): the SSE `status` transition
// events (Pause/Drain/Resume/engine completion) used to carry a bare
// pipeline name while the periodic ticker carried the full snapshot — one
// event type with two payload shapes, and the UI rendered the string as a
// table. Both now carry []PipelineStatus.
func TestStatusEventCarriesSnapshot(t *testing.T) {
	testkit.ResetFakePull()
	dir := t.TempDir()
	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := New(Options{DataDir: dir, Reg: ownerTestRegistry(t), Stores: owner, Clock: time.Now})
	t.Cleanup(svc.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := svc.Deploy(ctx, ownerTestYAML("sse-job", "sse-feed", filepath.Join(dir, "out.jsonl"))); err != nil {
		t.Fatal(err)
	}
	events, unsub := svc.Subscribe()
	defer unsub()

	if err := svc.Pause("sse-job"); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type != "status" {
				continue
			}
			snap, ok := ev.Data.([]PipelineStatus)
			if !ok {
				t.Fatalf("status event payload is %T, want []PipelineStatus", ev.Data)
			}
			if len(snap) != 1 || snap[0].Pipeline != "sse-job" || snap[0].Status != "paused" {
				t.Fatalf("status snapshot = %+v, want one paused sse-job", snap)
			}
			return
		case <-deadline:
			t.Fatal("no status event after Pause")
		}
	}
}

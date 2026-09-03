package buffer

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/riverpod/riverpod/internal/message"
)

// The ack chain attached before Append must survive the WAL roundtrip: the
// decoded message acks both the WAL offset commit and the original handler
// (fan-out aggregator → source OnAck).
func TestDiskWALReadNextRestoresEnqueuedAckChain(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)

	var acked atomic.Int32
	var ackErr error
	m := message.New([]byte("one"), nil)
	m.ID = "m1"
	m.SetAckFn(func(err error) {
		acked.Add(1)
		ackErr = err
	})
	if err := wal.Append(m); err != nil {
		t.Fatal(err)
	}
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}

	got, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "m1" {
		t.Fatalf("got %#v", got)
	}
	got.Ack(nil)
	if acked.Load() != 1 {
		t.Fatalf("enqueued ack chain lost across WAL roundtrip: fired %d times, want 1", acked.Load())
	}
	if ackErr != nil {
		t.Fatalf("ack error = %v, want nil", ackErr)
	}

	// error acks must propagate through the restored chain too
	var nackErr error
	m2 := message.New([]byte("two"), nil)
	m2.ID = "m2"
	m2.SetAckFn(func(err error) { nackErr = err })
	if err := wal.Append(m2); err != nil {
		t.Fatal(err)
	}
	got2, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	got2.Ack(errors.New("boom"))
	if nackErr == nil || nackErr.Error() != "boom" {
		t.Fatalf("nack error = %v, want boom", nackErr)
	}
}

// After a crash the in-process ack closures are gone: a replayed record must
// still commit its WAL offset on ack, but must NOT fire the stale in-process
// handler (e.g. a new kafka reader session must not commit offsets fetched by
// the previous, dead session — kafka will redeliver from its own offset).
func TestDiskWALReplayDropsInProcessAckChain(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)

	var acked atomic.Int32
	m := message.New([]byte("one"), nil)
	m.ID = "m1"
	m.SetAckFn(func(error) { acked.Add(1) })
	if err := wal.Append(m); err != nil {
		t.Fatal(err)
	}
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}

	// crash: new DiskWAL instance on the same dir
	wal2 := reopenWAL(t, dir)
	got, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "m1" {
		t.Fatalf("got %#v", got)
	}
	got.Ack(nil)
	if acked.Load() != 0 {
		t.Fatalf("replayed record fired stale in-process ack chain %d times, want 0", acked.Load())
	}

	// ...but the WAL offset must have been committed
	wal3 := reopenWAL(t, dir)
	next, err := wal3.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatalf("replayed ack did not commit WAL offset, got %#v", next)
	}
}

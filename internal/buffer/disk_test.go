package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/riverpod/riverpod/internal/message"
)

// reopenWAL simulates a crash: a new DiskWAL on the same dir without closing
// the previous instance.
func reopenWAL(t *testing.T, dir string) *DiskWAL {
	t.Helper()
	w, err := NewDiskWAL(DiskOptions{Dir: dir, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func appendMsg(t *testing.T, w *DiskWAL, id, payload string) {
	t.Helper()
	m := message.New([]byte(payload), nil)
	m.ID = id
	if err := w.Append(m); err != nil {
		t.Fatal(err)
	}
}

func TestDiskWALUnackedMessageRedeliveredAfterCrash(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
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
	// crash before ack: offset must not have been committed
	wal2 := reopenWAL(t, dir)
	again, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if again == nil || again.ID != "m1" {
		t.Fatalf("expected redelivery of unacked m1, got %#v", again)
	}

	// ack on the original reader commits the offset
	got.Ack(nil)
	wal3 := reopenWAL(t, dir)
	next, err := wal3.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || next.ID != "m2" {
		t.Fatalf("expected m2 after m1 ack, got %#v", next)
	}
}

func TestDiskWALShutdownNackedMessageRedeliveredAfterCrash(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	got, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	// a shutdown nack (context cancellation) means "not processed": the
	// offset must stay put so the message is redelivered after a restart
	got.Ack(context.Canceled)

	wal2 := reopenWAL(t, dir)
	again, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if again == nil || again.ID != "m1" {
		t.Fatalf("expected redelivery of shutdown-nacked m1, got %#v", again)
	}
}

func TestDiskWALTerminalNackCommitsOffset(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	m1, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	// a processing-failure nack is terminal (the engine already routed the
	// message to its DLQ/drop disposition): the record is consumed and must
	// not stall the ack watermark for later records
	m1.Ack(errors.New("processing failed"))
	m2, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	m2.Ack(nil)

	wal2 := reopenWAL(t, dir)
	next, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatalf("terminal nack must commit the offset, got redelivery of %#v", next)
	}
}

func TestDiskWALOffsetCommitIsPrefixOrdered(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	m1, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	// acking m2 alone must not skip m1 after a crash
	m2.Ack(nil)
	wal2 := reopenWAL(t, dir)
	again, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if again == nil || again.ID != "m1" {
		t.Fatalf("out-of-order ack skipped m1, got %#v", again)
	}

	m1.Ack(nil)
	wal3 := reopenWAL(t, dir)
	got, err := wal3.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected no pending messages, got %#v", got)
	}
}

func TestDiskWALSegmentRetainedUntilMessagesAcked(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewDiskWAL(DiskOptions{Dir: dir, SegmentSize: 1, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
	appendMsg(t, wal, "m3", "three")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}

	m1, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if m1.ID != "m1" {
		t.Fatalf("got %q", m1.ID)
	}
	// reading m2 crosses seg-1's EOF while m1 is still unacked: seg-1 must survive
	m2, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if m2.ID != "m2" {
		t.Fatalf("got %q", m2.ID)
	}

	wal2 := reopenWAL(t, dir)
	again, err := wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if again == nil || again.ID != "m1" {
		t.Fatalf("segment with unacked m1 was lost, got %#v", again)
	}
	if _, err := os.Stat(filepath.Join(dir, "seg-000001.wal")); err != nil {
		t.Fatalf("seg-000001.wal should be retained until ack: %v", err)
	}

	m1.Ack(nil)
	m2.Ack(nil)
	wal3 := reopenWAL(t, dir)
	got, err := wal3.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "m3" {
		t.Fatalf("expected m3 after acks, got %#v", got)
	}
}

func TestDiskWALTornWriteRecovers(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// simulate a torn write: cut the last record short
	seg := filepath.Join(dir, "seg-000001.wal")
	info, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(seg, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	wal2 := reopenWAL(t, dir)
	got, err := wal2.ReadNext()
	if err != nil {
		t.Fatalf("torn tail must not stall the edge: %v", err)
	}
	if got == nil || got.ID != "m1" {
		t.Fatalf("got %#v", got)
	}
	got.Ack(nil)
	got, err = wal2.ReadNext()
	if err != nil {
		t.Fatalf("torn tail must not stall the edge: %v", err)
	}
	if got != nil {
		t.Fatalf("corrupt tail should be discarded, got %#v", got)
	}
	// the WAL stays writable after recovery
	appendMsg(t, wal2, "m3", "three")
	got, err = wal2.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "m3" {
		t.Fatalf("got %#v", got)
	}
}

func TestDiskWALCorruptRecordRecovers(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// bit rot inside the second record
	seg := filepath.Join(dir, "seg-000001.wal")
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-3] ^= 0xff
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	wal2 := reopenWAL(t, dir)
	got, err := wal2.ReadNext()
	if err != nil {
		t.Fatalf("corrupt tail must not stall the edge: %v", err)
	}
	if got == nil || got.ID != "m1" {
		t.Fatalf("got %#v", got)
	}
	got.Ack(nil)
	got, err = wal2.ReadNext()
	if err != nil {
		t.Fatalf("corrupt tail must not stall the edge: %v", err)
	}
	if got != nil {
		t.Fatalf("corrupt record should be discarded, got %#v", got)
	}
}

func TestDiskWALOffsetPersistedAtomically(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	appendMsg(t, wal, "m2", "two")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	m1, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	m1.Ack(nil)

	// offset file must be complete valid JSON, no temp file left behind
	raw, err := os.ReadFile(filepath.Join(dir, "consumer.offset"))
	if err != nil {
		t.Fatal(err)
	}
	var st offsetState
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("consumer.offset is not intact JSON: %v", err)
	}
	if st.Segment != "seg-000001.wal" || st.Offset <= 0 {
		t.Fatalf("unexpected offset state %+v", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "consumer.offset.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp offset file left behind: %v", err)
	}

	// a second commit must replace the existing file (Windows rename path)
	m2, err := wal.ReadNext()
	if err != nil {
		t.Fatal(err)
	}
	m2.Ack(nil)
	raw2, err := os.ReadFile(filepath.Join(dir, "consumer.offset"))
	if err != nil {
		t.Fatal(err)
	}
	var st2 offsetState
	if err := json.Unmarshal(raw2, &st2); err != nil {
		t.Fatalf("consumer.offset is not intact JSON: %v", err)
	}
	if st2.Offset <= st.Offset {
		t.Fatalf("offset did not advance: %+v -> %+v", st, st2)
	}
}

func TestDiskWALCorruptOffsetFallsBackToFullReplay(t *testing.T) {
	dir := t.TempDir()
	wal := reopenWAL(t, dir)
	appendMsg(t, wal, "m1", "one")
	if err := wal.Fsync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// legacy non-atomic writer left a torn offset file behind
	if err := os.WriteFile(filepath.Join(dir, "consumer.offset"), []byte(`{"segment":"seg-0000`), 0o644); err != nil {
		t.Fatal(err)
	}
	// and a stale temp file from a crashed atomic write
	if err := os.WriteFile(filepath.Join(dir, "consumer.offset.tmp"), []byte(`{"segment":`), 0o644); err != nil {
		t.Fatal(err)
	}

	wal2 := reopenWAL(t, dir)
	got, err := wal2.ReadNext()
	if err != nil {
		t.Fatalf("torn offset must not kill the edge: %v", err)
	}
	if got == nil || got.ID != "m1" {
		t.Fatalf("expected full replay from corrupt offset, got %#v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "consumer.offset.tmp")); !os.IsNotExist(err) {
		t.Fatalf("stale temp offset file not cleaned: %v", err)
	}
}

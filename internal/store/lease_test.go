package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Candidate 08 acceptance 2 (store half): the durable owner's lease is
// exclusive across independent owners — flock/LockFileEx conflict even
// between two handles of ONE process — and Release frees it with no residue.
// The lock lives on a sidecar next to the canonical file, so SQLite's own
// locking is untouched and acquiring never creates the store.
func TestOwnerLeaseExclusiveAndRelease(t *testing.T) {
	dir := t.TempDir()
	a := NewOwner(dir)
	b := NewOwner(dir)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	leaseA, err := a.Acquire("p")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Acquire("p")
	var busy *LeaseBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("second acquire error = %v, want *LeaseBusyError", err)
	}
	path, err := a.Path("p")
	if err != nil {
		t.Fatal(err)
	}
	if want := path + ".lock"; busy.Path != want {
		t.Fatalf("busy lock path = %q, want %q", busy.Path, want)
	}
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Fatalf("lock sidecar missing: %v", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("acquire created the store file (the lease must not touch SQLite)")
	}

	// Release is idempotent; the next owner can take the lease immediately —
	// no leftover "held" state.
	if err := leaseA.Release(); err != nil {
		t.Fatal(err)
	}
	if err := leaseA.Release(); err != nil {
		t.Fatal(err)
	}
	leaseB, err := b.Acquire("p")
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := leaseB.Release(); err != nil {
		t.Fatal(err)
	}

	// Different pipelines never conflict.
	leaseQ, err := a.Acquire("q")
	if err != nil {
		t.Fatalf("acquire other pipeline: %v", err)
	}
	if err := leaseQ.Release(); err != nil {
		t.Fatal(err)
	}
}

// The memory owner's lease is a no-op: single-process tests and --ephemeral
// runs must be unaffected by the single-writer rule.
func TestMemoryOwnerLeaseIsNoop(t *testing.T) {
	a := NewMemoryOwner()
	b := NewMemoryOwner()
	la, err := a.Acquire("p")
	if err != nil {
		t.Fatal(err)
	}
	lb, err := b.Acquire("p")
	if err != nil {
		t.Fatalf("memory owner lease conflicted: %v", err)
	}
	if err := la.Release(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Release(); err != nil {
		t.Fatal(err)
	}
}

// A lease is scoped to the canonical store name (sanitized pipeline name):
// the same pipeline spelled through a different owner still conflicts, while
// owners for other dataDir trees do not (different files).
func TestOwnerLeaseScopeFollowsCanonicalPath(t *testing.T) {
	dir := t.TempDir()
	a := NewOwner(dir)
	b := NewOwner(dir)
	other := NewOwner(t.TempDir())
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	defer func() { _ = other.Close() }()

	lease, err := a.Acquire("my pipeline")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Release() }()
	if _, err := b.Acquire("my pipeline"); err == nil {
		t.Fatal("same canonical store must conflict across owners")
	}
	if otherLease, err := other.Acquire("my pipeline"); err != nil {
		t.Fatalf("different data dir must not conflict: %v", err)
	} else {
		_ = otherLease.Release()
	}
	path, _ := a.Path("my pipeline")
	if want := filepath.Join(dir, "stores", "my_pipeline.db.lock"); path+".lock" != want {
		t.Fatalf("lock sidecar = %q, want %q", path+".lock", want)
	}
}

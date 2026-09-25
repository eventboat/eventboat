package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// Candidate 04 acceptance 2 + 6: one canonical layout for every pipeline,
// one cached handle per pipeline per process, Close closes everything and is
// idempotent.
func TestOwnerLayoutAndHandleLifetime(t *testing.T) {
	dir := t.TempDir()
	owner := NewOwner(dir)

	path, err := owner.Path("orders.sync")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "stores", "orders_sync.db"); path != want {
		t.Fatalf("Path = %q, want %q", path, want)
	}

	st1, err := owner.Open("orders.sync")
	if err != nil {
		t.Fatal(err)
	}
	st2, err := owner.Open("orders.sync")
	if err != nil {
		t.Fatal(err)
	}
	if st1 != st2 {
		t.Fatalf("Open returned two handles for one pipeline (%p != %p)", st1, st2)
	}
	if got := owner.OpenHandles(); got != 1 {
		t.Fatalf("OpenHandles = %d, want 1", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("store file missing at the exposed path: %v", err)
	}
	msg := registry.Message{ID: "m-1", Codec: "json", Raw: []byte(`{}`), Meta: map[string]any{}}
	if _, err := st1.AppendSpool("orders.sync", msg, time.Now()); err != nil {
		t.Fatal(err)
	}

	// A second pipeline gets its own handle and file (fault isolation).
	stB, err := owner.Open("other")
	if err != nil {
		t.Fatal(err)
	}
	if stB == st1 {
		t.Fatal("two pipelines share one handle")
	}
	if got := owner.OpenHandles(); got != 2 {
		t.Fatalf("OpenHandles = %d, want 2", got)
	}

	// Close closes every handle; the closed handles fail writes and a
	// second Close is a clean no-op.
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := owner.OpenHandles(); got != 0 {
		t.Fatalf("OpenHandles after Close = %d, want 0", got)
	}
	if _, err := st1.AppendSpool("orders.sync", msg, time.Now()); err == nil {
		t.Fatal("write on a closed handle succeeded")
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := owner.Open("orders.sync"); err == nil {
		t.Fatal("Open after Close succeeded")
	}
}

// The name flows into the store's file base name, so the owner applies the
// shared rules: sanitize, reject the Windows reserved device names, reject an
// empty result.
func TestOwnerNameRules(t *testing.T) {
	owner := NewOwner(t.TempDir())
	for _, name := range []string{"CON", "con", "PRN", "NUL", "COM1", "LPT9"} {
		if _, err := owner.Open(name); err == nil {
			t.Errorf("Open(%q) succeeded, want a reserved-name error", name)
		}
		if _, err := owner.Path(name); err == nil {
			t.Errorf("Path(%q) succeeded, want a reserved-name error", name)
		}
	}
	if _, err := owner.Open(""); err == nil {
		t.Error(`Open("") succeeded, want an empty-name error`)
	}
	if got := owner.OpenHandles(); got != 0 {
		t.Fatalf("rejected opens cached %d handles", got)
	}
	// A sanitizable name that is not reserved opens under the sanitized file.
	path, err := owner.Path("a.b/c")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(owner.dataDir, "stores", "a_b_c.db"); path != want {
		t.Fatalf("Path = %q, want %q", path, want)
	}
}

// The zero OwnerOptions means "store defaults": NewOwner must agree with
// OpenSQLite (group commit on), not silently normalize to write-through.
func TestOwnerDefaultWriteOptions(t *testing.T) {
	owner := NewOwner(t.TempDir())
	t.Cleanup(func() { _ = owner.Close() })
	st, err := owner.Open("p")
	if err != nil {
		t.Fatal(err)
	}
	sqlite, ok := st.(*SQLite)
	if !ok {
		t.Fatalf("Open returned %T, want *SQLite", st)
	}
	opts := sqlite.w.opts
	if opts.MaxRows != defaultWriteBatchRows || opts.MaxWait != defaultWriteBatchWait {
		t.Fatalf("owner default write options = %+v, want %d rows / %v", opts, defaultWriteBatchRows, defaultWriteBatchWait)
	}
}

// The memory owner caches per pipeline (the --ephemeral fix): every surface
// of one process sees the same data, and Path has no answer.
func TestMemoryOwnerCachesPerPipeline(t *testing.T) {
	owner := NewMemoryOwner()
	a1, err := owner.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	a2, err := owner.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	if a1 != a2 {
		t.Fatal("memory owner returned two stores for one pipeline")
	}
	b, err := owner.Open("b")
	if err != nil {
		t.Fatal(err)
	}
	if b == a1 {
		t.Fatal("memory owner shares one store across pipelines")
	}
	if err := a1.SetCheckpoint("a", 7); err != nil {
		t.Fatal(err)
	}
	if cp, err := a2.Checkpoint("a"); err != nil || cp != 7 {
		t.Fatalf("second surface sees checkpoint %d (err %v), want 7", cp, err)
	}
	if _, err := owner.Path("a"); err == nil {
		t.Fatal("memory owner Path succeeded, want an error")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if owner.OpenHandles() != 0 {
		t.Fatalf("OpenHandles after Close = %d, want 0", owner.OpenHandles())
	}
	if _, err := owner.Open("a"); err == nil {
		t.Fatal("Open after Close succeeded")
	}
}

// The owner must satisfy the provider interface ops depends on.
var _ Provider = (*Owner)(nil)

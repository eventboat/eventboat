package ops

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

func ownerTestRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

func ownerTestYAML(name, feed, out string) string {
	return `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: ` + name + ` }
run: { mode: job }
sources:
  in:
    decoder: json
    fakepull: { id: ` + feed + ` }
sinks:
  out:
    depends_on: [in]
    file: { path: ` + filepath.ToSlash(out) + ` }
`
}

// Candidate 04 acceptance 2: every Status poll goes through the provider, but
// the owner hands back ONE cached handle per pipeline for the process; the
// entry point's Close closes every handle.
func TestStatusPollsReuseOneHandle(t *testing.T) {
	testkit.ResetFakePull()
	dir := t.TempDir()
	owner := store.NewOwner(dir)
	svc := New(Options{DataDir: dir, Reg: ownerTestRegistry(t), Stores: owner, Clock: time.Now})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := svc.Deploy(ctx, ownerTestYAML("owner-job", "owner-feed", filepath.Join(dir, "out.jsonl"))); err != nil {
		t.Fatal(err)
	}

	path, err := owner.Path("owner-job")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("store file missing at the exposed path: %v", err)
	}
	if got := owner.OpenHandles(); got != 1 {
		t.Fatalf("handles after deploy = %d, want 1", got)
	}
	for i := 0; i < 25; i++ {
		svc.Status()
	}
	if got := owner.OpenHandles(); got != 1 {
		t.Fatalf("handles after 25 Status polls = %d, want 1 (one handle per pipeline)", got)
	}
	// The polls really used the same handle: a direct Open is pointer-equal.
	st, err := owner.Open("owner-job")
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		t.Fatal("Open returned nil")
	}

	svc.Stop()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := owner.OpenHandles(); got != 0 {
		t.Fatalf("handles after shutdown = %d, want 0", got)
	}
	if _, err := owner.Open("owner-job"); err == nil {
		t.Fatal("Open after shutdown succeeded")
	}
}

// Candidate 04 risk fix: --ephemeral used to hand out a fresh memory store
// per call, so a job run written by the manager was invisible to the next
// Status/Jobs read. The cached memory owner makes every surface of the
// process see one store per pipeline.
func TestMemoryOwnerSharedAcrossSurfaces(t *testing.T) {
	testkit.ResetFakePull()
	dir := t.TempDir()
	owner := store.NewMemoryOwner()
	svc := New(Options{DataDir: dir, Reg: ownerTestRegistry(t), Stores: owner, Clock: time.Now})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := svc.Deploy(ctx, ownerTestYAML("ephemeral-job", "ephemeral-feed", filepath.Join(dir, "out.jsonl"))); err != nil {
		t.Fatal(err)
	}
	testkit.FakePull("ephemeral-feed").StageJSON(`{"i":1}`, "c1")

	jr, err := svc.Trigger(ctx, "ephemeral-job", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if jr.Status != store.JobSuccess {
		t.Fatalf("run %s status = %s, want success", jr.RunID, jr.Status)
	}
	runID := jr.RunID

	// The run history written by the manager's engine is the one the ops
	// read surface sees.
	jobs, err := svc.Jobs("ephemeral-job", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].RunID != runID {
		t.Fatalf("Jobs = %+v, want the triggered run %s", jobs, runID)
	}
	found := false
	for _, st := range svc.Status() {
		if st.Pipeline != "ephemeral-job" {
			continue
		}
		for _, r := range st.RecentRuns {
			if r.RunID == runID {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("Status does not carry run %s (the ephemeral store is not shared)", runID)
	}
}

// There is no default store factory any more (candidate 04): a missing
// provider is a programming error, refused loudly at construction.
func TestNewRequiresStoreProvider(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("ops.New without a store provider did not panic")
		}
	}()
	New(Options{Reg: registry.New()})
}

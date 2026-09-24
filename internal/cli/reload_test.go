package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/store"
)

// Rethink follow-up 2026-09-24 (supplement D): the SIGHUP reload deploys the
// new or changed files only — an unchanged file is skipped, so a reload is
// not a drain-and-swap of everything.
func TestReloadDirDeploysChangedOnly(t *testing.T) {
	reg, err := commandRegistry()
	if err != nil {
		t.Fatal(err)
	}
	dir, dataDir := t.TempDir(), t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: reload }
sources:
  in:
    decoder: json
    file: { path: in.jsonl }
sinks:
  out:
    depends_on: [in]
    debug: {}
`)

	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := ops.New(ops.Options{DataDir: dataDir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)
	ctx := context.Background()

	if n, err := reloadDir(ctx, svc, dir, dataDir); err != nil || n != 1 {
		t.Fatalf("first reload = %d, %v; want 1, nil", n, err)
	}
	if n, err := reloadDir(ctx, svc, dir, dataDir); err != nil || n != 0 {
		t.Fatalf("unchanged reload = %d, %v; want 0, nil", n, err)
	}

	// A byte-level change redeploys (the comparison is the deployed copy).
	write(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: reload }
sources:
  in:
    decoder: json
    file: { path: in.jsonl, poll_every_ms: 500 }
sinks:
  out:
    depends_on: [in]
    debug: {}
`)
	if n, err := reloadDir(ctx, svc, dir, dataDir); err != nil || n != 1 {
		t.Fatalf("changed reload = %d, %v; want 1, nil", n, err)
	}

	// A broken file is reported but does not stop the healthy ones.
	write(`apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: reload }
sources: {}
sinks:
  out: { depends_on: [in], debug: {} }
`)
	if n, err := reloadDir(ctx, svc, dir, dataDir); err == nil {
		t.Fatalf("broken reload = %d, nil; want the verify error", n)
	}
}

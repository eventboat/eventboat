package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 04 acceptance 1: one layout. A run history written by the
// `trigger` verb is the one `jobs list` and the daemon's Status read, because
// every entry point opens the same canonical file,
// dataDir/stores/<sanitized pipeline>.db.
func TestCrossEntryStoreLayout(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()

	input := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(input, []byte("{\"i\":1}\n{\"i\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pipeline := filepath.Join(dir, "pipeline.yaml")
	yaml := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: xentry }
run: { mode: job }
sources:
  in:
    decoder: json
    file: { path: ` + filepath.ToSlash(input) + `, on_eof: stop }
sinks:
  out:
    depends_on: [in]
    file: { path: ` + filepath.ToSlash(filepath.Join(dir, "out.jsonl")) + ` }
`
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")

	// 1. The one-shot trigger writes the run history.
	var jr struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(runCLISubprocess(t, bin, root, "trigger",
		"--config", pipeline, "--data-dir", dataDir, "--json")), &jr); err != nil {
		t.Fatal(err)
	}
	if jr.Status != "success" || jr.RunID == "" {
		t.Fatalf("trigger run = %+v, want a successful run id", jr)
	}

	// 2. `jobs list` reads the same history through the same layout.
	var runs []map[string]any
	if err := json.Unmarshal([]byte(runCLISubprocess(t, bin, root, "jobs", "list",
		"--config", pipeline, "--data-dir", dataDir, "--json")), &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0]["run_id"] != jr.RunID {
		t.Fatalf("jobs list = %+v, want the trigger run %s", runs, jr.RunID)
	}

	// 3. The daemon's Status reads the same history: deploy the same config
	// through an ops service over the same data dir (the --config-dir path).
	reg, err := commandRegistry()
	if err != nil {
		t.Fatal(err)
	}
	owner := store.NewOwner(dataDir)
	svc := ops.New(ops.Options{DataDir: dataDir, Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })

	content, err := os.ReadFile(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Deploy(context.Background(), string(content)); err != nil {
		t.Fatal(err)
	}
	found := false
	deadline := time.Now().Add(10 * time.Second)
	for !found && time.Now().Before(deadline) {
		for _, st := range svc.Status() {
			for _, r := range st.RecentRuns {
				if r.RunID == jr.RunID {
					found = true
				}
			}
		}
		if !found {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !found {
		t.Fatalf("daemon Status does not carry trigger run %s (different store file)", jr.RunID)
	}

	// All three entry points used the canonical layout.
	path, err := owner.Path("xentry")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dataDir, "stores", "xentry.db"); path != want {
		t.Fatalf("store path = %q, want %q", path, want)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("canonical store file missing: %v", err)
	}
	// The retired layouts are not created.
	for _, old := range []string{filepath.Join(dataDir, "eventboat.db"), filepath.Join(dataDir, "stores", "pipeline.db")} {
		if _, err := os.Stat(old); err == nil {
			t.Errorf("retired store layout written: %s", old)
		}
	}
}

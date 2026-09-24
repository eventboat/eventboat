package ops

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 02 acceptance 6: the batch status machine derives from the engine
// outcome. A completed batch lands on "completed"; a batch whose engine
// stopped itself (a failed source) lands on "failed" with the error — never
// running + err, and without the old 100ms settle window.

func batchYAML(name, input, output string) string {
	return fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: %s }
run: { mode: batch }
sources:
  in:
    decoder: json
    file: { path: %s, poll_every_ms: 10, on_eof: stop }
sinks:
  out:
    depends_on: [in]
    encoder: json
    file: { path: %s }
`, name, filepath.ToSlash(input), filepath.ToSlash(output))
}

func TestBatchStatusFromOutcome(t *testing.T) {
	dir := t.TempDir()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	svc := New(Options{
		DataDir: dir,
		Reg:     reg,
		Stores:  store.NewMemoryOwner(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Completed: the file source exhausts (on_eof: stop) and everything
	// commits; the watcher publishes completed.
	input := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(input, []byte("{\"i\":1}\n{\"i\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Deploy(ctx, batchYAML("batch-ok", input, filepath.Join(dir, "out.jsonl"))); err != nil {
		t.Fatal(err)
	}
	waitForBatchStatus(t, svc, "batch-ok", "completed", false)

	// Failed: a missing input under on_eof: stop is a loud source failure,
	// the engine stops itself and the status is failed (with the error).
	if _, err := svc.Deploy(ctx, batchYAML("batch-bad", filepath.Join(dir, "absent.jsonl"), filepath.Join(dir, "out2.jsonl"))); err != nil {
		t.Fatal(err)
	}
	waitForBatchStatus(t, svc, "batch-bad", "failed", true)

	svc.Stop()
}

func waitForBatchStatus(t *testing.T, svc *Service, name, want string, wantErr bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, st := range svc.Status() {
			if st.Pipeline != name || st.Status != want {
				continue
			}
			if wantErr && st.Error == "" {
				t.Fatalf("pipeline %s is %s without an error", name, want)
			}
			if !wantErr && st.Error != "" {
				t.Fatalf("pipeline %s is %s with error %q", name, want, st.Error)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, st := range svc.Status() {
		if st.Pipeline == name {
			t.Fatalf("pipeline %s status = %q (err %q), want %q", name, st.Status, st.Error, want)
		}
	}
	t.Fatalf("pipeline %s never appeared in status", name)
}

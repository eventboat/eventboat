package cli

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
)

// leaseJobPipeline writes the job pipeline both lease subprocess tests use: a
// finite file source with a file sink.
func leaseJobPipeline(t *testing.T, dir string) string {
	t.Helper()
	input := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(input, []byte("{\"i\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pipeline := filepath.Join(dir, "pipeline.yaml")
	yaml := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: lease-job }
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
	return pipeline
}

// Candidate 08 acceptance 2: a one-shot verb REFUSES with a clear message
// (pointing at the admin/MCP surface) while another process holds the
// pipeline's store lease — and the lease is released when that process exits
// (the test process re-acquires immediately; no residue).
func TestCLIStoreLeaseRefusalAndExitRelease(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()
	pipeline := leaseJobPipeline(t, dir)
	dataDir := filepath.Join(dir, "data")

	// The "daemon": this test process holds the pipeline lease.
	owner := store.NewOwner(dataDir)
	defer func() { _ = owner.Close() }()
	lease, err := owner.Acquire("lease-job")
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "trigger", "--config", pipeline, "--data-dir", dataDir, "--json")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("trigger ran while the daemon held the store lease:\n%s", out)
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("lease refusal exit code = %d, want 1:\n%s", code, out)
	}
	for _, want := range []string{"lease", "admin/MCP", "trigger"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("refusal message does not mention %q:\n%s", want, out)
		}
	}

	// Release: the one-shot verb runs, and its exit releases the lease (the
	// test process takes it again immediately — no residue).
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	rerun := runCLISubprocess(t, bin, root, "trigger", "--config", pipeline, "--data-dir", dataDir, "--json")
	if !strings.Contains(rerun, `"status":"success"`) {
		t.Fatalf("trigger after release: %s", rerun)
	}
	lease2, err := owner.Acquire("lease-job")
	if err != nil {
		t.Fatalf("lease not released when the subprocess exited: %v", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatal(err)
	}
}

// Candidate 08 acceptance 2 (crash half): killing a running engine without
// any cleanup releases the OS lock with the process — a subsequent run can
// take the lease immediately.
func TestCLIStoreLeaseCrashRelease(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()
	// A continuous tailing source over a never-created file: the run lives
	// until it is killed.
	pipeline := filepath.Join(dir, "crash.yaml")
	yaml := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: crash-job }
sources:
  in:
    decoder: json
    file: { path: ` + filepath.ToSlash(filepath.Join(dir, "absent.jsonl")) + `, poll_every_ms: 50 }
sinks:
  out:
    depends_on: [in]
    file: { path: ` + filepath.ToSlash(filepath.Join(dir, "out.jsonl")) + ` }
`
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")

	cmd := exec.Command(bin, "run", "--config", pipeline, "--data-dir", dataDir)
	cmd.Dir = root
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	killed := false
	defer func() {
		if !killed {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}()

	// The startup line is printed AFTER the lease is acquired, so seeing it
	// proves the subprocess holds the lock — no acquire-polling race that
	// could steal the lock from the subprocess's one attempt.
	started := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "running pipeline") {
				close(started)
				return
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("run subprocess never reported startup (and took the lease)")
	}

	owner := store.NewOwner(dataDir)
	defer func() { _ = owner.Close() }()
	if lease, err := owner.Acquire("crash-job"); err == nil {
		_ = lease.Release()
		t.Fatal("lease was free while the run subprocess was live")
	}

	// Hard kill: no signal handling, no deferred release. The kernel drops
	// the lock with the process.
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
	killed = true
	deadline := time.Now().Add(20 * time.Second)
	for {
		lease, err := owner.Acquire("crash-job")
		if err == nil {
			_ = lease.Release()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease still held after the process was killed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

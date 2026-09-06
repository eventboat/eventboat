// The custom-build acceptance gate: examples/custom-build is a separate Go
// module that uses only the root package's RunCLI and pkg/plugin. This test
// builds it the way an outside user would and drives the binary through the
// plugin catalog, the static verify gate and a real engine run (the run
// verb is a long-lived daemon, so the test polls the sink's output file and
// stops the process once the events arrived).
package eventboat_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const customBuildDir = "examples/custom-build"

func TestCustomBuildAcceptance(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "my-eventboat")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = customBuildDir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build custom-build: %v\n%s", err, out)
	}

	work := t.TempDir()
	input := filepath.Join(work, "orders.jsonl")
	if err := os.WriteFile(input, []byte("{\"id\":\"o-1\",\"qty\":2}\n{\"id\":\"o-2\",\"qty\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(work, "echoed.out.jsonl")
	pipeline := filepath.Join(work, "myecho.pipeline.yaml")
	yaml := fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: custom-build-acceptance }
sources:
  ingest:
    decoder: json
    file:
      path: %s
sinks:
  echoed:
    depends_on: [ingest]
    encoder: json
    myecho:
      path: %s
`, filepath.ToSlash(input), filepath.ToSlash(output))
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) string {
		cmd := exec.Command(bin, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", bin, args, err, out)
		}
		return string(out)
	}

	// The registered plugin is a first-class catalog member, like any
	// built-in.
	if catalog := run("--json", "plugin", "catalog"); !strings.Contains(catalog, `"myecho"`) {
		t.Errorf("plugin catalog misses myecho:\n%s", catalog)
	}

	// The static gate validates the plugin block against the schema
	// generated from the plugin's config struct.
	run("verify", "--config", pipeline)

	// A real run through the plugin sink.
	data := filepath.Join(work, "data")
	cmd := exec.Command(bin, "run", "--config", pipeline, "--ephemeral", "--data-dir", data)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	deadline := time.Now().Add(30 * time.Second)
	for {
		b, err := os.ReadFile(output)
		if err == nil && strings.Count(string(b), "\n") >= 2 {
			first := strings.SplitN(string(b), "\n", 2)[0]
			var event map[string]any
			if err := json.Unmarshal([]byte(first), &event); err != nil {
				t.Fatalf("sink output is not JSON lines: %v\n%s", err, b)
			}
			if event["id"] != "o-1" {
				t.Errorf("first event id = %v, want o-1", event["id"])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink output never reached 2 lines; have:\n%s", b)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestBatchRunCompletion drives the run.mode: batch contract end to end
// (v1.24): a pipeline over a complete file EXITS BY ITSELF with the
// job-aligned exit codes — 0 on success, 1 when a source fails.
func TestBatchRunCompletion(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "eventboat")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	// The shipped binary lives in ./cmd/eventboat (the root is the library).
	if out, err := exec.Command("go", "build", "-o", bin, "./cmd/eventboat").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	writePipeline := func(dir, sourceBlock string) string {
		yaml := fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: batch-drain }
run: { mode: batch }
sources:
  in:
    decoder: json
    file:
%s
sinks:
  out:
    depends_on: [in]
    encoder: json
    file: { path: %s }
`, sourceBlock, filepath.ToSlash(filepath.Join(dir, "output", "out.jsonl")))
		path := filepath.Join(dir, "pipeline.yaml")
		if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	// Success: three lines in, the process terminates on its own with 0.
	work := t.TempDir()
	input := filepath.Join(work, "in.jsonl")
	if err := os.WriteFile(input, []byte("{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pipeline := writePipeline(work, fmt.Sprintf("      path: %s\n      poll_every_ms: 10\n      on_eof: stop", filepath.ToSlash(input)))
	cmd := exec.Command(bin, "run", "--config", pipeline, "--data-dir", filepath.Join(work, "data"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("batch run failed: %v\n%s", err, out)
	}
	b, _ := os.ReadFile(filepath.Join(work, "output", "out.jsonl"))
	if strings.Count(string(b), "\n") != 3 {
		t.Fatalf("output = %q, want 3 lines", b)
	}

	// Missing input under stop mode: fail loudly, exit 1.
	work2 := t.TempDir()
	pipeline2 := writePipeline(work2, fmt.Sprintf("      path: %s\n      poll_every_ms: 10\n      on_eof: stop", filepath.ToSlash(filepath.Join(work2, "absent.jsonl"))))
	cmd2 := exec.Command(bin, "run", "--config", pipeline2, "--data-dir", filepath.Join(work2, "data"))
	out2, err2 := cmd2.CombinedOutput()
	if err2 == nil {
		t.Fatalf("batch run over a missing file exited 0:\n%s", out2)
	}

	// The job route over the same shape: trigger completes with exit 0.
	work3 := t.TempDir()
	input3 := filepath.Join(work3, "in.jsonl")
	if err := os.WriteFile(input3, []byte("{\"i\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	jobYAML := fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: file-job }
run:
  mode: job
  retention:
    history: 1h
sources:
  in:
    decoder: json
    file:
      path: %s
      poll_every_ms: 10
      on_eof: stop
sinks:
  out:
    depends_on: [in]
    encoder: json
    file: { path: %s }
`, filepath.ToSlash(input3), filepath.ToSlash(filepath.Join(work3, "output", "out.jsonl")))
	jobPath := filepath.Join(work3, "job.yaml")
	if err := os.WriteFile(jobPath, []byte(jobYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd3 := exec.Command(bin, "trigger", "--config", jobPath, "--data-dir", filepath.Join(work3, "data"))
	out3, err3 := cmd3.CombinedOutput()
	if err3 != nil {
		t.Fatalf("trigger failed: %v\n%s", err3, out3)
	}
	if !strings.Contains(string(out3), "success") {
		t.Fatalf("trigger output missing success:\n%s", out3)
	}
}

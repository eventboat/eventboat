package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var (
	buildOnce  sync.Once
	binaryPath string
	buildErr   error
)

// buildBinary compiles the CLI once for subprocess-level tests.
func buildBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		exe, err := os.CreateTemp("", "eventboat-test-*.exe")
		if err != nil {
			buildErr = err
			return
		}
		_ = exe.Close()
		cmd := exec.Command("go", "build", "-o", exe.Name(), "../../cmd/eventboat")
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			buildErr = err
			return
		}
		binaryPath = exe.Name()
	})
	if buildErr != nil {
		t.Fatalf("build binary: %v", buildErr)
	}
	t.Cleanup(func() {})
	return binaryPath
}

// The job CLI acceptance: a backfill trigger with explicit parameters, then
// an incremental trigger that resumes from the committed watermark, then
// jobs list/show over the durable history — all against the real sqlite sql
// source (§5.8 acceptance).
func TestJobTriggerAndHistoryCLI(t *testing.T) {
	build := buildBinary(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "sync.jsonl")
	outDir := t.TempDir()

	// The example pipeline with a temp output path (the committed one writes
	// into examples/); the source DB ships with the repo.
	example, err := os.ReadFile(filepath.Join("..", "..", "examples", "job-sync", "pipeline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	pipeline := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(pipeline, []byte(strings.Replace(string(example),
		"./examples/job-sync/out/orders-sync.jsonl", filepath.ToSlash(out), 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(outDir, "data")

	// The pipeline's dsn is relative to the repo root; run from there.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(filepath.Dir(cwd))
	runIn := func(t *testing.T, args ...string) string {
		t.Helper()
		cmd := exec.Command(build, args...)
		cmd.Dir = root
		outb, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("eventboat %s: %v\n%s", strings.Join(args, " "), err, outb)
		}
		return string(outb)
	}

	// Day-1 backfill.
	var jr struct {
		RunID    string `json:"run_id"`
		Status   string `json:"status"`
		RowsRead int64  `json:"rows_read"`
	}
	if err := json.Unmarshal([]byte(runIn(t, "trigger", "--config", pipeline,
		"--parameters", `{"from":"2026-09-01T00:00:00Z","to":"2026-09-02T00:00:00Z"}`,
		"--data-dir", dataDir, "--json")), &jr); err != nil {
		t.Fatal(err)
	}
	if jr.Status != "success" || jr.RowsRead != 3 {
		t.Fatalf("backfill run = %+v (want 3 rows, success)", jr)
	}

	// Incremental: from=cursor resumes after the day-1 watermark; to=now
	// covers day 2. Exactly the remaining three rows, no duplicates.
	if err := json.Unmarshal([]byte(runIn(t, "trigger", "--config", pipeline, "--data-dir", dataDir, "--json")), &jr); err != nil {
		t.Fatal(err)
	}
	if jr.Status != "success" || jr.RowsRead != 3 {
		t.Fatalf("incremental run = %+v (want 3 rows from watermark)", jr)
	}

	// The sink file holds all six orders exactly once (incremental resumed
	// from the committed watermark).
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "\n"); got != 6 {
		t.Fatalf("sink file has %d lines, want 6:\n%s", got, data)
	}

	// History lists both runs; show prints parameters and counts.
	var runs []map[string]any
	if err := json.Unmarshal([]byte(runIn(t, "jobs", "list", "--config", pipeline, "--data-dir", dataDir, "--json")), &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("history has %d runs, want 2", len(runs))
	}
	firstRunID, _ := runs[len(runs)-1]["run_id"].(string)
	detail := runIn(t, "jobs", "show", firstRunID, "--config", pipeline, "--data-dir", dataDir, "--json")
	if !strings.Contains(detail, "from") || !strings.Contains(detail, "2026-09-01T00:00:00Z") {
		t.Fatalf("jobs show lost the backfill parameters: %s", detail)
	}
}

// trigger takes no positional arguments: the pipeline comes from --config,
// and stray arguments are an error ("unknown is an error" discipline).
func TestTriggerRejectsStrayPositional(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	cmd := exec.Command(bin, "trigger", "--config", "examples/job-sync/pipeline.yaml", "stray-pipeline-name")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("trigger accepted a stray positional argument:\n%s", out)
	}
	if cmd.ProcessState.ExitCode() != 2 {
		t.Fatalf("exit code = %d, want 2:\n%s", cmd.ProcessState.ExitCode(), out)
	}
	if !strings.Contains(string(out), "unexpected argument") {
		t.Fatalf("error message does not name the stray argument:\n%s", out)
	}
}

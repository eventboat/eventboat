package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
)

// explain: message-level walkthrough over a real example (deterministic
// output, dry-run of the script, real CEL evaluation).
func TestExplainCLIMessageLevel(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	out := runCLISubprocess(t, bin, root, "explain",
		"--config", "examples/branching/pipeline.yaml",
		"--message", "examples/branching/tests/fixtures/eu-order.json")
	for _, want := range []string{
		`enters at node "ingest"`,
		"transform.script",
		"✓ MATCH",
		"exhausted → dead letter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("explain output missing %q:\n%s", want, out)
		}
	}

	topo := runCLISubprocess(t, bin, root, "explain",
		"--config", "examples/branching/pipeline.yaml", "--topology", "--json")
	var parsed struct {
		Mermaid string `json:"mermaid"`
		ASCII   string `json:"ascii"`
	}
	if err := json.Unmarshal([]byte(topo), &parsed); err != nil {
		t.Fatalf("topology json: %v\n%s", err, topo)
	}
	if !strings.Contains(parsed.Mermaid, "flowchart LR") || !strings.Contains(parsed.ASCII, "ingest → enrich") {
		t.Errorf("topology renderings incomplete:\n%s\n%s", parsed.Mermaid, parsed.ASCII)
	}
}

// replay --dlq: seed two dead letters (one matching a --where filter), replay
// the matching one into the pipeline's real file sink, verify delivery with
// is_replay + preserved message_id, and --delete removes it from the store.
func TestReplayCLIDeadLetters(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()

	pipeline := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(pipeline, []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: replayme }
sources:
  in:
    decoder: json
    file: { path: nonexistent-input.jsonl }
transforms:
  t:
    from: [in]
    script: |
      payload.replayed = True
sinks:
  out:
    from: [t]
    file: { path: `+filepath.ToSlash(filepath.Join(dir, "out.jsonl"))+` }
`), 0o644); err != nil {
		t.Fatal(err)
	}

	// Seed the store with two dead letters at node t (the transform).
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := store.OpenSQLite(filepath.Join(dataDir, "eventboat.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WriteDeadLetter(store.DeadLetter{
		Pipeline: "replayme", MessageID: "want-1", Node: "t", Reason: "script: bug",
		Raw: []byte(`{"k":"keep"}`), Meta: map[string]any{"job_run_id": "r1"}, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteDeadLetter(store.DeadLetter{
		Pipeline: "replayme", MessageID: "skip-1", Node: "t", Reason: "script: bug",
		Raw: []byte(`{"k":"drop"}`), Meta: map[string]any{}, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	out := runCLISubprocess(t, bin, root, "replay",
		"--config", pipeline, "--dlq", `--where`, `payload.k == "keep"`,
		"--data-dir", dataDir, "--delete", "--json")
	if !strings.Contains(out, `"replayed":1`) {
		t.Fatalf("replay output: %s", out)
	}

	// The sink file holds exactly the filtered message, transformed, with the
	// replay stamps.
	data, err := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("sink lines = %d, want 1:\n%s", len(lines), data)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["replayed"] != true {
		t.Errorf("transform did not run on reinjection: %s", lines[0])
	}

	// The replayed dead letter is gone; the filtered one remains.
	st2, err := store.OpenSQLite(filepath.Join(dataDir, "eventboat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	dls, err := st2.DeadLetters("replayme")
	if err != nil || len(dls) != 1 || dls[0].MessageID != "skip-1" {
		t.Fatalf("dead letters after --delete: %+v (%v)", dls, err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(filepath.Dir(cwd))
}

func runCLISubprocess(t *testing.T, bin, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("eventboat %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

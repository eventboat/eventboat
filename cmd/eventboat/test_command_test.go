package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Directory mode must recurse, run only files that declare a suite (a
// top-level `suite:` key), silently skip other YAML (pipelines, unrelated
// files), report the skipped count, and exit 0.
func TestTestCommandDirectoryMode(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "pipeline.yaml"), `apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: mixed }
sources:
  ingest:
    decoder: json
    file: { path: input/x.jsonl }
transforms:
  t:
    from: [ingest]
    script: |
      payload.seen = True
sinks:
  out:
    from: [t]
    drop: {}
`)
	write(t, filepath.Join(dir, "notes.yaml"), "# just some shared YAML, not a suite\nanchors: {a: 1}\n")
	write(t, filepath.Join(dir, "tests", "mixed.yaml"), `suite: mixed-dir
pipeline: ../pipeline.yaml
cases:
  - name: order-passes-through
    inject:
      at: ingest
      messages:
        - fixtures/order.json
    expect:
      capture:
        at: out
        messages:
          - payload.seen: true
`)
	write(t, filepath.Join(dir, "tests", "fixtures", "order.json"), `{"n": 1}
`)

	var human bytes.Buffer
	code := cmdTest([]string{dir}, false, &human)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; output:\n%s", code, human.String())
	}
	out := human.String()
	if !strings.Contains(out, "suite mixed-dir") {
		t.Errorf("suite in nested tests/ dir was not executed; output:\n%s", out)
	}
	if !strings.Contains(out, "PASS") || !strings.Contains(out, "order-passes-through") {
		t.Errorf("case not reported as passed; output:\n%s", out)
	}
	if !strings.Contains(out, "skipped 2") {
		t.Errorf("expected 2 skipped non-suite yaml files (pipeline.yaml, notes.yaml); output:\n%s", out)
	}

	var js bytes.Buffer
	code = cmdTest([]string{dir}, true, &js)
	if code != 0 {
		t.Fatalf("json mode exit code = %d, want 0; output:\n%s", code, js.String())
	}
	var payload struct {
		Skipped int              `json:"skipped"`
		Suites  []testOutputJSON `json:"suites"`
	}
	if err := json.Unmarshal(js.Bytes(), &payload); err != nil {
		t.Fatalf("json output not parseable: %v\n%s", err, js.String())
	}
	if payload.Skipped != 2 {
		t.Errorf("json skipped = %d, want 2", payload.Skipped)
	}
	if len(payload.Suites) != 1 || payload.Suites[0].Suite != "mixed-dir" || !payload.Suites[0].OK {
		t.Errorf("json suites = %+v, want the executed mixed-dir suite", payload.Suites)
	}
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

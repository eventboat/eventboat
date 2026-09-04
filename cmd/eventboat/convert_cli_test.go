package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The convert CLI surface: file in, v3 config + report out, exit 0 on a
// verifying conversion. Exercises the command plumbing (flag handling with
// the positional first) rather than conversion semantics (covered by
// internal/convert).
func TestConvertCLI(t *testing.T) {
	t.Setenv("WEBHOOK_TARGET_URL", "https://httpbin.example/post")
	src := filepath.Join("..", "..", "legacy", "_examples", "04-http-webhook.yaml")
	out := filepath.Join(t.TempDir(), "out.yaml")
	report := filepath.Join(t.TempDir(), "report.md")

	if code := cmdConvert([]string{src, "-o", out, "--report", report}, false); code != 0 {
		t.Fatalf("convert exit code = %d, want 0", code)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("output not written: %v", err)
	}
	rep, err := os.ReadFile(report)
	if err != nil {
		t.Fatalf("report not written: %v", err)
	}
	if len(rep) == 0 || string(rep[:1]) != "#" {
		t.Fatalf("report does not look like markdown: %q", rep[:40])
	}

	// A conversion whose output does not verify exits 1 (here: an unknown
	// encoder type survives passthrough and verify flags it).
	broken := filepath.Join(t.TempDir(), "broken.yaml")
	if err := os.WriteFile(broken, []byte("apiVersion: riverpod/v1\nkind: Pipeline\nmetadata: {name: b}\nsteps:\n  a:\n    source: {type: cron, config: {schedule: \"0 0 * * * *\"}}\n  b:\n    depends_on: [a]\n    sink: {type: drop, encoder: {type: nonexistent}}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	brokenOut := filepath.Join(t.TempDir(), "broken-out.yaml")
	if code := cmdConvert([]string{broken, "-o", brokenOut}, false); code != 1 {
		t.Fatalf("convert exit code = %d, want 1 (verify must fail)", code)
	}

	// Usage errors exit 2.
	if code := cmdConvert([]string{}, false); code != 2 {
		t.Fatalf("convert with no args exit = %d, want 2", code)
	}
}

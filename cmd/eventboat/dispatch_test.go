package main

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// runBin executes the built binary with separated stdout/stderr and returns
// both plus the exit code (the dispatch contract: data on stdout,
// diagnostics and usage hints on stderr, exit code 0/1/2).
func runBin(t *testing.T, dir string, args ...string) (string, string, int) {
	t.Helper()
	bin := buildBinary(t)
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	code := 0
	if runErr != nil {
		ee, ok := runErr.(*exec.ExitError)
		if !ok {
			t.Fatalf("eventboat %s: %v", strings.Join(args, " "), runErr)
		}
		code = ee.ExitCode()
	}
	return stdout.String(), stderr.String(), code
}

// --json is a global root bool (before the verb) with a verb-level fallback
// (after it): both spellings produce identical machine output on stdout.
func TestJSONGlobalAndVerbSpellings(t *testing.T) {
	root := repoRoot(t)
	base := []string{"verify", "--config", "examples/branching/pipeline.yaml"}

	before, _, codeBefore := runBin(t, root, append([]string{"--json"}, base...)...)
	after, _, codeAfter := runBin(t, root, append(base, "--json")...)
	if codeBefore != 0 || codeAfter != 0 {
		t.Fatalf("exit codes %d/%d, want 0/0", codeBefore, codeAfter)
	}
	if before != after {
		t.Fatalf("--json before/after the verb differ:\n%s\n---\n%s", before, after)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(before), &out); err != nil {
		t.Fatalf("verify --json is not JSON: %v\n%s", err, before)
	}
	if !out.OK {
		t.Fatalf("example pipeline failed verify:\n%s", before)
	}
}

// A verb invoked without its required flags exits 2 with a usage hint on
// stderr (the UsageError contract replacing the bare handwritten return 2).
func TestUsageErrorExitAndHint(t *testing.T) {
	_, stderr, code := runBin(t, repoRoot(t), "trigger")
	if code != 2 {
		t.Fatalf("trigger exit = %d, want 2:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "trigger: --config is required") {
		t.Fatalf("stderr lacks the diagnostic:\n%s", stderr)
	}
	if !strings.Contains(stderr, "usage:") {
		t.Fatalf("stderr lacks a usage hint line:\n%s", stderr)
	}
}

// jobs with a missing or unknown subcommand is a usage error (exit 2, hint).
func TestJobsSubcommandUsageError(t *testing.T) {
	root := repoRoot(t)
	for _, args := range [][]string{{"jobs"}, {"jobs", "bogus"}} {
		_, stderr, code := runBin(t, root, args...)
		if code != 2 {
			t.Fatalf("eventboat %s exit = %d, want 2:\n%s", strings.Join(args, " "), code, stderr)
		}
		if !strings.Contains(stderr, "usage:") {
			t.Fatalf("eventboat %s stderr lacks a usage hint:\n%s", strings.Join(args, " "), stderr)
		}
	}
}

// An unknown verb exits 2 and appends the command list so the miss is
// self-explaining.
func TestUnknownVerbListsCommands(t *testing.T) {
	_, stderr, code := runBin(t, repoRoot(t), "no-such-verb")
	if code != 2 {
		t.Fatalf("exit = %d, want 2:\n%s", code, stderr)
	}
	for _, want := range []string{"unknown verb", "verify", "trigger", "mcp"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr)
		}
	}
}

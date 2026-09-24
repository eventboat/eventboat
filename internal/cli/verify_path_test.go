package cli

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/verify"
)

// Candidate 05 acceptance 1: the CLI report and the ops (MCP/Admin) report
// are the same composition — same diagnostic sequence, same strict verdict.
func TestVerifyCompositionParity(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()
	guest := filepath.Join(root, "internal", "wasmhost", "testdata", "aggregate.wasm")
	pipeline := filepath.Join(dir, "p.yaml")
	content := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: parity }
sources:
  in: { file: { path: input.jsonl, on_eof: stop } }
transforms:
  t: { depends_on: [in], wasm: { module: ` + filepath.ToSlash(guest) + ` } }
sinks:
  out: { depends_on: [t], debug: {} }
`
	if err := os.WriteFile(pipeline, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	out := runCLISubprocess(t, bin, root, "verify", "--config", pipeline, "--json")
	var cliOut verifyOutput
	if err := json.Unmarshal([]byte(out), &cliOut); err != nil {
		t.Fatalf("verify --json: %v\n%s", err, out)
	}
	if !cliOut.OK || len(cliOut.Diagnostics) == 0 {
		t.Fatalf("fixture should pass non-strict with the kill-switch warning: %+v", cliOut)
	}

	reg, err := commandRegistry()
	if err != nil {
		t.Fatal(err)
	}
	svc := ops.New(ops.Options{Reg: reg, Stores: store.NewMemoryOwner()})
	mcpDiags := svc.Verify(content).Diagnostics
	// The File field names the entry (a path vs the pure-text submission
	// name); every other field must be identical, in order.
	normalize := func(diags config.Diagnostics) []config.Diagnostic {
		out := append([]config.Diagnostic(nil), diags...)
		for i := range out {
			out[i].File = ""
		}
		return out
	}
	if !reflect.DeepEqual(normalize(cliOut.Diagnostics), normalize(mcpDiags)) {
		t.Fatalf("CLI diagnostics differ from ops.Verify:\nCLI: %+v\nops: %+v", cliOut.Diagnostics, mcpDiags)
	}
	fileDiags := verify.File(pipeline, reg, verify.Options{}).Diagnostics
	if !reflect.DeepEqual(normalize(cliOut.Diagnostics), normalize(fileDiags)) {
		t.Fatalf("CLI diagnostics differ from verify.File:\nCLI: %+v\nfile: %+v", cliOut.Diagnostics, fileDiags)
	}

	// --strict: same diagnostics, verdict flips (warnings are errors).
	strictOut, code := runCLIAllowFail(t, bin, root, "verify", "--config", pipeline, "--strict", "--json")
	if code != 1 {
		t.Fatalf("verify --strict exit = %d, want 1\n%s", code, strictOut)
	}
	var strictParsed verifyOutput
	if err := json.Unmarshal([]byte(strictOut), &strictParsed); err != nil {
		t.Fatal(err)
	}
	if strictParsed.OK {
		t.Fatal("strict verdict must fail on a warning")
	}
	if !reflect.DeepEqual([]config.Diagnostic(strictParsed.Diagnostics), []config.Diagnostic(cliOut.Diagnostics)) {
		t.Fatalf("strict diagnostics differ:\n%+v\n%+v", strictParsed.Diagnostics, cliOut.Diagnostics)
	}
	if strict := verify.File(pipeline, reg, verify.Options{Strict: true}); strict.OK {
		t.Fatal("verify.File strict verdict must match the CLI")
	}
}

// Candidate 05 acceptance 5: explain's --at resolves the entry node through
// the same ops entry on the CLI and on MCP (`svc.Explain`).
func TestExplainAtParity(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()
	pipeline := filepath.Join(dir, "p.yaml")
	content := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: explain-at }
sources:
  first:  { file: { path: a.jsonl } }
  second: { file: { path: b.jsonl } }
transforms:
  t: { depends_on: [first, second], script: "payload.seen = True" }
sinks:
  out: { depends_on: [t], debug: {} }
`
	if err := os.WriteFile(pipeline, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	msg := filepath.Join(dir, "m.json")
	if err := os.WriteFile(msg, []byte(`{"k":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	defaultOut := runCLISubprocess(t, bin, root, "explain", "--config", pipeline, "--message", msg)
	if !strings.Contains(defaultOut, `enters at node "first"`) {
		t.Fatalf("default entry should be the first declared source:\n%s", defaultOut)
	}
	atOut := runCLISubprocess(t, bin, root, "explain", "--config", pipeline, "--message", msg, "--at", "second")
	if !strings.Contains(atOut, `enters at node "second"`) {
		t.Fatalf("--at second ignored:\n%s", atOut)
	}

	reg, err := commandRegistry()
	if err != nil {
		t.Fatal(err)
	}
	svc := ops.New(ops.Options{Reg: reg, Stores: store.NewMemoryOwner()})
	mcpOut, err := svc.Explain(content, ops.ExplainRequest{Message: []byte(`{"k":1}`), EntryNode: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if atOut != mcpOut {
		t.Fatalf("CLI --at and MCP at diverge:\nCLI:\n%s\nMCP:\n%s", atOut, mcpOut)
	}
}

// Candidate 05 acceptance 3: `--ids` with `--limit` replays and deletes only
// the selected rows; ids beyond the limit stay in the store.
func TestReplayIDsLimitKeepsUnreplayed(t *testing.T) {
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()
	pipeline := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(pipeline, []byte(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: replay-limit }
sources:
  in:
    decoder: json
    file: { path: nonexistent-input.jsonl }
sinks:
  out:
    depends_on: [in]
    file: { path: `+filepath.ToSlash(filepath.Join(dir, "out.jsonl"))+` }
`), 0o644); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")
	owner := store.NewOwner(dataDir)
	st, err := owner.Open("replay-limit")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b", "c"} {
		if err := st.WriteDeadLetter(store.DeadLetter{
			Pipeline: "replay-limit", MessageID: id, Node: "in", Reason: "test",
			Raw: []byte(`{"n":` + strconv.Itoa(i+1) + `}`), Meta: map[string]any{}, CreatedAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	seeded, err := st.DeadLetters("replay-limit")
	if err != nil || len(seeded) != 3 {
		t.Fatalf("seed: %+v (%v)", seeded, err)
	}
	all := []int64{seeded[0].ID, seeded[1].ID, seeded[2].ID}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	out := runCLISubprocess(t, bin, root, "replay",
		"--config", pipeline, "--dlq",
		"--ids", joinIDs(all),
		"--limit", "1", "--delete", "--json", "--data-dir", dataDir)
	if !strings.Contains(out, `"replayed":1`) {
		t.Fatalf("replay output: %s", out)
	}

	owner2 := store.NewOwner(dataDir)
	defer func() { _ = owner2.Close() }()
	st2, err := owner2.Open("replay-limit")
	if err != nil {
		t.Fatal(err)
	}
	rest, err := st2.DeadLetters("replay-limit")
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 {
		t.Fatalf("after --limit 1 --delete: %d dead letters left, want 2 (only the replayed row may be deleted): %+v", len(rest), rest)
	}
	for _, dl := range rest {
		if dl.ID == all[0] {
			t.Fatalf("replayed row %d was not deleted: %+v", dl.ID, rest)
		}
	}
}

func joinIDs(ids []int64) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	return strings.Join(parts, ",")
}

// runCLIAllowFail runs the binary and returns stdout+stderr with the exit
// code, for commands expected to exit non-zero.
func runCLIAllowFail(t *testing.T, bin, dir string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return string(out), ee.ExitCode()
	}
	t.Fatalf("eventboat %s: %v\n%s", strings.Join(args, " "), err, out)
	return "", -1
}

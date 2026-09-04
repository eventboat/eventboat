package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcp "github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

// The §7.4 M2 core acceptance: an agent drives the ENTIRE product lifecycle
// through MCP tools only — catalog → verify → test → explain → deploy →
// status → trigger (parameterized backfill) → fix an error → redeploy —
// against the real binary speaking MCP over stdio.
func TestAgentLoopOverMCP(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess MCP loop")
	}
	bin := buildBinary(t)
	root := repoRoot(t)
	dir := t.TempDir()

	// The agent's working material: a job pipeline over the sqlite sql source
	// (a fresh DB seeded here) plus a contract suite.
	seedDB := filepath.Join(dir, "orders.db")
	seedOrdersDB(t, seedDB)
	outFile := filepath.Join(dir, "sync.jsonl")

	pipelineYAML := func(scriptLine string) string {
		return fmt.Sprintf(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: agent-loop-sync }
run: { mode: job }
parameters:
  from: { type: string, default: cursor }
  to:   { type: string, default: now }
sources:
  pull:
    decoder: json
    sql:
      driver: sqlite
      dsn: "file:%s"
      query: |
        SELECT id, order_no, amount, updated_at FROM orders
        WHERE updated_at >= :from AND updated_at < :to
      args: { from: "${parameters.from}", to: "${parameters.to}" }
      cursor: { column: updated_at }
      pagination: { key: [updated_at, id], page_size: 2 }
transforms:
  enrich:
    from: [pull]
    script: |
      payload.stamp = "synced"
%s
sinks:
  out:
    from: [enrich]
    encoder: json
    file: { path: %s }
`, filepath.ToSlash(seedDB), scriptLine, filepath.ToSlash(outFile))
	}

	suiteYAML := `
suite: agent-loop-suite
pipeline: pipeline.yaml
cases:
  - name: row-stamped
    inject:
      at: pull
      raw: '{"id":1,"order_no":"o1","amount":9.5,"updated_at":"2026-09-01T00:00:00Z"}'
    expect:
      capture:
        at: out
        messages:
          - payload.stamp: synced
`

	session := startMCPSession(t, bin, root, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	call := func(tool string, args map[string]any) (string, error) {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{
			Name: tool, Arguments: args,
		})
		if err != nil {
			return "", err
		}
		var text string
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				text += tc.Text
			}
		}
		if res.IsError {
			return text, fmt.Errorf("tool %s failed: %s", tool, text)
		}
		return text, nil
	}
	mustCall := func(t *testing.T, tool string, args map[string]any) string {
		t.Helper()
		out, err := call(tool, args)
		if err != nil {
			t.Fatalf("%s: %v\n%s", tool, err, out)
		}
		return out
	}

	// 1. catalog — the agent learns the plugin space.
	cat := mustCall(t, "catalog", map[string]any{})
	for _, want := range []string{`"sources"`, `"sql"`, `"kafka"`, `"file"`} {
		if !strings.Contains(cat, want) {
			t.Fatalf("catalog missing %s:\n%.500s", want, cat)
		}
	}

	// 2. verify — a broken config returns STRUCTURED diagnostics (the tool
	// call itself succeeds; ok=false carries the verdict — deploy is the
	// enforcing gate).
	broken := strings.Replace(pipelineYAML(""), "run: { mode: job }", "run:\n  mode: job\n  overlap: cancel", 1)
	out := mustCall(t, "verify", map[string]any{"config": broken})
	if !strings.Contains(out, `"ok": false`) || !strings.Contains(out, "overlap") {
		t.Fatalf("verify did not report the overlap error:\n%s", out)
	}

	// 3. verify — the good config passes.
	good := pipelineYAML("")
	ok := mustCall(t, "verify", map[string]any{"config": good})
	if !strings.Contains(ok, `"ok": true`) {
		t.Fatalf("verify rejected the good config:\n%s", ok)
	}

	// 4. test — the contract suite passes against the real engine.
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "suite.yaml"), []byte(suiteYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	suite, _ := os.ReadFile(filepath.Join(dir, "suite.yaml"))
	rep := mustCall(t, "test", map[string]any{"suite": string(suite), "pipeline": good})
	if !strings.Contains(rep, `"pass"`) || strings.Contains(rep, `"fail"`) {
		t.Fatalf("contract suite did not pass:\n%s", rep)
	}

	// 5. explain — the walkthrough names the pipeline's shape.
	ex := mustCall(t, "explain", map[string]any{"config": good})
	for _, want := range []string{"agent-loop-sync", "transform.script", "sql"} {
		if !strings.Contains(ex, want) {
			t.Fatalf("explain missing %s:\n%.800s", want, ex)
		}
	}

	// 6. deploy (verify-first) — the pipeline starts running.
	dep := mustCall(t, "deploy", map[string]any{"config": good})
	if !strings.Contains(dep, `"mode": "job"`) {
		t.Fatalf("deploy summary: %s", dep)
	}

	// 7. status — the agent sees the deployed pipeline.
	st := mustCall(t, "status", map[string]any{})
	if !strings.Contains(st, "agent-loop-sync") || !strings.Contains(st, `"mode": "job"`) {
		t.Fatalf("status missing the deployed pipeline:\n%.800s", st)
	}

	// 8. trigger with parameters — the day-1 backfill runs to success.
	tr := mustCall(t, "trigger", map[string]any{
		"pipeline":   "agent-loop-sync",
		"parameters": map[string]any{"from": "2026-09-01T00:00:00Z", "to": "2026-09-02T00:00:00Z"},
		"wait":       true,
	})
	if !strings.Contains(tr, `"status": "success"`) || !strings.Contains(tr, `"rows_read": 3`) {
		t.Fatalf("backfill trigger: %s", tr)
	}

	// 9. jobs — the run history is queryable.
	jobsOut := mustCall(t, "jobs", map[string]any{"pipeline": "agent-loop-sync"})
	if !strings.Contains(jobsOut, `"trigger": "manual"`) || !strings.Contains(jobsOut, "2026-09-01T00:00:00Z") {
		t.Fatalf("jobs history lost the backfill parameters:\n%.800s", jobsOut)
	}

	// 10. fix-an-error loop: a broken script is rejected by deploy (verify),
	// the fixed version deploys.
	brokenScript := pipelineYAML("      payload.x = nosuch_binding")
	if out, err := call("deploy", map[string]any{"config": brokenScript}); err == nil {
		t.Fatalf("deploy accepted a broken script:\n%s", out)
	} else if !strings.Contains(out, "undefined") {
		t.Fatalf("deploy diagnostics missing the undefined name:\n%s", out)
	}
	_ = mustCall(t, "deploy", map[string]any{"config": good})

	// 11. the day-2 incremental run proves the redeployed pipeline works and
	// resumes from the committed watermark; tail shows the deliveries.
	tr2 := mustCall(t, "trigger", map[string]any{"pipeline": "agent-loop-sync", "wait": true})
	if !strings.Contains(tr2, `"status": "success"`) || !strings.Contains(tr2, `"rows_read": 3`) {
		t.Fatalf("incremental trigger after redeploy: %s", tr2)
	}
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "\n"); got != 6 {
		t.Fatalf("sink file has %d lines after backfill+incremental, want 6:\n%s", got, data)
	}
	tail := mustCall(t, "tail", map[string]any{"node": "out", "n": 5})
	if !strings.Contains(tail, "synced") {
		t.Fatalf("tail shows no deliveries:\n%.500s", tail)
	}
}

// startMCPSession spawns `eventboat mcp --stdio` and connects an SDK client.
func startMCPSession(t *testing.T, bin, root, dataDir string) *mcp.ClientSession {
	t.Helper()
	cmd := exec.Command(bin, "mcp", "--stdio", "--data-dir", dataDir)
	cmd.Dir = root
	transport := &mcp.CommandTransport{Command: cmd}
	client := mcp.NewClient(&mcp.Implementation{Name: "agent-loop-test", Version: "1.0"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("connect MCP: %v", err)
	}
	t.Cleanup(func() {
		_ = session.Close()
	})
	return session
}

// seedOrdersDB creates the sqlite source database for the loop.
func seedOrdersDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mustExec(t, db, `CREATE TABLE orders (id INTEGER PRIMARY KEY, order_no TEXT, amount REAL, updated_at TEXT)`)
	for i := 0; i < 6; i++ {
		day := 1 + i/3
		ts := fmt.Sprintf("2026-09-%02dT%02d:00:00Z", day, i%3*8)
		mustExec(t, db, `INSERT INTO orders (id, order_no, amount, updated_at) VALUES (?, ?, ?, ?)`,
			i+1, fmt.Sprintf("ord-%d", i+1), float64(i+1)*2.5, ts)
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("seed %q: %v", query, err)
	}
}

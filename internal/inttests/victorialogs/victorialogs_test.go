// Package victorialogs_int runs the collection pipeline against a REAL
// VictoriaLogs server: JSONL lines go through the real file source, the real
// engine and the victorialogs sink, and the test queries the ingested rows
// back through LogsQL (/select/logsql/query). The package is env-gated on
// EVENTBOAT_VICTORIALOGS_URL (log-collection design 2026-09-24 §4 P0):
// local `go test ./...` skips it (no Docker needed); CI runs the
// victorialogs-integration job.
package victorialogs_int

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
)

// The stamp constants the transform writes onto every line; the probe is the
// per-run marker that isolates this test's rows inside a shared VL instance.
const (
	host = "inttest-host"
	app  = "inttest-app"
)

// requireVL returns the base URL of the VictoriaLogs instance under test,
// skipping the test when the gate is unset.
func requireVL(t *testing.T) string {
	t.Helper()
	base := strings.TrimSpace(os.Getenv("EVENTBOAT_VICTORIALOGS_URL"))
	if base == "" {
		t.Skip("set EVENTBOAT_VICTORIALOGS_URL (e.g. http://127.0.0.1:9428) to run the VictoriaLogs integration test")
	}
	return strings.TrimRight(base, "/")
}

func uniqueProbe() string { return fmt.Sprintf("inttest-%d-%d", time.Now().UnixNano(), os.Getpid()) }

// writeLines writes the raw JSONL lines to a temp file and returns its path.
func writeLines(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// collectorYAML is the real collection shape: a finite file source (so the
// batch run completes by itself), the stamp transform and the victorialogs
// sink. poll_every_ms is short so the whole file is read within a few ticks.
func collectorYAML(base, path, probe string, gzip bool) string {
	gzipLine := ""
	if gzip {
		gzipLine = "      gzip: true\n"
	}
	return fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: {name: vl-inttest}
run: {mode: batch}
constants:
  host: %[3]q
  app: %[4]q
  probe: %[5]q
sources:
  logs:
    decoder: json
    file:
      path: %[2]q
      on_eof: stop
      poll_every_ms: 10
transforms:
  stamp:
    depends_on: [logs]
    script: |
      payload.host = constants.host
      payload.app = constants.app
      payload.probe = constants.probe
sinks:
  victorialogs:
    depends_on: [stamp]
    encoder: json
    batch: {size: 2, timeout_ms: 200}
    victorialogs:
      url: %[1]q
      stream_fields: host,app
      time_field: ts
%[6]s`, base, path, host, app, probe, gzipLine)
}

// runBatch builds the real engine for the config, runs it to quiescence
// through the batch completion primitive (Engine.Wait: every source
// exhausted, every branch committed, checkpoint flushed) and returns the
// outcome plus the store for dead-letter assertions.
func runBatch(t *testing.T, yamlText string) (engine.Outcome, *store.SQLite, string) {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	lr := config.LoadBytes("victorialogs-int.yaml", []byte(yamlText))
	if lr.Diagnostics.HasErrors() {
		t.Fatalf("config: %+v", lr.Diagnostics)
	}
	pip, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		t.Fatalf("verify: %+v", diags)
	}
	for _, d := range diags {
		if d.Severity == "error" {
			t.Fatalf("verify: %+v", diags)
		}
	}
	st, err := store.OpenSQLite(filepath.Join(t.TempDir(), "spool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng, err := engine.New(pip, st, reg, engine.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// done is buffered: Engine.Wait owns the run-result channel (it drains it
	// when Run returns before quiescence), so the goroutine's send never
	// blocks and Wait's outcome is the single terminal report.
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	return eng.Wait(ctx, done, engine.WaitOptions{}), st, pip.Config.Name
}

// queryVL runs one LogsQL query and parses the NDJSON response: one JSON
// object per line, no wrapper, empty body = no rows.
func queryVL(base, query string) ([]map[string]any, error) {
	u, err := url.Parse(base + "/select/logsql/query")
	if err != nil {
		return nil, fmt.Errorf("bad base URL: %w", err)
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("limit", "100")
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query %q: status %d: %s", query, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("query %q: bad NDJSON line %q: %w", query, line, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// waitRows polls LogsQL until the probe filter returns want rows: VL makes
// ingested data queryable asynchronously, so the test retries within a bound
// instead of sleeping a fixed amount.
func waitRows(t *testing.T, base, probe string, want int) []map[string]any {
	t.Helper()
	query := fmt.Sprintf("probe:%q", probe)
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	lastCount := 0
	for time.Now().Before(deadline) {
		rows, err := queryVL(base, query)
		if err != nil {
			lastErr = err
		} else {
			lastErr = nil
			lastCount = len(rows)
			if lastCount >= want {
				return rows
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("query %q never returned %d rows within 20s (got %d, last error: %v)", query, want, lastCount, lastErr)
	return nil
}

// want describes one expected ingested row.
type want struct {
	line  int
	ts    string // the ts field the file carried; must come back as _time
	level string
	msg   string
}

// checkRows indexes the probe-filtered rows by their `line` field and asserts
// the stamped fields, the stream labels and the event time of every row.
func checkRows(t *testing.T, rows []map[string]any, probe string, wants []want) {
	t.Helper()
	byLine := map[string]map[string]any{}
	for _, r := range rows {
		byLine[fmt.Sprint(r["line"])] = r
	}
	for _, w := range wants {
		r, ok := byLine[strconv.Itoa(w.line)]
		if !ok {
			t.Errorf("line %d missing from the query result: %v", w.line, rows)
			continue
		}
		if got := r["probe"]; got != probe {
			t.Errorf("line %d: probe = %v, want %v", w.line, got, probe)
		}
		if got := r["host"]; got != host {
			t.Errorf("line %d: host = %v, want %v", w.line, got, host)
		}
		if got := r["app"]; got != app {
			t.Errorf("line %d: app = %v, want %v", w.line, got, app)
		}
		if got := r["level"]; got != w.level {
			t.Errorf("line %d: level = %v, want %v", w.line, got, w.level)
		}
		if got := r["msg"]; got != w.msg {
			t.Errorf("line %d: msg = %v, want %v", w.line, got, w.msg)
		}
		// _stream_fields=host,app must have produced the stream labels.
		stream, _ := r["_stream"].(string)
		if !strings.Contains(stream, `host="`+host+`"`) || !strings.Contains(stream, `app="`+app+`"`) {
			t.Errorf("line %d: _stream = %q, want the host/app stream fields", w.line, stream)
		}
		// _time_field=ts: _time is the event's ts, not the insert time. VL
		// normalizes the rendering (fractional digits are trimmed), so both
		// sides parse and the instants are compared.
		gotTime, err := time.Parse(time.RFC3339Nano, fmt.Sprint(r["_time"]))
		if err != nil {
			t.Errorf("line %d: _time %v is not RFC3339: %v", w.line, r["_time"], err)
			continue
		}
		wantTime, err := time.Parse(time.RFC3339Nano, w.ts)
		if err != nil {
			t.Fatalf("bad ts in the test fixture: %v", err)
		}
		if !gotTime.Equal(wantTime) {
			t.Errorf("line %d: _time = %v, want %v (the ts field %s)", w.line, gotTime, wantTime, w.ts)
		}
	}
}

// TestVictoriaLogsCollectorIngestsAndQueriesBack is the P0 acceptance path:
// a real file is read by the real file source, stamped by the real script
// transform, shipped by the real sink in batches, and queried back from VL.
// The malformed line must dead-letter at decode and never reach VL.
func TestVictoriaLogsCollectorIngestsAndQueriesBack(t *testing.T) {
	base := requireVL(t)
	probe := uniqueProbe()
	path := writeLines(t,
		`{"ts":"2026-09-24T10:00:01Z","level":"info","msg":"first good line","line":1}`,
		`{"ts":"2026-09-24T10:00:02Z","level":"warn","msg":"second good line","line":2}`,
		`{not json`,
		`{"ts":"2026-09-24T10:00:03Z","level":"error","msg":"third good line","line":3}`,
	)

	outcome, st, pipeline := runBatch(t, collectorYAML(base, path, probe, false))
	if outcome.Status != engine.RunPartial || outcome.DeadLettered != 1 {
		t.Fatalf("outcome = %+v, want partial with exactly 1 dead letter", outcome)
	}
	if outcome.Committed != 4 {
		t.Errorf("committed = %d, want 4 (3 rows delivered + 1 dead letter)", outcome.Committed)
	}
	dls, err := st.DeadLetters(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1: %+v", len(dls), dls)
	}
	if !strings.Contains(dls[0].Reason, "decode") || !strings.Contains(string(dls[0].Raw), "not json") {
		t.Errorf("dead letter = reason %q raw %q, want the malformed line with a decode reason", dls[0].Reason, dls[0].Raw)
	}

	rows := waitRows(t, base, probe, 3)
	if len(rows) != 3 {
		t.Fatalf("VL holds %d rows for the probe, want exactly 3 (the malformed line must not ship)", len(rows))
	}
	checkRows(t, rows, probe, []want{
		{line: 1, ts: "2026-09-24T10:00:01Z", level: "info", msg: "first good line"},
		{line: 2, ts: "2026-09-24T10:00:02Z", level: "warn", msg: "second good line"},
		{line: 3, ts: "2026-09-24T10:00:03Z", level: "error", msg: "third good line"},
	})
}

// TestVictoriaLogsCollectorGzip covers the sink's compressed-body path
// against the real server (VL rejects a bad gzip body, so a queried-back row
// proves the encoding).
func TestVictoriaLogsCollectorGzip(t *testing.T) {
	base := requireVL(t)
	probe := uniqueProbe()
	path := writeLines(t,
		`{"ts":"2026-09-24T10:00:04Z","level":"info","msg":"gzipped batch","line":1}`,
	)

	outcome, _, _ := runBatch(t, collectorYAML(base, path, probe, true))
	if outcome.Status != engine.RunCompleted {
		t.Fatalf("outcome = %+v, want completed", outcome)
	}
	rows := waitRows(t, base, probe, 1)
	if len(rows) != 1 {
		t.Fatalf("VL holds %d rows for the probe, want exactly 1", len(rows))
	}
	checkRows(t, rows, probe, []want{
		{line: 1, ts: "2026-09-24T10:00:04Z", level: "info", msg: "gzipped batch"},
	})
}

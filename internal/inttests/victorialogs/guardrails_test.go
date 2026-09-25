// Guardrail coverage against a REAL VictoriaLogs server: the encoded-line
// checks the sink added after the adversarial review (a 200 response used to
// hide server-side skipped lines) and the raw+wrap_field shape that makes
// text payloads ingestible. Same gate as the rest of the package
// (EVENTBOAT_VICTORIALOGS_URL; local runs skip without Docker/CI).
package victorialogs_int

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/engine"
)

// waitNoRows polls LogsQL and fails if any row matches. It is used where the
// correct end state is absence: a line VL skipped (or the sink refused) never
// becomes queryable, and the pipeline is already quiesced when it is called,
// so a match would mean the silent-drop defect is back.
func waitNoRows(t *testing.T, base, query string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := queryVL(base, query)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		if len(rows) > 0 {
			t.Fatalf("query %q returned %d rows, want 0: %v", query, len(rows), rows)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// guardrailYAML is the finite JSON pipeline the violation tests share: the
// probe rides in the lines themselves, so the query isolates the run without
// a transform in between (the sink guardrail must be what fires). The batch
// block is a parameter: size 1 isolates each line's verdict (a guardrail
// refusal fails the whole batch by design), while size 2 forces the mixed
// batch VL used to answer 200 for while skipping the bad line.
func guardrailYAML(base, path, sourceExtra, batch string) string {
	return fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: {name: vl-guardrail}
run: {mode: batch}
sources:
  logs:
    decoder: json
    file:
      path: %[2]q
      on_eof: stop
      poll_every_ms: 10
%[3]ssinks:
  victorialogs:
    depends_on: [logs]
    encoder: json
    batch: %[4]s
    victorialogs:
      url: %[1]q
`, base, path, sourceExtra, batch)
}

// The D1 shape end to end (POSITIVE case): a raw JSON line just under
// 262144 bytes, dense in '<', must be ingested — the old HTML-escaping
// encoder inflated it past VL's -insert.maxLineSizeBytes (~6x), VL skipped it
// while answering 200 and the engine committed it as delivered. Querying the
// full value back proves the row landed unaltered.
func TestVictoriaLogsHTMLLikeLineIngests(t *testing.T) {
	base := requireVL(t)
	probe := uniqueProbe()
	msg := strings.Repeat("<", 99900)
	path := writeLines(t, fmt.Sprintf(`{"msg":%q,"probe":%q}`, msg, probe))

	outcome, _, _ := runBatch(t, guardrailYAML(base, path, "", "{size: 1, timeout_ms: 200}"))
	if outcome.Status != engine.RunCompleted || outcome.DeadLettered != 0 {
		t.Fatalf("outcome = %+v, want completed with no dead letters (the '<'-dense line is under the encoded bound once HTML escaping is off)", outcome)
	}
	rows := waitRows(t, base, probe, 1)
	if len(rows) != 1 {
		t.Fatalf("VL holds %d rows for the probe, want exactly 1", len(rows))
	}
	if got := fmt.Sprint(rows[0]["msg"]); got != msg {
		t.Fatalf("row msg = %d bytes, want the %d-byte original (encoded line stayed under VL's limit)", len(got), len(msg))
	}
}

// An encoded line over 262144 bytes must dead-letter (visible, replayable)
// instead of being POSTed and 200-skipped by VL. Both lines carry the probe,
// so the query proves the over-long one never became a row.
func TestVictoriaLogsSinkGuardrailOverlongLine(t *testing.T) {
	base := requireVL(t)
	probe := uniqueProbe()
	long := fmt.Sprintf(`{"msg":"%s","probe":%q}`, strings.Repeat("a", 300000), probe)
	path := writeLines(t,
		fmt.Sprintf(`{"msg":"good line","probe":%q}`, probe),
		long,
	)

	outcome, st, pipeline := runBatch(t, guardrailYAML(base, path, "      max_line_bytes: 1048576\n", "{size: 1, timeout_ms: 200}"))
	if outcome.Status != engine.RunPartial || outcome.DeadLettered != 1 {
		t.Fatalf("outcome = %+v, want partial with exactly 1 dead letter (the over-long line must not be committed as delivered)", outcome)
	}
	dls, err := st.DeadLetters(pipeline)
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters = %d (%v), want 1", len(dls), err)
	}
	if !strings.Contains(dls[0].Reason, "over max_line_bytes 262144") {
		t.Errorf("dead letter reason = %q, want the max_line_bytes guardrail reason", dls[0].Reason)
	}
	if !strings.Contains(dls[0].Reason, "message ") || dls[0].MessageID == "" {
		t.Errorf("dead letter lacks the message-id attribution: reason %q id %q", dls[0].Reason, dls[0].MessageID)
	}
	rows := waitRows(t, base, probe, 1)
	if len(rows) != 1 {
		t.Fatalf("VL holds %d rows for the probe, want exactly 1 (the good line; the over-long line must not be silently ingested)", len(rows))
	}
	if got := rows[0]["msg"]; got != "good line" {
		t.Errorf("row msg = %v, want the good line", got)
	}
}

// A non-object JSON line sharing a batch with a valid object must dead-letter
// through the sink guardrail. Before the guardrail, VL answered 200 while
// skipping the array and the engine committed the batch as delivered; now the
// refusal fails the whole batch (by design), so both members dead-letter and
// NEITHER becomes a row.
func TestVictoriaLogsSinkGuardrailNonObjectLine(t *testing.T) {
	base := requireVL(t)
	probe := uniqueProbe()
	path := writeLines(t,
		fmt.Sprintf(`{"msg":"object line","probe":%q}`, probe),
		`[1,2,3]`,
	)

	outcome, st, pipeline := runBatch(t, guardrailYAML(base, path, "", "{size: 2, timeout_ms: 200}"))
	if outcome.Status != engine.RunPartial || outcome.DeadLettered != 2 {
		t.Fatalf("outcome = %+v, want partial with both batch members dead-lettered (a bad line fails its whole batch by design)", outcome)
	}
	dls, err := st.DeadLetters(pipeline)
	if err != nil || len(dls) != 2 {
		t.Fatalf("dead letters = %d (%v), want 2", len(dls), err)
	}
	for _, dl := range dls {
		if !strings.Contains(dl.Reason, "not a JSON object") {
			t.Errorf("dead letter reason = %q, want the non-object guardrail reason", dl.Reason)
		}
	}
	waitNoRows(t, base, fmt.Sprintf("probe:%q", probe))
}

// A raw decoder without a wrap transform encodes to a JSON string, which the
// jsonline API refuses; the sink guardrail must dead-letter it rather than
// let VL 200-skip it.
func TestVictoriaLogsSinkGuardrailRawWithoutWrap(t *testing.T) {
	base := requireVL(t)
	marker := uniqueProbe()
	path := writeLines(t, "plain text from a text log "+marker)

	yamlText := fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: {name: vl-guardrail-raw}
run: {mode: batch}
sources:
  logs:
    decoder: raw
    file:
      path: %[2]q
      on_eof: stop
      poll_every_ms: 10
sinks:
  victorialogs:
    depends_on: [logs]
    encoder: json
    batch: {size: 2, timeout_ms: 200}
    victorialogs:
      url: %[1]q
`, base, path)

	outcome, st, pipeline := runBatch(t, yamlText)
	if outcome.Status != engine.RunPartial || outcome.DeadLettered != 1 {
		t.Fatalf("outcome = %+v, want partial with exactly 1 dead letter", outcome)
	}
	dls, err := st.DeadLetters(pipeline)
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters = %d (%v), want 1", len(dls), err)
	}
	if !strings.Contains(dls[0].Reason, "not a JSON object") {
		t.Errorf("dead letter reason = %q, want the non-object guardrail reason", dls[0].Reason)
	}
	waitNoRows(t, base, fmt.Sprintf("%q", marker))
}

// F3 end to end: a raw multi-line payload (a Java stack trace) aggregated by
// the file source's multiline block, wrapped by the fields transform with
// wrap_field and metadata, is queried back from VL as one row per group.
func TestVictoriaLogsRawWrapFieldQueriesBack(t *testing.T) {
	base := requireVL(t)
	probe := uniqueProbe()
	path := writeLines(t,
		"java.lang.RuntimeException: boom",
		"\tat com.example.Main.a(Main.java:1)",
		"\tat com.example.Main.b(Main.java:2)",
		"plain single-line entry",
	)

	yamlText := fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: {name: vl-raw-wrap}
run: {mode: batch}
sources:
  logs:
    decoder: raw
    file:
      path: %[2]q
      on_eof: stop
      poll_every_ms: 10
      multiline: {pattern: "^\\s", match: after, timeout_ms: 100}
transforms:
  wrap:
    depends_on: [logs]
    fields:
      wrap_field: msg
      fields: {app: nginx, probe: %[3]q}
      from_meta: [file_path]
sinks:
  victorialogs:
    depends_on: [wrap]
    encoder: json
    batch: {size: 2, timeout_ms: 200}
    victorialogs:
      url: %[1]q
`, base, path, probe)

	outcome, _, _ := runBatch(t, yamlText)
	if outcome.Status != engine.RunCompleted {
		t.Fatalf("outcome = %+v, want completed (the raw payload must ship wrapped)", outcome)
	}
	rows := waitRows(t, base, probe, 2)
	if len(rows) != 2 {
		t.Fatalf("VL holds %d rows for the probe, want exactly 2 (one per multiline group)", len(rows))
	}
	byMsg := map[string]map[string]any{}
	for _, r := range rows {
		byMsg[fmt.Sprint(r["msg"])] = r
	}
	stack := byMsg["java.lang.RuntimeException: boom\n\tat com.example.Main.a(Main.java:1)\n\tat com.example.Main.b(Main.java:2)"]
	if stack == nil {
		t.Fatalf("the multiline stack trace was not queried back as one wrapped row: %v", rows)
	}
	if stack["app"] != "nginx" {
		t.Errorf("stack row app = %v, want nginx (static fields applied after wrap)", stack["app"])
	}
	if _, ok := stack["file_path"]; !ok {
		t.Errorf("stack row lacks file_path from meta: %v", stack)
	}
	if _, ok := byMsg["plain single-line entry"]; !ok {
		t.Errorf("the single-line group is missing: %v", rows)
	}
}

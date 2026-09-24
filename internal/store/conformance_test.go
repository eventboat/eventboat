package store

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// TestConformanceSQLiteAndMemory (candidate 04 acceptance 4): the same
// operation sequence against the SQLite and in-memory implementations must
// produce identical observable results — spool pagination, checkpoint,
// source state, dead letters and job runs, with multi-pipeline isolation
// throughout.
func TestConformanceSQLiteAndMemory(t *testing.T) {
	sql, err := OpenSQLite(filepath.Join(t.TempDir(), "conformance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sql.Close() }()
	mem := NewMemory()
	defer func() { _ = mem.Close() }()

	want := conformanceLog(t, sql)
	got := conformanceLog(t, mem)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("in-memory results differ from SQLite:\nSQLite:    %v\nin-memory: %v", want, got)
	}
	if len(got) == 0 {
		t.Fatal("conformance log is empty")
	}
}

// conformanceLog runs one deterministic sequence and returns a string log of
// every observable result. Times are explicit (no time.Now) and rows are
// written with non-nil Meta/Parameters/Raw so both backends round-trip the
// same values.
func conformanceLog(t *testing.T, st Store) []string {
	t.Helper()
	var log []string
	note := func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) }

	t0 := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(500 * time.Millisecond) // sub-second precision: the timestamp-layout probe
	msg := func(id string) registry.Message {
		return registry.Message{ID: id, Codec: "json", Raw: []byte(`{"id":"` + id + `"}`),
			Meta: map[string]any{"k": id}, SrcName: "in", SrcSeq: 1, Cursor: "c-" + id}
	}
	replay := func(p string) string {
		var out []string
		err := st.ReplayFrom(p, 0, func(seq int64, m registry.Message, _ time.Time) error {
			out = append(out, fmt.Sprintf("%d/%s", seq, m.ID))
			return nil
		})
		if err != nil {
			t.Fatalf("ReplayFrom(%s): %v", p, err)
		}
		return fmt.Sprintf("%s=%v", p, out)
	}

	// --- spool, checkpoint, source state: two pipelines interleaved ---
	s1, err := st.AppendSpool("a", msg("a1"), t0)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := st.AppendSpool("b", msg("b1"), t0)
	if err != nil {
		t.Fatal(err)
	}
	s3, err := st.AppendSpool("a", msg("a2"), t0)
	if err != nil {
		t.Fatal(err)
	}
	note("spool seqs %d %d %d", s1, s2, s3)
	note("replay %s %s", replay("a"), replay("b"))

	page := func(after int64) string {
		last, more, err := st.ReplayPage("a", after, 1, func(int64, registry.Message, time.Time) error { return nil })
		if err != nil {
			t.Fatalf("ReplayPage: %v", err)
		}
		return fmt.Sprintf("last=%d more=%v", last, more)
	}
	note("page a from 0: %s", page(0))
	note("page a from 1: %s", page(1))
	note("page a from 3: %s", page(3))

	if err := st.SetCheckpoint("a", s1); err != nil {
		t.Fatal(err)
	}
	cpA, err := st.Checkpoint("a")
	if err != nil {
		t.Fatal(err)
	}
	cpB, err := st.Checkpoint("b")
	if err != nil {
		t.Fatal(err)
	}
	note("checkpoints a=%d b=%d", cpA, cpB)
	if err := st.SetSourceState("a", "in", []byte(`{"w":"c-a1"}`), 1); err != nil {
		t.Fatal(err)
	}
	stateA, seqA, err := st.SourceState("a", "in")
	if err != nil {
		t.Fatal(err)
	}
	stateB, seqB, err := st.SourceState("b", "in")
	if err != nil {
		t.Fatal(err)
	}
	note("source states a=%s/%d b=%s/%d", stateA, seqA, stateB, seqB)

	trimmed, err := st.DeleteSpoolThrough("b", int64(1)<<62)
	if err != nil {
		t.Fatal(err)
	}
	note("trim b removed=%d", trimmed)
	note("after trim %s %s", replay("a"), replay("b"))

	// --- dead letters: two pipelines, run attribution, since/delete ---
	dl := func(p, id, runID string, at time.Time) DeadLetter {
		return DeadLetter{Pipeline: p, MessageID: id, RunID: runID, Node: "out", Edge: "t -> out",
			Reason: "delivery: x", Class: DLClassDelivery, Backtrace: "bt", Raw: []byte(`{"id":"` + id + `"}`), Codec: "json",
			Meta: map[string]any{"m": id}, Cursor: "c", SrcName: "in", SrcSeq: 1, CreatedAt: at}
	}
	if err := st.WriteDeadLetter(dl("a", "d1", "r1", t0)); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteDeadLetter(dl("a", "d2", "r2", t1)); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteDeadLetter(dl("b", "d3", "r1", t0)); err != nil {
		t.Fatal(err)
	}
	dlList := func(p string) string {
		dls, err := st.DeadLetters(p)
		if err != nil {
			t.Fatalf("DeadLetters(%s): %v", p, err)
		}
		var out []string
		for _, d := range dls {
			out = append(out, fmt.Sprintf("%d/%s/%s", d.ID, d.MessageID, d.Class))
		}
		return fmt.Sprintf("%s=%v", p, out)
	}
	note("dlq %s %s", dlList("a"), dlList("b"))
	forRun, err := st.DeadLettersForRun("a", "r1")
	if err != nil {
		t.Fatal(err)
	}
	note("dlq run r1 a=%d", len(forRun))
	since, err := st.DeadLettersSince("a", t1)
	if err != nil {
		t.Fatal(err)
	}
	note("dlq since t1 a=%d first=%s", len(since), since[0].MessageID)
	firstID := forRun[0].ID
	deleted, err := st.DeleteDeadLetters("a", []int64{firstID})
	if err != nil {
		t.Fatal(err)
	}
	note("delete dlq a id=%d removed=%d", firstID, deleted)
	swept, err := st.DeleteDeadLettersBefore("a", t1)
	if err != nil {
		t.Fatal(err)
	}
	note("dlq sweep before t1 removed=%d", swept)
	note("dlq after %s %s", dlList("a"), dlList("b"))

	// --- job runs: two pipelines, ordering, duplicate, retention ---
	jr := func(p, id, status string, started, ended time.Time, sched string) JobRun {
		return JobRun{RunID: id, Pipeline: p, Status: status, TriggerType: "manual",
			Parameters: map[string]any{"p": id}, ScheduledFor: sched, StartedAt: started, EndedAt: ended}
	}
	if err := st.CreateJobRun(jr("a", "r1", JobSuccess, t0, t0.Add(time.Minute), "t1")); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateJobRun(jr("a", "r2", JobPending, t1, time.Time{}, "t2")); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateJobRun(jr("b", "r3", JobSuccess, t0, t0.Add(time.Minute), "t1")); err != nil {
		t.Fatal(err)
	}
	dup := st.CreateJobRun(jr("a", "r1", JobFailed, t0, time.Time{}, ""))
	note("duplicate run id rejected=%v", dup != nil)

	runs := func(p string) string {
		rs, err := st.JobRuns(p, 10)
		if err != nil {
			t.Fatalf("JobRuns(%s): %v", p, err)
		}
		var out []string
		for _, r := range rs {
			out = append(out, r.RunID+"/"+r.Status)
		}
		return fmt.Sprintf("%s=%v", p, out)
	}
	note("runs %s %s", runs("a"), runs("b"))
	runnable, err := st.RunnableJobRuns("a")
	if err != nil {
		t.Fatal(err)
	}
	var runnableIDs []string
	for _, r := range runnable {
		runnableIDs = append(runnableIDs, r.RunID)
	}
	note("runnable a=%v", runnableIDs)
	okA, err := st.HasSuccessfulRunFor("a", "t1")
	if err != nil {
		t.Fatal(err)
	}
	okB, err := st.HasSuccessfulRunFor("b", "t2")
	if err != nil {
		t.Fatal(err)
	}
	last, err := st.LastScheduledFor("a")
	if err != nil {
		t.Fatal(err)
	}
	note("successful a/t1=%v b/t2=%v lastScheduled a=%s", okA, okB, last)

	got, err := st.GetJobRun("a", "r2")
	if err != nil {
		t.Fatal(err)
	}
	got.Status = JobSuccess
	got.EndedAt = t0.Add(2 * time.Minute)
	got.RowsRead = 9
	if err := st.UpdateJobRun(*got); err != nil {
		t.Fatal(err)
	}
	got2, err := st.GetJobRun("a", "r2")
	if err != nil {
		t.Fatal(err)
	}
	note("get r2 status=%s rows=%d params=%v ended=%s", got2.Status, got2.RowsRead, got2.Parameters,
		got2.EndedAt.UTC().Format(time.RFC3339Nano))
	expired, err := st.DeleteJobRunsBefore("a", t0.Add(90*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	note("run retention removed=%d", expired)
	note("runs after %s", runs("a"))
	return log
}

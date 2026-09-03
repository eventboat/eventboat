package store

import (
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/eventboat/eventboat/internal/registry"
)

func exerciseStore(t *testing.T, st Store) {
	t.Helper()
	defer st.Close()

	msg := registry.Message{
		ID:      "m-1",
		Codec:   "json",
		Raw:     []byte(`{"a":1}`),
		Meta:    map[string]any{"region": "eu"},
		SrcName: "in",
		SrcSeq:  1,
		Cursor:  "c1",
	}
	ingest := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	seq1, err := st.AppendSpool("p", msg, ingest)
	if err != nil || seq1 != 1 {
		t.Fatalf("append: seq=%d err=%v", seq1, err)
	}
	seq2, err := st.AppendSpool("p", msg, ingest)
	if err != nil || seq2 != 2 {
		t.Fatalf("append2: seq=%d err=%v", seq2, err)
	}

	// Nothing replayed before checkpoint moves.
	if cp, _ := st.Checkpoint("p"); cp != 0 {
		t.Fatalf("checkpoint = %d, want 0", cp)
	}
	var replayed []int64
	if err := st.ReplayFrom("p", 0, func(seq int64, m registry.Message, ts time.Time) error {
		replayed = append(replayed, seq)
		if m.ID != "m-1" || m.Meta["region"] != "eu" || m.Codec != "json" {
			t.Errorf("roundtrip lost fields: %+v", m)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %v", replayed)
	}

	if err := st.SetCheckpoint("p", 1); err != nil {
		t.Fatal(err)
	}
	cp, err := st.Checkpoint("p")
	if err != nil {
		t.Fatal(err)
	}
	replayed = nil
	_ = st.ReplayFrom("p", cp, func(seq int64, m registry.Message, ts time.Time) error {
		replayed = append(replayed, seq)
		return nil
	})
	if len(replayed) != 1 || replayed[0] != 2 {
		t.Fatalf("after checkpoint, replayed %v, want [2]", replayed)
	}

	if err := st.SetSourceState("p", "in", []byte(`{"watermark":"c1"}`), 1); err != nil {
		t.Fatal(err)
	}
	state, srcSeq, err := st.SourceState("p", "in")
	if err != nil || srcSeq != 1 || string(state) != `{"watermark":"c1"}` {
		t.Fatalf("source state: %s %d %v", state, srcSeq, err)
	}

	dl := DeadLetter{
		Pipeline: "p", MessageID: "m-1", Node: "out", Edge: "t -> out",
		Reason: "delivery: sink write failed after retries", Backtrace: "t.star:3:1",
		Raw: msg.Raw, Codec: msg.Codec, Meta: msg.Meta, SrcName: "in", SrcSeq: 1,
	}
	if err := st.WriteDeadLetter(dl); err != nil {
		t.Fatal(err)
	}
	dls, err := st.DeadLetters("p")
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters: %v %v", dls, err)
	}
	if dls[0].Reason != dl.Reason || dls[0].Backtrace != "t.star:3:1" {
		t.Errorf("dead letter roundtrip: %+v", dls[0])
	}
}

func TestMemoryStore(t *testing.T) {
	exerciseStore(t, NewMemory("p"))
}

func TestSQLiteStore(t *testing.T) {
	path := t.TempDir() + "/test.db"
	st, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	exerciseStore(t, st)
}

// exerciseJobStore covers the M2 surface: job run history, run-attributed
// dead letters, since-filters and windowed replay pagination.
func exerciseJobStore(t *testing.T, st Store) {
	t.Helper()
	defer st.Close()

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	// Spool 5 rows for pagination.
	for i := 0; i < 5; i++ {
		if _, err := st.AppendSpool("p", registry.Message{ID: "m", Raw: []byte(`{}`)}, now); err != nil {
			t.Fatal(err)
		}
	}
	// ReplayPage windows without materializing everything.
	last, more, err := st.ReplayPage("p", 0, 2, func(seq int64, m registry.Message, _ time.Time) error {
		return nil
	})
	if err != nil || last != 2 || !more {
		t.Fatalf("page1: last=%d more=%v err=%v", last, more, err)
	}
	last, more, err = st.ReplayPage("p", last, 2, func(seq int64, m registry.Message, _ time.Time) error {
		return nil
	})
	if err != nil || last != 4 || !more {
		t.Fatalf("page2: last=%d more=%v err=%v", last, more, err)
	}
	last, more, err = st.ReplayPage("p", last, 2, func(seq int64, m registry.Message, _ time.Time) error {
		return nil
	})
	if err != nil || last != 5 || more {
		t.Fatalf("page3: last=%d more=%v err=%v", last, more, err)
	}

	// Job run lifecycle records.
	jr := JobRun{
		RunID: "run-1", Pipeline: "p", Status: JobPending, TriggerType: "schedule",
		Parameters:   map[string]any{"from": "cursor", "to": "2026-09-03T12:00:00Z"},
		ScheduledFor: "2026-09-03T01:00:00Z", StartedAt: now,
	}
	if err := st.CreateJobRun(jr); err != nil {
		t.Fatal(err)
	}
	jr.Status = JobRunning
	jr.RowsRead = 10
	if err := st.UpdateJobRun(jr); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetJobRun("p", "run-1")
	if err != nil || got.Status != JobRunning || got.RowsRead != 10 {
		t.Fatalf("get: %+v %v", got, err)
	}
	if got.Parameters["from"] != "cursor" {
		t.Errorf("parameters roundtrip: %+v", got.Parameters)
	}
	runnable, err := st.RunnableJobRuns("p")
	if err != nil || len(runnable) != 1 || !runnable[0].Runnable() {
		t.Fatalf("runnable: %+v %v", runnable, err)
	}
	if ok, _ := st.HasSuccessfulRunFor("p", "2026-09-03T01:00:00Z"); ok {
		t.Error("no successful run yet for the tick")
	}
	if last, _ := st.LastScheduledFor("p"); last != "2026-09-03T01:00:00Z" {
		t.Errorf("last scheduled_for = %q", last)
	}
	jr.Status = JobSuccess
	jr.EndedAt = now
	if err := st.UpdateJobRun(jr); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.HasSuccessfulRunFor("p", "2026-09-03T01:00:00Z"); !ok {
		t.Error("successful run for the tick not found")
	}
	if runnable, _ := st.RunnableJobRuns("p"); len(runnable) != 0 {
		t.Errorf("finished run still runnable: %+v", runnable)
	}
	runs, err := st.JobRuns("p", 10)
	if err != nil || len(runs) != 1 || runs[0].Status != JobSuccess {
		t.Fatalf("job runs: %+v %v", runs, err)
	}

	// Run-attributed dead letters.
	if err := st.WriteDeadLetter(DeadLetter{Pipeline: "p", MessageID: "m1", RunID: "run-1", Node: "out", Reason: "parse", Raw: []byte(`{}`), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteDeadLetter(DeadLetter{Pipeline: "p", MessageID: "m2", RunID: "run-2", Node: "out", Reason: "delivery", Raw: []byte(`{}`), CreatedAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	forRun, err := st.DeadLettersForRun("p", "run-1")
	if err != nil || len(forRun) != 1 || forRun[0].MessageID != "m1" {
		t.Fatalf("for run: %+v %v", forRun, err)
	}
	since, err := st.DeadLettersSince("p", now.Add(30*time.Second))
	if err != nil || len(since) != 1 || since[0].MessageID != "m2" {
		t.Fatalf("since: %+v %v", since, err)
	}
	if n, err := st.DeleteDeadLetters("p", []int64{forRun[0].ID}); err != nil || n != 1 {
		t.Fatalf("delete: n=%d err=%v", n, err)
	}
	if all, _ := st.DeadLetters("p"); len(all) != 1 {
		t.Errorf("dead letters after delete: %+v", all)
	}

	// Retention: finished runs before the cutoff vanish, runnable ones stay.
	old := JobRun{RunID: "run-0", Pipeline: "p", Status: JobFailed, TriggerType: "manual", StartedAt: now.Add(-100 * time.Hour), EndedAt: now.Add(-96 * time.Hour)}
	if err := st.CreateJobRun(old); err != nil {
		t.Fatal(err)
	}
	if n, err := st.DeleteJobRunsBefore("p", now.Add(-48*time.Hour)); err != nil || n != 1 {
		t.Fatalf("retention: n=%d err=%v", n, err)
	}
	if _, err := st.GetJobRun("p", "run-0"); err == nil {
		t.Error("old run survived retention")
	}
	if _, err := st.GetJobRun("p", "run-1"); err != nil {
		t.Error("recent run deleted by retention")
	}
}

func TestMemoryJobStore(t *testing.T) {
	exerciseJobStore(t, NewMemory("p"))
}

func TestSQLiteJobStore(t *testing.T) {
	st, err := OpenSQLite(t.TempDir() + "/jobs.db")
	if err != nil {
		t.Fatal(err)
	}
	exerciseJobStore(t, st)
}

// A database created by the M1 schema (no job_run_id column) migrates in
// place on open (M2 review R6).
func TestSQLiteMigratesM1DeadLetterTable(t *testing.T) {
	path := t.TempDir() + "/m1.db"
	st, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Simulate an M1 database: drop the column by recreating the table
	// without it, as the M1 schema did.
	if err := rewriteM1DeadLetter(path); err != nil {
		t.Fatal(err)
	}
	st2, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen with migration: %v", err)
	}
	defer st2.Close()
	if err := st2.WriteDeadLetter(DeadLetter{Pipeline: "p", MessageID: "m", RunID: "run-9", Node: "out", Reason: "x", Raw: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	forRun, err := st2.DeadLettersForRun("p", "run-9")
	if err != nil || len(forRun) != 1 {
		t.Fatalf("run-attributed dead letter after migration: %+v %v", forRun, err)
	}
}

func rewriteM1DeadLetter(path string) error {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`
		DROP TABLE dead_letter;
		CREATE TABLE dead_letter (
		  id          INTEGER PRIMARY KEY AUTOINCREMENT,
		  pipeline    TEXT    NOT NULL,
		  message_id  TEXT    NOT NULL,
		  node        TEXT    NOT NULL,
		  edge        TEXT    NOT NULL DEFAULT '',
		  reason      TEXT    NOT NULL,
		  backtrace   TEXT    NOT NULL DEFAULT '',
		  origin_node TEXT    NOT NULL DEFAULT '',
		  raw         BLOB    NOT NULL,
		  codec       TEXT    NOT NULL DEFAULT '',
		  meta        TEXT    NOT NULL DEFAULT '{}',
		  cursor      TEXT    NOT NULL DEFAULT '',
		  src_name    TEXT    NOT NULL DEFAULT '',
		  src_seq     INTEGER NOT NULL DEFAULT 0,
		  retry_count INTEGER NOT NULL DEFAULT 0,
		  created_at  TEXT    NOT NULL
		)`)
	return err
}

func TestSQLitePersistsAcrossReopen(t *testing.T) {
	path := t.TempDir() + "/persist.db"
	st, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.AppendSpool("p", registry.Message{ID: "m-1", Codec: "json", Raw: []byte(`{}`), Meta: map[string]any{}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetCheckpoint("p", 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if cp, _ := st2.Checkpoint("p"); cp != 1 {
		t.Fatalf("checkpoint lost across reopen: %d", cp)
	}
	count := 0
	_ = st2.ReplayFrom("p", 1, func(seq int64, m registry.Message, ts time.Time) error {
		count++
		return nil
	})
	if count != 0 {
		t.Fatalf("settled message replayed: %d", count)
	}
}

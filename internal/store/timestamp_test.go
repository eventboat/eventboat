package store

import (
	"path/filepath"
	"testing"
	"time"
)

// Candidate 04: every persisted timestamp uses the fixed-width timeLayout, so
// the text order equals the time order. With RFC3339Nano the fraction is
// omitted when zero, and "…T12:00:00Z" sorts AFTER "…T12:00:00.5Z" — the
// range deletes and the run-history ORDER BY would silently misorder rows
// that differ only in sub-second precision.
func TestSQLiteTimestampOrdering(t *testing.T) {
	st, err := OpenSQLite(filepath.Join(t.TempDir(), "timestamps.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	t0 := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC) // no fraction
	t1 := t0.Add(500 * time.Millisecond)               // .5 fraction

	for _, dl := range []DeadLetter{
		{Pipeline: "p", MessageID: "early", Node: "out", Reason: "x", Raw: []byte(`{}`), Meta: map[string]any{}, CreatedAt: t0},
		{Pipeline: "p", MessageID: "late", Node: "out", Reason: "x", Raw: []byte(`{}`), Meta: map[string]any{}, CreatedAt: t1},
	} {
		if err := st.WriteDeadLetter(dl); err != nil {
			t.Fatal(err)
		}
	}

	// A cutoff at t1 deletes exactly the t0 row: with the old layout the
	// comparison text order would invert and delete zero rows.
	if n, err := st.DeleteDeadLettersBefore("p", t1); err != nil || n != 1 {
		t.Fatalf("DeleteDeadLettersBefore(t1) = %d, %v; want 1", n, err)
	}
	kept, err := st.DeadLetters("p")
	if err != nil || len(kept) != 1 || kept[0].MessageID != "late" {
		t.Fatalf("kept after sweep = %+v (%v), want only the t1 row", kept, err)
	}
	since, err := st.DeadLettersSince("p", t1)
	if err != nil || len(since) != 1 || since[0].MessageID != "late" {
		t.Fatalf("DeadLettersSince(t1) = %+v (%v), want only the t1 row", since, err)
	}

	// Run history orders by started_at: the later run leads (the old layout
	// would put the fraction-less timestamp first).
	for _, jr := range []JobRun{
		{RunID: "early", Pipeline: "p", Status: JobSuccess, TriggerType: "manual", StartedAt: t0, EndedAt: t0},
		{RunID: "late", Pipeline: "p", Status: JobSuccess, TriggerType: "manual", StartedAt: t1, EndedAt: t1},
	} {
		if err := st.CreateJobRun(jr); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := st.JobRuns("p", 10)
	if err != nil || len(runs) != 2 || runs[0].RunID != "late" {
		t.Fatalf("JobRuns = %+v (%v), want the t1 run first", runs, err)
	}
	// The padded layout still parses back to the exact time.
	if !runs[0].StartedAt.Equal(t1) {
		t.Fatalf("StartedAt roundtrip = %s, want %s", runs[0].StartedAt, t1)
	}
}

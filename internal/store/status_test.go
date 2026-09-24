package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Candidate 08 acceptance 5 (store half): after the JobCommitting removal the
// status enum, the runnable predicate and BOTH backends' queries agree. The
// SQL set is a literal ('pending','running'), so this pins it against the
// in-memory predicate; the retention sweep's NOT IN list is pinned the same
// way.
func TestRunnableStatusQueryConsistency(t *testing.T) {
	statuses := []string{JobPending, JobRunning, JobSuccess, JobPartial, JobFailed, JobCanceled}
	backends := []struct {
		name string
		open func(t *testing.T) Store
	}{
		{"sqlite", func(t *testing.T) Store {
			st, err := OpenSQLite(filepath.Join(t.TempDir(), "runs.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return st
		}},
		{"memory", func(t *testing.T) Store { return NewMemory() }},
	}
	now := time.Now()
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.open(t)
			for i, status := range statuses {
				jr := JobRun{
					RunID: fmt.Sprintf("run-%d", i), Pipeline: "p", Status: status,
					TriggerType: "manual",
					StartedAt:   now.Add(-3 * time.Hour),
					EndedAt:     now.Add(-2 * time.Hour),
				}
				if err := st.CreateJobRun(jr); err != nil {
					t.Fatal(err)
				}
			}

			runs, err := st.RunnableJobRuns("p")
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]bool{}
			for _, r := range runs {
				got[r.Status] = true
			}
			if len(runs) != 2 || !got[JobPending] || !got[JobRunning] {
				t.Fatalf("RunnableJobRuns = %+v, want exactly pending+running", runs)
			}

			// The retention sweep keeps exactly the runnable set: every other
			// status is finished and past the cutoff.
			n, err := st.DeleteJobRunsBefore("p", now.Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if n != int64(len(statuses)-2) {
				t.Fatalf("retention removed %d rows, want %d (all non-runnable)", n, len(statuses)-2)
			}
			left, err := st.RunnableJobRuns("p")
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 2 {
				t.Fatalf("retention removed runnable runs: %+v", left)
			}
		})
	}
}

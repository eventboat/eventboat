package cli

import (
	"testing"

	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/store"
)

// Candidate 02 acceptance 2: the exit-code mappings are pure functions of the
// run outcome / job status, so the process contract is pinned without a
// subprocess. (The end-to-end batch exit codes 0/1 are covered by the root
// TestBatchRunCompletion, which drives the real binary.)

func TestBatchExitCodes(t *testing.T) {
	cases := []struct {
		status engine.RunStatus
		want   int
	}{
		{engine.RunCompleted, 0},
		{engine.RunPartial, 1},
		{engine.RunFailed, 1},
		{engine.RunInterrupted, 1}, // one-shot: a run that did not complete exits non-zero
	}
	for _, tc := range cases {
		if got := batchExitCode(engine.Outcome{Status: tc.status}); got != tc.want {
			t.Errorf("batchExitCode(%s) = %d, want %d", tc.status, got, tc.want)
		}
	}
}

func TestContinuousExitCodes(t *testing.T) {
	cases := []struct {
		status engine.RunStatus
		want   int
	}{
		{engine.RunCompleted, 0},
		{engine.RunPartial, 0}, // dead letters are operator data; the process stopped cleanly
		{engine.RunInterrupted, 0},
		{engine.RunFailed, 1}, // source failure / worker-fatal: a dead pipeline is not a graceful stop
	}
	for _, tc := range cases {
		if got := continuousExitCode(engine.Outcome{Status: tc.status}); got != tc.want {
			t.Errorf("continuousExitCode(%s) = %d, want %d", tc.status, got, tc.want)
		}
	}
}

func TestTriggerExitCodes(t *testing.T) {
	cases := []struct {
		status string
		want   int
	}{
		{store.JobSuccess, 0},
		{store.JobPartial, 1},
		{store.JobFailed, 1},
		{store.JobCanceled, 1},
	}
	for _, tc := range cases {
		if got := triggerExitCode(tc.status); got != tc.want {
			t.Errorf("triggerExitCode(%s) = %d, want %d", tc.status, got, tc.want)
		}
	}
}

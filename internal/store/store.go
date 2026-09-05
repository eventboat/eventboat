// Package store implements the durable spine of a pipeline: the append-only
// spool, the checkpoint, source commit states and the dead letter queue,
// backed by SQLite (modernc.org/sqlite, pure Go — no hand-written WAL,
// redesign-v3.md §6.3) with an in-memory implementation for --ephemeral runs
// and tests.
package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// DeadLetter is one dead-lettered message with its full original content and
// the error context (including Starlark backtraces / CEL error text).
type DeadLetter struct {
	ID         int64          `json:"id"`
	Pipeline   string         `json:"pipeline"`
	MessageID  string         `json:"message_id"`
	RunID      string         `json:"job_run_id"` // job run attribution ("" for continuous pipelines)
	Node       string         `json:"node"`
	Edge       string         `json:"edge"` // "from -> to"
	Reason     string         `json:"reason"`
	Backtrace  string         `json:"backtrace"`
	OriginNode string         `json:"origin_node"`
	Raw        []byte         `json:"raw"`
	Codec      string         `json:"codec"`
	Meta       map[string]any `json:"meta"`
	Cursor     string         `json:"cursor"`
	SrcName    string         `json:"src_name"`
	SrcSeq     int64          `json:"src_seq"`
	RetryCount int            `json:"retry_count"`
	CreatedAt  time.Time      `json:"created_at"`
}

// Job statuses (redesign-v3.md §5.8 lifecycle).
const (
	JobPending    = "pending"
	JobRunning    = "running"
	JobCommitting = "committing"
	JobSuccess    = "success"
	JobPartial    = "partial"
	JobFailed     = "failed"
	JobCanceled   = "canceled"
)

// JobRun is one job-pipeline execution record (run history, §5.8).
type JobRun struct {
	RunID        string         `json:"run_id"`
	Pipeline     string         `json:"pipeline"`
	Status       string         `json:"status"`  // pending|running|committing|success|partial|failed|canceled
	TriggerType  string         `json:"trigger"` // schedule|manual|catchup
	Parameters   map[string]any `json:"parameters"`
	ScheduledFor string         `json:"scheduled_for"` // RFC3339 tick identity ("" for manual runs)
	StartedAt    time.Time      `json:"started_at"`
	EndedAt      time.Time      `json:"ended_at"`
	RowsRead     int64          `json:"rows_read"`
	Delivered    int64          `json:"delivered"`
	DeadLettered int64          `json:"dead_lettered"`
	Error        string         `json:"error"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

// Runnable reports whether the run was in flight when its process died and
// must be resumed (or failed) on restart.
func (j JobRun) Runnable() bool {
	return j.Status == JobPending || j.Status == JobRunning || j.Status == JobCommitting
}

// Store is the persistence surface used by the engine. All methods must be
// safe for concurrent use by engine internals.
type Store interface {
	// AppendSpool durably records an inbound message and returns its spool
	// sequence. A message must not become visible to the DAG before this
	// succeeds (invariant 1).
	AppendSpool(pipeline string, msg registry.Message, ingestTime time.Time) (seq int64, err error)

	// ReplayFrom walks spooled messages with seq > afterSeq in order.
	ReplayFrom(pipeline string, afterSeq int64, fn func(seq int64, msg registry.Message, ingestTime time.Time) error) error

	// ReplayPage walks at most limit spooled messages with seq > afterSeq in
	// order, without materializing the whole window (M2 review R7). It
	// returns the last sequence visited and whether more remain.
	ReplayPage(pipeline string, afterSeq int64, limit int, fn func(seq int64, msg registry.Message, ingestTime time.Time) error) (lastSeq int64, more bool, err error)

	// SetCheckpoint persists the contiguous committed-through spool sequence
	// (invariant 2: only after commit).
	SetCheckpoint(pipeline string, seq int64) error

	// Checkpoint reads the persisted committed-through sequence.
	Checkpoint(pipeline string) (int64, error)

	// SetSourceState persists a source's commit state at srcSeq.
	SetSourceState(pipeline, source string, state []byte, srcSeq int64) error

	// SourceState reads a source's commit state and frontier.
	SourceState(pipeline, source string) (state []byte, srcSeq int64, err error)

	// WriteDeadLetter durably records a dead letter. Failure here must block
	// commit (invariant 4), never drop the message.
	WriteDeadLetter(dl DeadLetter) error

	// DeadLetters lists dead letters for a pipeline (newest first).
	DeadLetters(pipeline string) ([]DeadLetter, error)

	// DeadLettersSince lists dead letters created at or after since
	// (newest first); a zero since returns everything.
	DeadLettersSince(pipeline string, since time.Time) ([]DeadLetter, error)

	// DeadLettersForRun lists dead letters attributed to one job run.
	DeadLettersForRun(pipeline, runID string) ([]DeadLetter, error)

	// DeleteDeadLetters removes the listed dead letters (after successful
	// replay reinjection) and returns how many were removed.
	DeleteDeadLetters(pipeline string, ids []int64) (int64, error)

	// --- job run history (§5.8) ---

	// CreateJobRun inserts a run record.
	CreateJobRun(jr JobRun) error

	// UpdateJobRun persists the mutable fields of one run.
	UpdateJobRun(jr JobRun) error

	// GetJobRun reads one run by run-id.
	GetJobRun(pipeline, runID string) (*JobRun, error)

	// JobRuns lists runs of one pipeline, newest first, up to limit
	// (limit <= 0 means a sane default).
	JobRuns(pipeline string, limit int) ([]JobRun, error)

	// RunnableJobRuns lists in-flight runs of one pipeline (crash recovery).
	RunnableJobRuns(pipeline string) ([]JobRun, error)

	// HasSuccessfulRunFor reports whether a run with this scheduled_for tick
	// already succeeded (skip_if_successful).
	HasSuccessfulRunFor(pipeline, scheduledFor string) (bool, error)

	// LastScheduledFor returns the newest scheduled_for of any run of the
	// pipeline ("" when none exist) — catchup needs the last processed tick.
	LastScheduledFor(pipeline string) (string, error)

	// DeleteJobRunsBefore deletes finished runs ended before cutoff and
	// returns how many were removed (retention).
	DeleteJobRunsBefore(pipeline string, cutoff time.Time) (int64, error)

	Close() error
}

// --- in-memory implementation (ephemeral mode, tests) ---

type memRow struct {
	seq        int64
	msg        registry.Message
	ingestTime time.Time
}

type memStore struct {
	mu          sync.Mutex
	pipeline    string
	spool       []memRow
	checkpoints map[string]int64
	srcStates   map[string]memSrcState
	deadLetters []DeadLetter
	jobRuns     []JobRun
	nextDLID    int64
	closed      bool
}

type memSrcState struct {
	state  []byte
	srcSeq int64
}

// NewMemory returns an in-memory store bound to one pipeline name.
func NewMemory(pipeline string) Store {
	return &memStore{
		pipeline:    pipeline,
		checkpoints: map[string]int64{},
		srcStates:   map[string]memSrcState{},
	}
}

func (s *memStore) AppendSpool(pipeline string, msg registry.Message, ingestTime time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("store closed")
	}
	seq := int64(len(s.spool) + 1)
	s.spool = append(s.spool, memRow{seq: seq, msg: msg, ingestTime: ingestTime})
	return seq, nil
}

func (s *memStore) ReplayFrom(pipeline string, afterSeq int64, fn func(int64, registry.Message, time.Time) error) error {
	s.mu.Lock()
	rows := append([]memRow(nil), s.spool...)
	s.mu.Unlock()
	for _, r := range rows {
		if r.seq <= afterSeq {
			continue
		}
		if err := fn(r.seq, r.msg, r.ingestTime); err != nil {
			return err
		}
	}
	return nil
}

func (s *memStore) ReplayPage(pipeline string, afterSeq int64, limit int, fn func(int64, registry.Message, time.Time) error) (int64, bool, error) {
	if limit <= 0 {
		limit = 500
	}
	s.mu.Lock()
	rows := append([]memRow(nil), s.spool...)
	s.mu.Unlock()
	last := afterSeq
	count := 0
	for _, r := range rows {
		if r.seq <= afterSeq {
			continue
		}
		if count >= limit {
			return last, true, nil
		}
		if err := fn(r.seq, r.msg, r.ingestTime); err != nil {
			return last, false, err
		}
		last = r.seq
		count++
	}
	return last, false, nil
}

func (s *memStore) SetCheckpoint(pipeline string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[pipeline] = seq
	return nil
}

func (s *memStore) Checkpoint(pipeline string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoints[pipeline], nil
}

func (s *memStore) SetSourceState(pipeline, source string, state []byte, srcSeq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.srcStates[sourceKey(pipeline, source)] = memSrcState{state: state, srcSeq: srcSeq}
	return nil
}

func (s *memStore) SourceState(pipeline, source string) ([]byte, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.srcStates[sourceKey(pipeline, source)]
	return st.state, st.srcSeq, nil
}

func (s *memStore) WriteDeadLetter(dl DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextDLID++
	dl.ID = s.nextDLID
	if dl.CreatedAt.IsZero() {
		dl.CreatedAt = time.Now()
	}
	s.deadLetters = append(s.deadLetters, dl)
	return nil
}

func (s *memStore) DeadLetters(pipeline string) ([]DeadLetter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]DeadLetter, 0, len(s.deadLetters))
	for i := len(s.deadLetters) - 1; i >= 0; i-- {
		if s.deadLetters[i].Pipeline == pipeline {
			out = append(out, s.deadLetters[i])
		}
	}
	return out, nil
}

func (s *memStore) DeadLettersSince(pipeline string, since time.Time) ([]DeadLetter, error) {
	all, err := s.DeadLetters(pipeline)
	if err != nil {
		return nil, err
	}
	if since.IsZero() {
		return all, nil
	}
	out := all[:0:0]
	for _, dl := range all {
		if !dl.CreatedAt.Before(since) {
			out = append(out, dl)
		}
	}
	return out, nil
}

func (s *memStore) DeadLettersForRun(pipeline, runID string) ([]DeadLetter, error) {
	all, err := s.DeadLetters(pipeline)
	if err != nil {
		return nil, err
	}
	var out []DeadLetter
	for _, dl := range all {
		if dl.RunID == runID {
			out = append(out, dl)
		}
	}
	return out, nil
}

func (s *memStore) DeleteDeadLetters(pipeline string, ids []int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	drop := map[int64]bool{}
	for _, id := range ids {
		drop[id] = true
	}
	kept := s.deadLetters[:0:0]
	var removed int64
	for _, dl := range s.deadLetters {
		if dl.Pipeline == pipeline && drop[dl.ID] {
			removed++
			continue
		}
		kept = append(kept, dl)
	}
	s.deadLetters = kept
	return removed, nil
}

// --- job run history (in-memory) ---

func (s *memStore) CreateJobRun(jr JobRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if jr.UpdatedAt.IsZero() {
		jr.UpdatedAt = time.Now()
	}
	s.jobRuns = append(s.jobRuns, jr)
	return nil
}

func (s *memStore) UpdateJobRun(jr JobRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.jobRuns {
		if s.jobRuns[i].RunID == jr.RunID && s.jobRuns[i].Pipeline == jr.Pipeline {
			jr.UpdatedAt = time.Now()
			s.jobRuns[i] = jr
			return nil
		}
	}
	return fmt.Errorf("store: job run %q not found", jr.RunID)
}

func (s *memStore) GetJobRun(pipeline, runID string) (*JobRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.jobRuns) - 1; i >= 0; i-- {
		if s.jobRuns[i].Pipeline == pipeline && s.jobRuns[i].RunID == runID {
			jr := s.jobRuns[i]
			return &jr, nil
		}
	}
	return nil, fmt.Errorf("store: job run %q not found", runID)
}

func (s *memStore) JobRuns(pipeline string, limit int) ([]JobRun, error) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []JobRun
	for i := len(s.jobRuns) - 1; i >= 0 && len(out) < limit; i-- {
		if s.jobRuns[i].Pipeline == pipeline {
			out = append(out, s.jobRuns[i])
		}
	}
	return out, nil
}

func (s *memStore) RunnableJobRuns(pipeline string) ([]JobRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []JobRun
	for i := len(s.jobRuns) - 1; i >= 0; i-- {
		if s.jobRuns[i].Pipeline == pipeline && s.jobRuns[i].Runnable() {
			out = append(out, s.jobRuns[i])
		}
	}
	return out, nil
}

func (s *memStore) HasSuccessfulRunFor(pipeline, scheduledFor string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, jr := range s.jobRuns {
		if jr.Pipeline == pipeline && jr.ScheduledFor == scheduledFor && jr.Status == JobSuccess {
			return true, nil
		}
	}
	return false, nil
}

func (s *memStore) LastScheduledFor(pipeline string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := ""
	for _, jr := range s.jobRuns {
		if jr.Pipeline == pipeline && jr.ScheduledFor > last {
			last = jr.ScheduledFor
		}
	}
	return last, nil
}

func (s *memStore) DeleteJobRunsBefore(pipeline string, cutoff time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.jobRuns[:0:0]
	var removed int64
	for _, jr := range s.jobRuns {
		if jr.Pipeline == pipeline && !jr.Runnable() && !jr.EndedAt.IsZero() && jr.EndedAt.Before(cutoff) {
			removed++
			continue
		}
		kept = append(kept, jr)
	}
	s.jobRuns = kept
	return removed, nil
}

func (s *memStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func sourceKey(pipeline, source string) string { return pipeline + "\x00" + source }

// marshalMeta is shared JSON encoding of message metadata.
func marshalMeta(meta map[string]any) []byte {
	if meta == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func unmarshalMeta(b []byte) map[string]any {
	if len(b) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

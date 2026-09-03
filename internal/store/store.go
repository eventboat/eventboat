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
	ID         int64
	Pipeline   string
	MessageID  string
	Node       string
	Edge       string // "from -> to"
	Reason     string
	Backtrace  string
	OriginNode string
	Raw        []byte
	Codec      string
	Meta       map[string]any
	Cursor     string
	SrcName    string
	SrcSeq     int64
	RetryCount int
	CreatedAt  time.Time
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

	// SetCheckpoint persists the contiguous settled-through spool sequence
	// (invariant 2: only after settle).
	SetCheckpoint(pipeline string, seq int64) error

	// Checkpoint reads the persisted settled-through sequence.
	Checkpoint(pipeline string) (int64, error)

	// SetSourceState persists a source's commit state at srcSeq.
	SetSourceState(pipeline, source string, state []byte, srcSeq int64) error

	// SourceState reads a source's commit state and frontier.
	SourceState(pipeline, source string) (state []byte, srcSeq int64, err error)

	// WriteDeadLetter durably records a dead letter. Failure here must block
	// settle (invariant 4), never drop the message.
	WriteDeadLetter(dl DeadLetter) error

	// DeadLetters lists dead letters for a pipeline (newest first).
	DeadLetters(pipeline string) ([]DeadLetter, error)

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

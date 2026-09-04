package testkit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/eventboat/eventboat/internal/registry"
)

// FakePullSource is a deterministic pull source for engine/jobs tests
// (M2 review: engine-level job semantics need injectable rows and exhaustion
// control without a database). Configure through the "fakepull" plugin:
//
//	fakepull: { id: feed }   // the instance is looked up by id
//
// Tests stage rows with Stage/StageJSON and control failure with FailNext.
type FakePullSource struct {
	ID string

	mu        sync.Mutex
	rows      []fakeRow
	failNext  bool
	watermark string // committed watermark (from Settled)
	pending   map[int64]fakeRow
	nextSeq   int64
	exhausted bool
}

type fakeRow struct {
	payload any
	cursor  string
}

// Stage queues one row (payload must be JSON-marshalable) with its cursor.
func (s *FakePullSource) Stage(payload any, cursor string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, fakeRow{payload: payload, cursor: cursor})
}

// StageJSON queues one raw JSON payload.
func (s *FakePullSource) StageJSON(raw string, cursor string) {
	var v any
	_ = json.Unmarshal([]byte(raw), &v)
	s.Stage(v, cursor)
}

// FailNext makes the next Pull return an error (source failure → run failed).
func (s *FakePullSource) FailNext() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = true
}

// Watermark returns the committed cursor watermark.
func (s *FakePullSource) Watermark() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark
}

func (s *FakePullSource) Init(state []byte) error {
	if len(state) == 0 {
		return nil
	}
	var st struct {
		Watermark string `json:"watermark"`
	}
	if err := json.Unmarshal(state, &st); err != nil {
		return fmt.Errorf("fakepull: bad state: %w", err)
	}
	s.mu.Lock()
	s.watermark = st.Watermark
	s.mu.Unlock()
	return nil
}

// Pull emits staged rows with cursor strictly after the restored watermark
// (rows resume after the settled frontier), then reports exhaustion.
func (s *FakePullSource) Pull(ctx context.Context, emit func(registry.Message)) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.mu.Unlock()
		return fmt.Errorf("fakepull: injected source failure")
	}
	rows := append([]fakeRow(nil), s.rows...)
	s.nextSeq = 0
	s.pending = map[int64]fakeRow{}
	s.exhausted = false
	s.mu.Unlock()

	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.cursor <= s.readWatermark() {
			continue // already settled in a previous run
		}
		raw, err := json.Marshal(r.payload)
		if err != nil {
			return fmt.Errorf("fakepull: marshal: %w", err)
		}
		s.mu.Lock()
		s.nextSeq++
		seq := s.nextSeq
		s.pending[seq] = r
		s.mu.Unlock()
		// Emit blocks in the engine's admission gate: backpressure pauses
		// the pull naturally (§5.8 point 4).
		emit(registry.Message{Raw: raw, Codec: "json", SrcName: "fakepull", SrcSeq: seq, Cursor: r.cursor})
	}
	return nil
}

func (s *FakePullSource) readWatermark() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark
}

// Settled commits the watermark at the contiguous settled frontier.
func (s *FakePullSource) Settled(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxSeq int64
	var maxRow fakeRow
	for seq := int64(1); seq <= throughSrcSeq; seq++ {
		if r, ok := s.pending[seq]; ok {
			if seq > maxSeq {
				maxSeq = seq
				maxRow = r
			}
			delete(s.pending, seq)
		}
	}
	if maxSeq > 0 {
		s.watermark = maxRow.cursor
	}
	st, _ := json.Marshal(map[string]string{"watermark": s.watermark})
	return st, nil
}

func (s *FakePullSource) Run(ctx context.Context, emit func(registry.Message)) {
	_ = s.Pull(ctx, emit)
	<-ctx.Done()
}

func (s *FakePullSource) Close() error { return nil }

// fakePullRegistry wires FakePullSource instances to a plugin name, sharing
// the lookup-by-id convention with the engine test harness.
type fakePullRegistry struct {
	mu    sync.Mutex
	known map[string]*FakePullSource
}

var fakePull = &fakePullRegistry{known: map[string]*FakePullSource{}}

// FakePull returns (creating if needed) the staged source under id.
func FakePull(id string) *FakePullSource {
	fakePull.mu.Lock()
	defer fakePull.mu.Unlock()
	if s, ok := fakePull.known[id]; ok {
		return s
	}
	s := &FakePullSource{ID: id}
	fakePull.known[id] = s
	return s
}

// ResetFakePull drops all staged sources (test isolation).
func ResetFakePull() {
	fakePull.mu.Lock()
	defer fakePull.mu.Unlock()
	fakePull.known = map[string]*FakePullSource{}
}

// RegisterFakePull registers the "fakepull" source plugin (capabilities:
// [pull]) into reg. Instances resolve by their id config field.
func RegisterFakePull(reg *registry.Registry) error {
	return reg.RegisterSource("fakepull", 1, `{
		"type": "object",
		"required": ["id"],
		"properties": { "id": { "type": "string", "minLength": 1 } },
		"additionalProperties": false
	}`, []string{"pull"}, func(cfg map[string]any) (registry.Source, error) {
		id, _ := cfg["id"].(string)
		if id == "" {
			return nil, fmt.Errorf("fakepull: id is required")
		}
		return FakePull(id), nil
	})
}

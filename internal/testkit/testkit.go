// Package testkit provides injection, capture and fault-injection primitives
// for engine tests and the contract test runner (redesign-v3.md §3.2), plus
// a deterministic clock and ID generator.
package testkit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// FixedClock returns a frozen clock.
func FixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// CounterID returns an ID generator producing id-000001-style values.
func CounterID() func() string {
	var mu sync.Mutex
	n := 0
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return fmt.Sprintf("id-%06d", n)
	}
}

// ManualSource is a source whose emissions are driven by the test: Emit
// pushes one raw message and returns only after the engine has accepted it
// (spooled + dispatched), making test flow deterministic. Run may be called
// multiple times across engine restarts sharing the same instance.
type ManualSource struct {
	Name    string
	mu      sync.Mutex
	emitted chan manualEmission
	runCtx  context.Context
	started chan struct{}
	once    sync.Once
	cursors map[int64]string
	nextSeq int64
	state   []byte
}

type manualEmission struct {
	msg  registry.Message
	done chan struct{}
}

// NewManualSource builds a manual source registered under a plugin name.
func NewManualSource() *ManualSource {
	return &ManualSource{emitted: make(chan manualEmission, 1024), started: make(chan struct{})}
}

func (s *ManualSource) Init(state []byte) error { s.state = state; return nil }

func (s *ManualSource) Run(ctx context.Context, emit func(registry.Message)) {
	s.mu.Lock()
	s.runCtx = ctx
	s.mu.Unlock()
	s.once.Do(func() { close(s.started) })
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-s.emitted:
			emit(e.msg)
			close(e.done)
		}
	}
}

// Emit pushes a raw message and blocks until the engine has accepted it
// (or the engine context is done). Cursor (when non-empty) participates in
// the settle watermark via Settled.
func (s *ManualSource) Emit(raw []byte, cursor string) {
	<-s.started
	s.mu.Lock()
	s.nextSeq++
	seq := s.nextSeq
	if cursor != "" {
		if s.cursors == nil {
			s.cursors = map[int64]string{}
		}
		s.cursors[seq] = cursor
	}
	s.mu.Unlock()
	done := make(chan struct{})
	select {
	case s.emitted <- manualEmission{msg: registry.Message{Raw: raw, SrcName: s.Name, SrcSeq: seq, Cursor: cursor}, done: done}:
	case <-s.runCtx.Done():
		return
	}
	select {
	case <-done:
	case <-s.runCtx.Done():
	}
}

// Settled records the watermark: it persists the cursor of the highest
// settled emission (mirrors how a future sql pull source would commit).
func (s *ManualSource) Settled(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxCur string
	for seq, c := range s.cursors {
		if seq <= throughSrcSeq && c > maxCur {
			maxCur = c
		}
	}
	if maxCur == "" {
		return nil, nil
	}
	st, _ := json.Marshal(map[string]string{"watermark": maxCur})
	s.state = st
	return st, nil
}

func (s *ManualSource) Close() error { return nil }

// Recorder captures messages delivered to a wrapped sink.
type Recorder struct {
	Name     string
	mu       sync.Mutex
	messages []registry.Message
}

// Captured returns a copy of the captured messages in arrival order.
func (r *Recorder) Captured() []registry.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]registry.Message(nil), r.messages...)
}

// Count returns the number of captured messages.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.messages)
}

// CaptureSink wraps a real sink and records every batch after a successful
// write.
type CaptureSink struct {
	Inner registry.Sink
	Rec   *Recorder
}

// NewCaptureSink wraps sink with a recorder.
func NewCaptureSink(name string, inner registry.Sink) *CaptureSink {
	return &CaptureSink{Inner: inner, Rec: &Recorder{Name: name}}
}

func (s *CaptureSink) Write(ctx context.Context, msgs []registry.Message) error {
	err := s.Inner.Write(ctx, msgs)
	if err == nil {
		s.Rec.mu.Lock()
		s.Rec.messages = append(s.Rec.messages, msgs...)
		s.Rec.mu.Unlock()
	}
	return err
}

func (s *CaptureSink) Close() error { return s.Inner.Close() }

// DiscardSink accepts everything and writes nowhere (contract tests).
type DiscardSink struct{}

func (DiscardSink) Write(ctx context.Context, msgs []registry.Message) error { return nil }
func (DiscardSink) Close() error                                             { return nil }

// FlakySink fails writes according to a predicate; Failures counts them.
type FlakySink struct {
	Inner    registry.Sink
	Fail     func(attempt int) bool
	Failures int
}

func (s *FlakySink) Write(ctx context.Context, msgs []registry.Message) error {
	s.Failures++
	if s.Fail != nil && s.Fail(s.Failures) {
		return fmt.Errorf("flaky sink: injected failure #%d", s.Failures)
	}
	return s.Inner.Write(ctx, msgs)
}

func (s *FlakySink) Close() error { return s.Inner.Close() }

// StoreWrapper wraps a store with fault hooks; nil hooks delegate.
type StoreWrapper struct {
	Inner             store.Store
	AppendHook        func(msg registry.Message) error
	DeadLetterHook    func(dl store.DeadLetter) error
	SetCheckpointHook func(seq int64) error
}

func (w *StoreWrapper) AppendSpool(pipeline string, msg registry.Message, ingestTime time.Time) (int64, error) {
	if w.AppendHook != nil {
		if err := w.AppendHook(msg); err != nil {
			return 0, err
		}
	}
	return w.Inner.AppendSpool(pipeline, msg, ingestTime)
}

func (w *StoreWrapper) ReplayFrom(pipeline string, afterSeq int64, fn func(int64, registry.Message, time.Time) error) error {
	return w.Inner.ReplayFrom(pipeline, afterSeq, fn)
}

func (w *StoreWrapper) SetCheckpoint(pipeline string, seq int64) error {
	if w.SetCheckpointHook != nil {
		if err := w.SetCheckpointHook(seq); err != nil {
			return err
		}
	}
	return w.Inner.SetCheckpoint(pipeline, seq)
}

func (w *StoreWrapper) Checkpoint(pipeline string) (int64, error) {
	return w.Inner.Checkpoint(pipeline)
}

func (w *StoreWrapper) SetSourceState(pipeline, source string, state []byte, srcSeq int64) error {
	return w.Inner.SetSourceState(pipeline, source, state, srcSeq)
}

func (w *StoreWrapper) SourceState(pipeline, source string) ([]byte, int64, error) {
	return w.Inner.SourceState(pipeline, source)
}

func (w *StoreWrapper) WriteDeadLetter(dl store.DeadLetter) error {
	if w.DeadLetterHook != nil {
		if err := w.DeadLetterHook(dl); err != nil {
			return err
		}
	}
	return w.Inner.WriteDeadLetter(dl)
}

func (w *StoreWrapper) DeadLetters(pipeline string) ([]store.DeadLetter, error) {
	return w.Inner.DeadLetters(pipeline)
}

func (w *StoreWrapper) Close() error { return w.Inner.Close() }

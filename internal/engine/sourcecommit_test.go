package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// Candidate 01 acceptance: the per-source committer. Advances coalesce to the
// maximum, a failed commit keeps its pending value and retries on the next
// advance, and the shutdown flush leaves SetSourceState at the latest
// frontier. The file-source test pins the deadlock regression: a source may
// hold an internal lock across emit, and watermark persistence must not run
// on the committing goroutine.

// countingSource is a test source that records Commit calls, can hold the
// first call on a gate (deterministic coalescing) and can fail once.
type countingSource struct {
	mu       sync.Mutex
	calls    int
	max      int64
	nextSeq  int64
	failNext bool
	gate     chan struct{}
	emitFn   func(registry.Message) error

	started chan struct{}
	once    sync.Once
}

func newCountingSource() *countingSource {
	return &countingSource{started: make(chan struct{})}
}

func (s *countingSource) Init([]byte) error { return nil }

func (s *countingSource) Run(ctx context.Context, emit func(registry.Message) error) error {
	s.mu.Lock()
	s.emitFn = emit
	s.mu.Unlock()
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return nil // cancelled: a voluntary stop
}

func (s *countingSource) Commit(ctx context.Context, through int64) ([]byte, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	if through > s.max {
		s.max = through
	}
	fail := s.failNext
	s.failNext = false
	gate := s.gate
	s.mu.Unlock()
	if first && gate != nil {
		<-gate
	}
	if fail {
		return nil, errString("commit refused")
	}
	return []byte(fmt.Sprintf(`{"through":%d}`, through)), nil
}

func (s *countingSource) Close() error { return nil }

// emit pushes one message through the engine's emit callback and returns the
// admission verdict (nil = accepted).
func (s *countingSource) emit(raw string) error {
	<-s.started
	s.mu.Lock()
	s.nextSeq++
	seq := s.nextSeq
	emit := s.emitFn
	s.mu.Unlock()
	return emit(registry.Message{Raw: []byte(raw), SrcName: "counting", SrcSeq: seq})
}

func (s *countingSource) snapshot() (calls int, max int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.max
}

func registerCountingSource(t *testing.T, h *harness) *countingSource {
	t.Helper()
	src := newCountingSource()
	err := h.reg.RegisterSource("counting", 1, manualSchema, nil, func(cfg map[string]any) (registry.Source, error) {
		return src, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

const countingYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: counting }
sources:
  in:
    decoder: json
    counting: {}
sinks:
  out:
    depends_on: [in]
    mem: { id: out }
`

// Many frontier advances converge to one Commit with the maximum: the first
// commit is held on a gate while later advances coalesce, so exactly one more
// call carries the final frontier.
func TestSourceCommitterCoalescesAdvances(t *testing.T) {
	h := newHarness(t)
	src := registerCountingSource(t, h)
	gate := make(chan struct{})
	src.gate = gate
	t.Cleanup(func() { safeClose(gate) })
	pip := h.build(countingYAML)
	st := &testkit.StoreWrapper{Inner: store.NewMemory()}
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	for i := 1; i <= 10; i++ {
		if err := src.emit(fmt.Sprintf(`{"i":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		calls, _ := src.snapshot()
		return calls >= 1 // the first commit is now held on the gate
	})
	waitCommit(t, eng) // every message committed; every frontier posted
	safeClose(gate)

	waitFor(t, func() bool {
		_, max := src.snapshot()
		return max == 10
	})
	calls, max := src.snapshot()
	if calls > 2 {
		t.Fatalf("commit calls = %d, want ≤ 2 (coalesced maximum)", calls)
	}
	if max != 10 {
		t.Fatalf("max frontier = %d, want 10", max)
	}
	waitFor(t, func() bool {
		_, seq, err := st.SourceState("counting", "in")
		return err == nil && seq == 10
	})
}

// A failed commit keeps its pending frontier and is retried on the next
// advance — the failed value is never persisted, the retried one is.
func TestSourceCommitterRetriesFailedCommitOnNextAdvance(t *testing.T) {
	h := newHarness(t)
	src := registerCountingSource(t, h)
	src.failNext = true
	pip := h.build(countingYAML)
	st := &testkit.StoreWrapper{Inner: store.NewMemory()}
	var mu sync.Mutex
	var persisted []int64
	st.SetSourceStateHook = func(_, _ string, _ []byte, srcSeq int64) error {
		mu.Lock()
		persisted = append(persisted, srcSeq)
		mu.Unlock()
		return nil
	}
	eng, _ := runEngine(t, pip, st, h.reg, fastOptions())

	if err := src.emit(`{"i":1}`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		calls, _ := src.snapshot()
		return calls >= 1 // the first commit failed
	})
	if _, seq, _ := st.SourceState("counting", "in"); seq != 0 {
		t.Fatalf("failed commit was persisted (frontier %d)", seq)
	}

	if err := src.emit(`{"i":2}`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, seq, err := st.SourceState("counting", "in")
		return err == nil && seq == 2
	})
	calls, max := src.snapshot()
	if calls < 2 {
		t.Fatalf("failed commit was not retried on the next advance (calls=%d)", calls)
	}
	if max != 2 {
		t.Fatalf("max frontier = %d, want 2", max)
	}
	mu.Lock()
	got := append([]int64(nil), persisted...)
	mu.Unlock()
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("persisted frontiers = %v, want exactly [2]", got)
	}
	waitCommit(t, eng)
}

// A commit that failed while the run was live is retried by the shutdown
// flush: after Run returns, SetSourceState is at the latest frontier.
func TestSourceCommitterShutdownFlushPersistsLatestFrontier(t *testing.T) {
	h := newHarness(t)
	src := registerCountingSource(t, h)
	src.failNext = true
	pip := h.build(countingYAML)
	st := &testkit.StoreWrapper{Inner: store.NewMemory()}
	eng, stop := runEngine(t, pip, st, h.reg, fastOptions())

	if err := src.emit(`{"i":1}`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		calls, _ := src.snapshot()
		return calls >= 1 // failed; pending retained, no further advance
	})
	waitCommit(t, eng)
	stop() // Run's shutdown: drain, stop the committer, flush synchronously

	state, seq, err := st.SourceState("counting", "in")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("source state frontier = %d, want 1 (shutdown flush)", seq)
	}
	if !strings.Contains(string(state), `"through":1`) {
		t.Fatalf("source state = %s, want through=1", state)
	}
	if calls, _ := src.snapshot(); calls < 2 {
		t.Fatalf("shutdown flush did not retry the failed commit (calls=%d)", calls)
	}
}

// slowSink delays every write, keeping the file source's pump ahead of the
// sink until the admission gate fills.
type slowSink struct {
	inner registry.Sink
	delay time.Duration
}

func (s *slowSink) Write(ctx context.Context, msgs []registry.Message) error {
	time.Sleep(s.delay)
	return s.inner.Write(ctx, msgs)
}

func (s *slowSink) Close() error { return s.inner.Close() }

// The file source holds its mutex across emit and Commit takes the same
// mutex: a backpressured file source deadlocked against the old synchronous
// commit path (the committing goroutine waited for a lock the pump held while
// blocked on the gate). With the per-source committer the run completes, the
// committer sees the final frontier and the state is persisted.
func TestLockHoldingFileSourceUnderBackpressureCompletes(t *testing.T) {
	h := newHarness(t)
	const lines = 40
	dir := t.TempDir()
	path := filepath.Join(dir, "input.jsonl")
	var sb strings.Builder
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&sb, `{"i":%d}`+"\n", i)
	}
	content := sb.String()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	pip := h.build(fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: filesrc }
sources:
  in:
    decoder: json
    file: { path: %q, poll_every_ms: 10, on_eof: stop }
sinks:
  out:
    depends_on: [in]
    mem: { id: out }
`, filepath.ToSlash(path)))

	wrapped := &testkit.StoreWrapper{Inner: store.NewMemory()}
	var mu sync.Mutex
	var commits []int64
	wrapped.SetSourceStateHook = func(pipeline, source string, state []byte, srcSeq int64) error {
		mu.Lock()
		commits = append(commits, srcSeq)
		mu.Unlock()
		return nil
	}
	opts := fastOptions()
	opts.ChannelSize = 2
	opts.HighWatermark = 4
	opts.SinkWrapper = func(node string, s registry.Sink) registry.Sink {
		return &slowSink{inner: s, delay: 2 * time.Millisecond}
	}
	eng, stop := runEngine(t, pip, wrapped, h.reg, opts)

	deadline := time.Now().Add(10 * time.Second)
	for !eng.SourcesDone() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !eng.SourcesDone() {
		t.Fatal("file source never exhausted: the pump's lock deadlocked the commit path")
	}
	waitCommit(t, eng)
	stop()

	mu.Lock()
	last := int64(0)
	if len(commits) > 0 {
		last = commits[len(commits)-1]
	}
	mu.Unlock()
	if last != lines {
		t.Fatalf("committer's last frontier = %d, want %d", last, lines)
	}
	state, srcSeq, err := wrapped.SourceState("filesrc", "in")
	if err != nil {
		t.Fatal(err)
	}
	if srcSeq != lines {
		t.Fatalf("persisted source frontier = %d, want %d", srcSeq, lines)
	}
	if !strings.Contains(string(state), fmt.Sprintf(`"offset":%d`, len(content))) {
		t.Fatalf("persisted offset state = %s, want the file's end offset %d", state, len(content))
	}
}

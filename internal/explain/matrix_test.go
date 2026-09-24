package explain

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// Candidate 09 acceptance 2: the comparison matrix. For one deterministic
// scenario set (when/route, zero-match filtering, optional drop, transform
// dead letters, split expansion) the walkthrough's predictions are compared
// with the real engine's terminal states. The engine runs on the same IR the
// walkthrough walked, with testkit's manual source and a recording sink, so
// both sides describe the same execution.
//
// Semantics of the comparison: the trace predicts which sinks a message
// REACHES (write attempts) and how it terminates (drop/filter/dead letter);
// the engine reports the same reach set (the sink records every write
// attempt — every scenario pins delivery retries to 0 so attempts ==
// messages) plus the terminal counters. `delivered` is engine-only: explain
// does not simulate sink writes.

type matrixOutcome struct {
	reached    map[string]int // sink node -> messages that entered a write attempt
	deadLetter string         // transform node that dead-lettered ("" = none)
	dropped    int            // optional-edge drops
	filtered   int            // commit-as-filtered
}

type matrixCase struct {
	name      string
	yaml      string
	message   string
	failSinks []string // sinks whose writes always fail (the fault the scenario needs)
	want      matrixOutcome
	// engine-only terminal facts
	wantClass     string // dead-letter class ("" = no dead letter)
	wantDelivered int    // successful sink writes
	wantExpand    int    // split fan-out children the trace must report
}

const matrixSourceSchema = `{"type":"object","properties":{"id":{"type":"string"}},"additionalProperties":false}`

// matrixSink records every write attempt (the trace's "reached") and the
// successful writes (the engine's terminal delivery). fail models an edge
// that cannot deliver.
type matrixSink struct {
	mu        sync.Mutex
	id        string
	fail      bool
	writes    int
	delivered int
}

func (s *matrixSink) Write(_ context.Context, msgs []registry.Message) error {
	s.mu.Lock()
	s.writes += len(msgs)
	s.mu.Unlock()
	if s.fail {
		return errors.New("matrix sink: injected write failure")
	}
	s.mu.Lock()
	s.delivered += len(msgs)
	s.mu.Unlock()
	return nil
}

func (s *matrixSink) Close() error { return nil }

func (s *matrixSink) stats() (writes, delivered int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes, s.delivered
}

type matrixHarness struct {
	t       *testing.T
	reg     *registry.Registry
	mu      sync.Mutex
	sources map[string]*testkit.ManualSource
	sinks   map[string]*matrixSink
}

func newMatrixHarness(t *testing.T, failing ...string) *matrixHarness {
	t.Helper()
	failSet := map[string]bool{}
	for _, id := range failing {
		failSet[id] = true
	}
	h := &matrixHarness{
		t:       t,
		reg:     registry.New(),
		sources: map[string]*testkit.ManualSource{},
		sinks:   map[string]*matrixSink{},
	}
	if err := builtin.RegisterAll(h.reg); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSource("manual", 1, matrixSourceSchema, nil, func(cfg map[string]any) (registry.Source, error) {
		id, _ := cfg["id"].(string)
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.sources[id]; ok {
			return s, nil
		}
		s := testkit.NewManualSource()
		s.Name = id
		h.sources[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.reg.RegisterSink("mem", 1, matrixSourceSchema, func(cfg map[string]any) (registry.Sink, error) {
		id, _ := cfg["id"].(string)
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.sinks[id]; ok {
			return s, nil
		}
		s := &matrixSink{id: id, fail: failSet[id]}
		h.sinks[id] = s
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	return h
}

// build compiles the scenario in the retaining lifecycle (explain dry-runs
// the script transforms) and closes it when the test ends.
func (h *matrixHarness) build(yamlText string) *ir.Pipeline {
	h.t.Helper()
	lr := config.LoadBytes("matrix.yaml", []byte(yamlText))
	if lr.Diagnostics.HasErrors() {
		h.t.Fatalf("config errors:\n%+v", lr.Diagnostics)
	}
	pip, diags := ir.BuildForExplain(lr.Pipeline, h.reg, starhost.DefaultOptions(), nil)
	if pip == nil {
		h.t.Fatalf("ir build errors:\n%+v", diags)
	}
	h.t.Cleanup(func() { _ = pip.Close() })
	return pip
}

// run executes one message through a real engine and reports the terminal
// facts plus the dead-letter class. Deterministic options (fixed clock,
// counter ids, millisecond backoffs) keep the matrix stable.
func (h *matrixHarness) run(pip *ir.Pipeline, raw string) (matrixOutcome, string) {
	h.t.Helper()
	opts := engine.DefaultOptions()
	opts.Clock = testkit.FixedClock(time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC))
	opts.NewID = testkit.CounterID()
	opts.BackoffBase = time.Millisecond
	opts.DLBackoff = time.Millisecond
	opts.BatchFlush = 10 * time.Millisecond
	opts.DefaultTimeout = 2 * time.Second

	st := store.NewMemory()
	eng, err := engine.New(pip, st, h.reg, opts)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			h.t.Error("engine did not stop")
		}
	}()
	for i := 0; i < 500 && !eng.Ready(); i++ {
		time.Sleep(2 * time.Millisecond)
	}
	if !eng.Ready() {
		h.t.Fatal("engine not ready")
	}

	entry := pip.Order[0]
	src := h.sources[entry]
	if src == nil {
		h.t.Fatalf("no manual source for entry %q", entry)
	}
	src.Emit([]byte(raw), "")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := eng.WaitCommit(waitCtx); err != nil {
		h.t.Fatalf("wait commit: %v", err)
	}

	out := matrixOutcome{reached: map[string]int{}}
	for id, sink := range h.sinks {
		writes, _ := sink.stats()
		if writes > 0 {
			out.reached[id] = writes
		}
	}
	class := ""
	dls, err := st.DeadLetters(pip.Config.Name)
	if err != nil {
		h.t.Fatal(err)
	}
	if len(dls) > 0 {
		out.deadLetter = dls[0].Node
		class = dls[0].Class
		if len(dls) > 1 {
			h.t.Errorf("multiple dead letters: %+v", dls)
		}
	}
	out.dropped = int(eng.Metrics.OptionalDrops.Load())
	out.filtered = int(eng.Metrics.NoMatch.Load())
	return out, class
}

var (
	matrixSplitLine      = regexp.MustCompile(`→ (\d+) messages \(children share the parent's identity; walking child #1\)`)
	matrixTransformLine  = regexp.MustCompile(`^(\S+): transform\.`)
	matrixDeadLetterLine = regexp.MustCompile(`would dead-letter`)
	matrixSinkLine       = regexp.MustCompile(`^  (\S+): sink `)
	matrixDropLine       = regexp.MustCompile(`exhausted → dropped \(required: false`)
	matrixFilteredLine   = regexp.MustCompile(`zero matching edges: the message commits as filtered`)
)

// predictFromTrace parses the walkthrough into the same outcome shape the
// engine observer reports. The trace walks child #1 of a split expansion, so
// every sink on the walked path is scaled by the expansion factor (children
// share the parent's identity and path).
func predictFromTrace(trace string) (matrixOutcome, int) {
	out := matrixOutcome{reached: map[string]int{}}
	factor := 1
	expand := 0
	lastTransform := ""
	for _, line := range strings.Split(trace, "\n") {
		if m := matrixSplitLine.FindStringSubmatch(line); m != nil {
			expand, _ = strconv.Atoi(m[1])
			factor = expand
			continue
		}
		if m := matrixTransformLine.FindStringSubmatch(line); m != nil {
			lastTransform = m[1]
			continue
		}
		if matrixDeadLetterLine.MatchString(line) {
			out.deadLetter = lastTransform
			continue
		}
		if m := matrixSinkLine.FindStringSubmatch(line); m != nil {
			out.reached[m[1]] += factor
			continue
		}
		if matrixDropLine.MatchString(line) {
			out.dropped++
			continue
		}
		if matrixFilteredLine.MatchString(line) {
			out.filtered++
		}
	}
	return out, expand
}

func TestExplainEngineComparisonMatrix(t *testing.T) {
	cases := []matrixCase{
		{
			name: "when-route",
			yaml: `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: matrix-route }
edge_defaults: { delivery: { retries: 0 } }
sources:
  in: { manual: { id: in } }
transforms:
  tag:
    depends_on: [in]
    script: |
      if payload.region == "eu":
          meta.route = "eu"
      else:
          meta.route = "us"
sinks:
  eu:
    depends_on: { tag: { route: eu } }
    mem: { id: eu }
  us:
    depends_on: { tag: { route: us } }
    mem: { id: us }
`,
			message:       `{"region": "eu"}`,
			want:          matrixOutcome{reached: map[string]int{"eu": 1}},
			wantDelivered: 1,
		},
		{
			name: "zero-match-filtering",
			yaml: `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: matrix-filter }
edge_defaults: { delivery: { retries: 0 } }
sources:
  in: { manual: { id: in } }
transforms:
  keep:
    depends_on: [in]
    script: "payload.seen = True"
sinks:
  out:
    depends_on: { keep: { when: 'payload.kind == "keep"' } }
    mem: { id: out }
`,
			message: `{"kind": "drop"}`,
			want:    matrixOutcome{reached: map[string]int{}, filtered: 1},
		},
		{
			name: "optional-drop",
			yaml: `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: matrix-drop }
edge_defaults: { delivery: { retries: 0 } }
sources:
  in: { manual: { id: in } }
sinks:
  out:
    depends_on: { in: { required: false } }
    mem: { id: out }
`,
			message:   `{"k": 1}`,
			want:      matrixOutcome{reached: map[string]int{"out": 1}, dropped: 1},
			failSinks: []string{"out"},
		},
		{
			name: "transform-dead-letter",
			yaml: `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: matrix-boom }
edge_defaults: { delivery: { retries: 0 } }
sources:
  in: { manual: { id: in } }
transforms:
  boom:
    depends_on: [in]
    script: |
      payload.x = 1
      fail("kaboom")
sinks:
  out: { depends_on: [boom], mem: { id: out } }
`,
			message:   `{"k": 1}`,
			want:      matrixOutcome{reached: map[string]int{}, deadLetter: "boom"},
			wantClass: "runtime",
		},
		{
			name: "split-expansion",
			yaml: `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: matrix-split }
edge_defaults: { delivery: { retries: 0 } }
sources:
  in: { manual: { id: in } }
transforms:
  splitter:
    depends_on: [in]
    split: {}
sinks:
  out: { depends_on: [splitter], mem: { id: out } }
`,
			message:       `[1, 2, 3]`,
			want:          matrixOutcome{reached: map[string]int{"out": 3}},
			wantDelivered: 3,
			wantExpand:    3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newMatrixHarness(t, tc.failSinks...)
			pip := h.build(tc.yaml)

			trace, err := Trace(pip, Options{Message: []byte(tc.message)})
			if err != nil && trace == "" {
				t.Fatalf("explain: %v", err)
			}
			predicted, expand := predictFromTrace(trace)
			if tc.wantExpand != 0 && expand != tc.wantExpand {
				t.Fatalf("trace expansion = %d, want %d:\n%s", expand, tc.wantExpand, trace)
			}

			observed, class := h.run(pip, tc.message)
			if !reflect.DeepEqual(predicted, tc.want) {
				t.Fatalf("trace prediction = %+v, want %+v:\n%s", predicted, tc.want, trace)
			}
			if !reflect.DeepEqual(observed, tc.want) {
				t.Fatalf("engine terminal state = %+v, want %+v:\n%s", observed, tc.want, trace)
			}
			if !reflect.DeepEqual(predicted, observed) {
				t.Fatalf("explain prediction %+v != engine terminal state %+v:\n%s", predicted, observed, trace)
			}
			if class != tc.wantClass {
				t.Fatalf("dead-letter class = %q, want %q", class, tc.wantClass)
			}
			if tc.wantDelivered > 0 {
				delivered := 0
				for _, s := range h.sinks {
					_, d := s.stats()
					delivered += d
				}
				if delivered != tc.wantDelivered {
					t.Fatalf("engine delivered %d messages, want %d", delivered, tc.wantDelivered)
				}
			}
		})
	}
}

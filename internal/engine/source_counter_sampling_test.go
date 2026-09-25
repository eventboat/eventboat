package engine

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// stepClock is a test clock the test can move forward deterministically
// (testkit.FixedClock cannot advance, and the sampling interval is
// time-based).
type stepClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *stepClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// counterManualSource is a ManualSource that also exposes the CounterSource
// facet with test-controllable values.
type counterManualSource struct {
	*testkit.ManualSource
	mu sync.Mutex
	c  map[string]int64
}

func (s *counterManualSource) Counters() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.c))
	for k, v := range s.c {
		out[k] = v
	}
	return out
}

func (s *counterManualSource) setCounter(name string, v int64) {
	s.mu.Lock()
	s.c[name] = v
	s.mu.Unlock()
}

// sourceCounterValue reads eventboat_source_<counter>_total from the
// exposition; an absent instrument reads as 0.
func sourceCounterValue(t *testing.T, o *obs.Obs, counter string) int64 {
	t.Helper()
	h := o.Handler()
	if h == nil {
		t.Fatal("no prometheus handler")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	prefix := "eventboat_source_" + counter + "_total{"
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return int64(v)
	}
	return 0
}

// The engine samples the registry.CounterSource facet on the commit path
// (rate-limited by Options.SourceCounterInterval) and writes positive deltas
// to Obs. No ops Service exists in this test: the `run --config` shape — an
// engine with telemetry and no admin/status surface — is exactly what it
// covers. The first sample is a baseline; a counter regression (source
// restart) resets the baseline without a negative delta.
func TestEngineSamplesSourceCountersWithoutOps(t *testing.T) {
	if got := DefaultOptions().SourceCounterInterval; got != time.Second {
		t.Fatalf("default SourceCounterInterval = %v, want 1s", got)
	}
	o, err := obs.Setup(context.Background(), obs.Config{Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()

	clk := &stepClock{t: time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)}
	h := newHarness(t)
	src := &counterManualSource{
		ManualSource: testkit.NewManualSource(),
		c:            map[string]int64{"lines_read": 3},
	}
	src.ManualSource.Name = "counted"
	if err := h.reg.RegisterSource("countmanual", 1, manualSchema, nil, func(map[string]any) (registry.Source, error) {
		return src, nil
	}); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: counter-sampling }
sources:
  counted:
    countmanual: { id: counted }
sinks:
  out:
    depends_on: [counted]
    encoder: json
    batch: { size: 100, timeout_ms: 10 }
    mem: { id: out }
`)
	st := store.NewMemory()
	opts := fastOptions()
	opts.Clock = clk.Now
	opts.Obs = o
	opts.SourceCounterInterval = time.Second
	eng, _ := runEngine(t, pip, st, h.reg, opts)

	// First commit: the starting value is a baseline, never a delta.
	src.Emit([]byte(`{"n":1}`), "")
	waitCommit(t, eng)
	if got := sourceCounterValue(t, o, "lines_read"); got != 0 {
		t.Fatalf("baseline wrote %d, want 0", got)
	}

	// Past the interval, a bump becomes a delta.
	clk.Advance(2 * time.Second)
	src.setCounter("lines_read", 11)
	src.Emit([]byte(`{"n":2}`), "")
	waitCommit(t, eng)
	if got := sourceCounterValue(t, o, "lines_read"); got != 8 {
		t.Fatalf("after 3->11 = %d, want 8", got)
	}

	// An unchanged counter writes nothing even when commits keep coming.
	clk.Advance(2 * time.Second)
	src.Emit([]byte(`{"n":3}`), "")
	waitCommit(t, eng)
	if got := sourceCounterValue(t, o, "lines_read"); got != 8 {
		t.Fatalf("unchanged counter moved to %d, want 8", got)
	}

	// A regression resets the baseline without a negative delta...
	clk.Advance(2 * time.Second)
	src.setCounter("lines_read", 2)
	src.Emit([]byte(`{"n":4}`), "")
	waitCommit(t, eng)
	if got := sourceCounterValue(t, o, "lines_read"); got != 8 {
		t.Fatalf("regression wrote a delta: %d, want 8", got)
	}
	// ... and the next increment counts from the reset value.
	clk.Advance(2 * time.Second)
	src.setCounter("lines_read", 4)
	src.Emit([]byte(`{"n":5}`), "")
	waitCommit(t, eng)
	if got := sourceCounterValue(t, o, "lines_read"); got != 10 {
		t.Fatalf("after reset 2->4 = %d, want 10", got)
	}
}

// The sampling interval gates the writes: a commit inside the window does not
// sample, so a busy pipeline cannot turn counter polling into per-message
// overhead.
func TestEngineSourceCounterSamplingIsRateLimited(t *testing.T) {
	o, err := obs.Setup(context.Background(), obs.Config{Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()

	clk := &stepClock{t: time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)}
	h := newHarness(t)
	src := &counterManualSource{
		ManualSource: testkit.NewManualSource(),
		c:            map[string]int64{"lines_read": 0},
	}
	src.ManualSource.Name = "counted"
	if err := h.reg.RegisterSource("countmanual", 1, manualSchema, nil, func(map[string]any) (registry.Source, error) {
		return src, nil
	}); err != nil {
		t.Fatal(err)
	}
	pip := h.build(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: counter-rate }
sources:
  counted:
    countmanual: { id: counted }
sinks:
  out:
    depends_on: [counted]
    encoder: json
    batch: { size: 100, timeout_ms: 10 }
    mem: { id: out }
`)
	opts := fastOptions()
	opts.Clock = clk.Now
	opts.Obs = o
	opts.SourceCounterInterval = time.Minute
	eng, _ := runEngine(t, pip, store.NewMemory(), h.reg, opts)

	src.Emit([]byte(`{"n":1}`), "")
	waitCommit(t, eng) // baseline sample at t0
	clk.Advance(time.Second)
	src.setCounter("lines_read", 5)
	src.Emit([]byte(`{"n":2}`), "")
	waitCommit(t, eng) // inside the 1m window: no sample
	if got := sourceCounterValue(t, o, "lines_read"); got != 0 {
		t.Fatalf("in-window commit wrote %d, want 0 (rate-limited)", got)
	}
}

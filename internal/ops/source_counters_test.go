package ops

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/eventboat/eventboat/internal/obs"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
)

// settableSource is a source with controllable CounterSource values.
type settableSource struct {
	mu sync.Mutex
	c  map[string]int64
}

func (s *settableSource) Init([]byte) error { return nil }
func (s *settableSource) Run(ctx context.Context, emit func(registry.Message) error) error {
	<-ctx.Done()
	return nil
}
func (s *settableSource) Commit(context.Context, int64) ([]byte, error) { return nil, nil }
func (s *settableSource) Close() error                                  { return nil }
func (s *settableSource) Counters() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.c))
	for k, v := range s.c {
		out[k] = v
	}
	return out
}
func (s *settableSource) set(counter string, v int64) {
	s.mu.Lock()
	s.c[counter] = v
	s.mu.Unlock()
}

// scrapeCounter reads the current value of eventboat_source_<counter>_total
// from the Prometheus exposition; an absent metric reads as 0 (the lazy
// instrument does not exist before the first delta is written — the exposure
// itself is asserted in the obs package's exposition test).
func scrapeCounter(t *testing.T, o *obs.Obs, counter string) int64 {
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

// Status writes source-counter DELTAS to telemetry: the first poll is the
// baseline, unchanged polls write nothing, and a regression (restart) resets
// the baseline without a negative delta.
func TestStatusWritesSourceCounterDeltas(t *testing.T) {
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	src := &settableSource{c: map[string]int64{"lines_read": 0}}
	schema := `{"type":"object","properties":{},"additionalProperties":false}`
	if err := reg.RegisterSource("countsrc", 1, schema, nil, func(map[string]any) (registry.Source, error) {
		return src, nil
	}); err != nil {
		t.Fatal(err)
	}

	o, err := obs.Setup(context.Background(), obs.Config{Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Shutdown(context.Background()) })

	owner := store.NewMemoryOwner()
	svc := New(Options{DataDir: t.TempDir(), Reg: reg, Stores: owner, Obs: o})
	t.Cleanup(svc.Stop)
	t.Cleanup(func() { _ = owner.Close() })

	cfg := `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: counter-pipeline }
sources:
  in:
    countsrc: {}
sinks:
  out:
    depends_on: [in]
    encoder: json
    batch: { size: 100, timeout_ms: 1000 }
    file: { path: out.jsonl }
`
	if _, err := svc.Deploy(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}

	// First observations are the baseline: no metric is written yet.
	svc.Status()
	if got := scrapeCounter(t, o, "lines_read"); got != 0 {
		t.Fatalf("baseline poll wrote %d, want 0", got)
	}

	src.set("lines_read", 10)
	svc.Status()
	if got := scrapeCounter(t, o, "lines_read"); got != 10 {
		t.Fatalf("after 0->10 = %d, want 10", got)
	}
	// An unchanged poll writes no delta.
	svc.Status()
	if got := scrapeCounter(t, o, "lines_read"); got != 10 {
		t.Fatalf("unchanged poll moved the counter to %d, want 10", got)
	}
	// A regression (source restart) resets the baseline without a negative
	// delta; the next increment is counted from the reset value.
	src.set("lines_read", 4)
	svc.Status()
	if got := scrapeCounter(t, o, "lines_read"); got != 10 {
		t.Fatalf("regression wrote a delta: %d, want 10", got)
	}
	src.set("lines_read", 6)
	svc.Status()
	if got := scrapeCounter(t, o, "lines_read"); got != 12 {
		t.Fatalf("after reset 4->6 = %d, want 12", got)
	}
}

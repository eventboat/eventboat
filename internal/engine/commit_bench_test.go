package engine

import (
	"context"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// BenchmarkCommitThroughput measures end-to-end accept→commit throughput:
// parallel emitters through a manual source, a four-way fan-out (four sink
// workers committing concurrently), then a full WaitCommit barrier inside the
// timed region. Two variants: an in-memory store (pure bookkeeping cost) and
// a simulated-fsync store (SetCheckpoint sleeps 100µs — the regime the
// out-of-lock persistence refactor targets: while one commit flushes, the
// tracker must stay available to arrivals and other commits).
func BenchmarkCommitThroughput(b *testing.B) {
	const benchYAML = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: bench }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in:
    decoder: json
    manual: { id: in }
sinks:
  out1: { from: [in], mem: { id: b1 } }
  out2: { from: [in], mem: { id: b2 } }
  out3: { from: [in], mem: { id: b3 } }
  out4: { from: [in], mem: { id: b4 } }
`
	for _, tc := range []struct {
		name string
		fsyn bool
	}{
		{name: "mem", fsyn: false},
		{name: "fsync_sim", fsyn: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			h := newHarness(b)
			pip := h.build(benchYAML)
			st := store.NewMemory()
			if tc.fsyn {
				wrapped := &testkit.StoreWrapper{Inner: st}
				wrapped.SetCheckpointHook = func(seq int64) error {
					time.Sleep(100 * time.Microsecond)
					return nil
				}
				st = wrapped
			}
			opts := fastOptions()
			opts.HighWatermark = 1024
			opts.BatchFlush = time.Millisecond
			eng, stop := runEngine(b, pip, st, h.reg, opts)
			defer stop()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var n int64
				for pb.Next() {
					n++
					h.source("in").Emit([]byte(`{"i":1}`), "")
				}
				_ = n
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := eng.WaitCommit(ctx); err != nil {
				b.Fatal(err)
			}
			if got := eng.Metrics.CommittedCount.Load(); got != int64(b.N) {
				b.Fatalf("committed %d, want %d", got, b.N)
			}
			b.StopTimer()
		})
	}
}

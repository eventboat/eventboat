package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
)

// sqliteBenchYAML is the BenchmarkCommitThroughput pipeline shape (manual
// sources, four-way fan-out, parallel emit callers) with an explicit sink
// batch size. The durable end-to-end gate of the log-collection plan (§2.2,
// P1) is measured against a real SQLite store.
//
// Three source shapes:
//
//   - one_source: one manual source, exactly like BenchmarkCommitThroughput.
//     Its Run loop admits emissions one at a time, so the store sees strictly
//     serial appends — the single-file tailer shape, where a group of one is
//     the best the writer can ever build.
//   - sources4: four manual sources admitting in parallel — a small host.
//   - sources16: sixteen manual sources admitting in parallel — the shape the
//     collection plan actually targets (a host with many log files, each read
//     concurrently). This is where group commit fills a transaction.
const sqliteBenchYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: bench }
edge_defaults:
  delivery: { retries: 0, backoff: constant }
sources:
  in1: { decoder: json, manual: { id: in1 } }
  in2: { decoder: json, manual: { id: in2 } }
  in3: { decoder: json, manual: { id: in3 } }
  in4: { decoder: json, manual: { id: in4 } }
  in5: { decoder: json, manual: { id: in5 } }
  in6: { decoder: json, manual: { id: in6 } }
  in7: { decoder: json, manual: { id: in7 } }
  in8: { decoder: json, manual: { id: in8 } }
  in9: { decoder: json, manual: { id: in9 } }
  in10: { decoder: json, manual: { id: in10 } }
  in11: { decoder: json, manual: { id: in11 } }
  in12: { decoder: json, manual: { id: in12 } }
  in13: { decoder: json, manual: { id: in13 } }
  in14: { decoder: json, manual: { id: in14 } }
  in15: { decoder: json, manual: { id: in15 } }
  in16: { decoder: json, manual: { id: in16 } }
sinks:
  out1: { depends_on: [in1, in2, in3, in4, in5, in6, in7, in8, in9, in10, in11, in12, in13, in14, in15, in16], mem: { id: b1 }, batch: { size: %[1]d, timeout_ms: 5 } }
  out2: { depends_on: [in1, in2, in3, in4, in5, in6, in7, in8, in9, in10, in11, in12, in13, in14, in15, in16], mem: { id: b2 }, batch: { size: %[1]d, timeout_ms: 5 } }
  out3: { depends_on: [in1, in2, in3, in4, in5, in6, in7, in8, in9, in10, in11, in12, in13, in14, in15, in16], mem: { id: b3 }, batch: { size: %[1]d, timeout_ms: 5 } }
  out4: { depends_on: [in1, in2, in3, in4, in5, in6, in7, in8, in9, in10, in11, in12, in13, in14, in15, in16], mem: { id: b4 }, batch: { size: %[1]d, timeout_ms: 5 } }
`

// BenchmarkCommitThroughputSQLite measures accept→commit throughput against a
// real SQLite store: every accepted message is spooled durably before it
// becomes visible, and every sink branch commits through the same store.
func BenchmarkCommitThroughputSQLite(b *testing.B) {
	for _, shape := range []struct {
		name    string
		sources int
	}{
		{name: "one_source", sources: 1},
		{name: "sources4", sources: 4},
		{name: "sources16", sources: 16},
	} {
		for _, batchSize := range []int{1, 100} {
			b.Run(shape.name+"/batch="+strconv.Itoa(batchSize), func(b *testing.B) {
				h := newHarness(b)
				pip := h.build(fmt.Sprintf(sqliteBenchYAML, batchSize))
				// A fresh database per invocation: b.TempDir() is shared
				// across the framework's calibration rounds, and every row an
				// earlier round left behind would slow the measured one.
				dir, err := os.MkdirTemp("", "eventboat-sqlite-bench-")
				if err != nil {
					b.Fatal(err)
				}
				st, err := store.OpenSQLite(filepath.Join(dir, "bench.db"))
				if err != nil {
					_ = os.RemoveAll(dir)
					b.Fatal(err)
				}
				// Close per invocation — not b.Cleanup, which would hold every
				// calibration round's store (writer goroutine, connection,
				// page cache, temp dir) open until the whole benchmark ends
				// and slow the measured round down.
				defer func() {
					_ = st.Close()
					_ = os.RemoveAll(dir)
				}()

				opts := fastOptions()
				opts.HighWatermark = 4096
				opts.BatchFlush = 5 * time.Millisecond
				eng, stop := runEngine(b, pip, st, h.reg, opts)
				defer stop()

				var next atomic.Int64
				emit := func() {
					src := "in1"
					if shape.sources > 1 {
						src = "in" + strconv.Itoa(int(next.Add(1))%shape.sources+1)
					}
					h.source(src).Emit([]byte(`{"i":1}`), "")
				}

				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						emit()
					}
				})
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				if err := eng.WaitCommit(ctx); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				// WaitCommit observes the commit tracker, while CommittedCount
				// is incremented by the commit callback just after the tracker
				// reports quiescence: settling the count is an assertion
				// detail, so it stays outside the timed region.
				deadline := time.Now().Add(5 * time.Second)
				for eng.Metrics.CommittedCount.Load() != int64(b.N) && time.Now().Before(deadline) {
					time.Sleep(100 * time.Microsecond)
				}
				if got := eng.Metrics.CommittedCount.Load(); got != int64(b.N) {
					b.Fatalf("committed %d, want %d (in=%d dlq=%d srcErr=%v)", got, b.N,
						eng.Metrics.MessagesIn.Load(), eng.Metrics.DeadLettered.Load(), eng.SourceErrors())
				}
			})
		}
	}
}

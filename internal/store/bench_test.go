package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// appendBenchMessage is the row shape the engine writes on every admission.
var appendBenchMessage = registry.Message{
	ID:     "m-000001",
	Codec:  "json",
	Raw:    []byte(`{"level":"info","msg":"request served","n":1}`),
	Meta:   map[string]any{"source": "in", "ingest_time": "2026-09-24T12:00:00Z"},
	Cursor: "c-1",
}

// BenchmarkAppendSpool measures the durable append path in isolation, before
// the engine is involved (design §1.2 "Store.AppendSpool (real SQLite)":
// per-message durable write is the ceiling the group-commit work targets).
//
//   - sqlite: parallel callers, the shape the engine's source goroutines
//     produce when several pull concurrently.
//   - sqlite_serial: one caller at a time — the single file-source shape,
//     where group commit cannot borrow concurrency from callers.
//   - memory: the bookkeeping floor (no durability).
func BenchmarkAppendSpool(b *testing.B) {
	for _, tc := range []struct {
		name     string
		sqlite   bool
		parallel bool
	}{
		{name: "sqlite", sqlite: true, parallel: true},
		{name: "sqlite_serial", sqlite: true, parallel: false},
		{name: "memory", sqlite: false, parallel: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var st Store
			if tc.sqlite {
				// A fresh database per invocation: b.TempDir() is shared
				// across the framework's calibration rounds, and every row an
				// earlier round left behind would slow the measured one.
				dir, err := os.MkdirTemp("", "eventboat-store-bench-")
				if err != nil {
					b.Fatal(err)
				}
				s, err := OpenSQLite(filepath.Join(dir, "append.db"))
				if err != nil {
					_ = os.RemoveAll(dir)
					b.Fatal(err)
				}
				// Close per invocation (not b.Cleanup, which holds every
				// calibration round open until the benchmark ends).
				defer func() {
					_ = s.Close()
					_ = os.RemoveAll(dir)
				}()
				st = s
			} else {
				st = NewMemory()
				b.Cleanup(func() { _ = st.Close() })
			}

			msg := appendBenchMessage
			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			if tc.parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := st.AppendSpool("bench", msg, now); err != nil {
							b.Error(err)
							return
						}
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					if _, err := st.AppendSpool("bench", msg, now); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
		})
	}
}

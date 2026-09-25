package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
)

// BenchmarkCollectE2E is the end-to-end collection benchmark of the
// log-collection program (design 2026-09-24 §2.6.3, P4): a real file source
// tails a temp file through the real engine and the group-commit SQLite spool
// into the real victorialogs sink, whose endpoint is a process-local httptest
// server answering 200 and counting the rows it receives. No VictoriaLogs
// instance is needed — the sink's wire shape (one POST per batch, JSONL body)
// is exercised, only the server's ingest behavior is stubbed (it validates
// nothing, so VL's "200 while skipping bad lines" trap is out of scope here;
// the sink unit suite covers the body).
//
// Environment (see scripts/bench-collect.sh for the reference shapes):
//
//	EVENTBOAT_BENCH_LINE_BYTES  JSON bytes per line, newline excluded (default 512)
//	EVENTBOAT_BENCH_LINES       lines per run (default 20000)
//	EVENTBOAT_BENCH_RATE        producer lines/s; 0 = write as fast as the OS takes (default 0)
//
// One invocation of the benchmark function measures ONE full collection: the
// producer appends the lines, the source reads them, the sink POSTs batches of
// 500 and the run ends when the last Write returned 200 (a counting sink
// wrapper signals completion — no polling skew in the timed region). Run it
// with -benchtime 1x (scripts/bench-collect.sh does) so ns/op is one run; the
// headline metric is lines/s and b.SetBytes reports MB/s.
//
// Measurement discipline (S2 finding, scripts/bench-gate.sh): a shape's
// number depends on what ran in the same process before it, so every shape
// must run through its own `go test` invocation. The script does that; the
// numbers are a machine-local order-of-magnitude reference, not a promise.
//
// The engine runs with production defaults: 10 000 max_in_flight,
// storage.write_batch 256 rows / 2 ms (store.OpenSQLite defaults), sink batch
// 500 / timeout_ms 100. The payload is JSON (decoder json -> encoder json),
// host/app fields present so the sink's stream_fields are meaningful. No
// transform: the numbers measure collection, not transformation.
const (
	collectBenchBatchSize  = 500
	collectBenchBatchMS    = 100
	collectBenchChunksPerS = 10 // rate-limited producer: chunk per 100 ms
	collectBenchMinLine    = 128
)

// collectBenchConfig is one benchmark shape.
type collectBenchConfig struct {
	lineBytes int
	lines     int
	rate      int
}

// collectBenchEnv is one fully wired collection run: file, store, engine,
// sink tap and the httptest server standing in for VictoriaLogs.
type collectBenchEnv struct {
	cfg  collectBenchConfig
	path string
	st   *store.SQLite
	eng  *Engine

	cancel  context.CancelFunc
	runDone chan error

	srv      *httptest.Server
	requests atomic.Int64
	rows     atomic.Int64

	// deliveredRows counts what the sink wrapper saw handed to the real sink;
	// delivered is closed on the run's last successful batch write.
	deliveredRows atomic.Int64
	delivered     chan struct{}
	tap           *collectBenchTap
}

// collectBenchTap is a transparent sink wrapper that signals when the real
// sink has accepted the configured number of rows. It only counts; Write and
// Close delegate to the wrapped sink unchanged.
type collectBenchTap struct {
	inner  registry.Sink
	target int64
	total  *atomic.Int64
	done   chan struct{}
	once   atomic.Bool
}

func (t *collectBenchTap) Write(ctx context.Context, msgs []registry.Message) error {
	if err := t.inner.Write(ctx, msgs); err != nil {
		return err
	}
	if n := t.total.Add(int64(len(msgs))); n >= t.target && t.once.CompareAndSwap(false, true) {
		close(t.done)
	}
	return nil
}

func (t *collectBenchTap) Close() error { return t.inner.Close() }

// collectBenchYAML is the collection shape under test: a real file source on a
// temp file (poll_every_ms 10 so discovery adds no artificial latency), the
// group-commit SQLite spool through engine defaults, and the victorialogs
// sink in its reference batch shape.
const collectBenchYAML = `
apiVersion: eventboat/v1
kind: Pipeline
metadata: { name: bench-collect }
sources:
  logs:
    decoder: json
    file:
      path: %q
      poll_every_ms: 10
      start_at: beginning
      on_eof: tail
sinks:
  vl:
    depends_on: [logs]
    encoder: json
    batch: { size: %d, timeout_ms: %d }
    victorialogs:
      url: %q
      stream_fields: host,app
`

// The generated line's JSON envelope; the msg field is padded with 'a' so the
// finished line (newline excluded) is exactly lineBytes.
const (
	collectBenchHead = `{"ts":"2026-09-25T00:00:00Z","host":"bench-host","app":"bench-app","seq":`
	collectBenchMid  = `,"msg":"`
	collectBenchTail = `"}`
)

// collectBenchEnvelope is the smallest sensible line for the configured line
// count: the JSON envelope plus one message byte.
func collectBenchEnvelope(lines int) int {
	return len(collectBenchHead) + len(strconv.Itoa(lines-1)) + len(collectBenchMid) + len(collectBenchTail) + 1
}

// appendCollectLine appends one JSON line of exactly lineBytes bytes
// (newline excluded).
func appendCollectLine(dst []byte, lineBytes, seq int) []byte {
	start := len(dst)
	dst = append(dst, collectBenchHead...)
	dst = strconv.AppendInt(dst, int64(seq), 10)
	dst = append(dst, collectBenchMid...)
	for pad := lineBytes - (len(dst) - start) - len(collectBenchTail); pad > 0; pad-- {
		dst = append(dst, 'a')
	}
	return append(dst, collectBenchTail...)
}

func envBenchInt(b *testing.B, name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		b.Fatalf("%s=%q is not an integer: %v", name, raw, err)
	}
	return v
}

// BenchmarkCollectE2E measures one full file-to-sink collection per
// invocation. See the file comment for the environment contract.
func BenchmarkCollectE2E(b *testing.B) {
	cfg := collectBenchConfig{
		lineBytes: envBenchInt(b, "EVENTBOAT_BENCH_LINE_BYTES", 512),
		lines:     envBenchInt(b, "EVENTBOAT_BENCH_LINES", 20000),
		rate:      envBenchInt(b, "EVENTBOAT_BENCH_RATE", 0),
	}
	if cfg.lines < 1 {
		b.Fatalf("EVENTBOAT_BENCH_LINES=%d must be >= 1", cfg.lines)
	}
	if cfg.rate < 0 {
		b.Fatalf("EVENTBOAT_BENCH_RATE=%d must be >= 0 (0 = unthrottled)", cfg.rate)
	}
	if cfg.lineBytes < collectBenchMinLine || cfg.lineBytes < collectBenchEnvelope(cfg.lines) {
		b.Fatalf("EVENTBOAT_BENCH_LINE_BYTES=%d is too small: the JSON envelope needs >= %d bytes (lines=%d)",
			cfg.lineBytes, max(collectBenchMinLine, collectBenchEnvelope(cfg.lines)), cfg.lines)
	}
	// One "op" is one whole collection, so SetBytes carries the run's input
	// size and go test's MB/s column is the true ingest throughput.
	b.SetBytes(int64(cfg.lines) * int64(cfg.lineBytes))

	var total time.Duration
	var requests, rows int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		env := newCollectBenchEnv(b, cfg, i)
		b.StartTimer()
		total += env.collect(b)
		b.StopTimer()
		env.assert(b)
		requests += env.requests.Load()
		rows += env.rows.Load()
		env.closeEnv(b)
	}
	if total <= 0 {
		b.Fatal("no measured run")
	}
	b.ReportMetric(float64(cfg.lines)*float64(b.N)/total.Seconds(), "lines/s")
	if cfg.rate > 0 {
		b.ReportMetric(float64(cfg.rate), "target-lines/s")
	}
	if requests > 0 {
		b.ReportMetric(float64(rows)/float64(requests), "lines/request")
	}
}

// newCollectBenchEnv wires one run: empty temp file, SQLite store, the
// engine with a counting sink wrapper, and the httptest VictoriaLogs stand-in.
func newCollectBenchEnv(b *testing.B, cfg collectBenchConfig, run int) *collectBenchEnv {
	b.Helper()
	env := &collectBenchEnv{cfg: cfg, delivered: make(chan struct{})}
	dir := filepath.Join(b.TempDir(), fmt.Sprintf("run%d", run))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("bench dir: %v", err)
	}
	env.path = filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(env.path, nil, 0o644); err != nil {
		b.Fatalf("bench file: %v", err)
	}

	env.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.requests.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		env.rows.Add(int64(bytes.Count(body, []byte{'\n'})))
		w.WriteHeader(http.StatusOK)
	}))

	st, err := store.OpenSQLite(filepath.Join(dir, "bench.db"))
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	env.st = st

	h := newHarness(b)
	pip := h.build(fmt.Sprintf(collectBenchYAML,
		filepath.ToSlash(env.path), collectBenchBatchSize, collectBenchBatchMS, env.srv.URL))
	opts := DefaultOptions()
	opts.SinkWrapper = func(_ string, inner registry.Sink) registry.Sink {
		env.tap = &collectBenchTap{inner: inner, target: int64(cfg.lines), total: &env.deliveredRows, done: env.delivered}
		return env.tap
	}
	eng, err := New(pip, st, h.reg, opts)
	if err != nil {
		b.Fatalf("engine: %v", err)
	}
	env.eng = eng
	if env.tap == nil {
		b.Fatal("sink wrapper never wrapped the victorialogs sink")
	}
	ctx, cancel := context.WithCancel(context.Background())
	env.cancel = cancel
	env.runDone = make(chan error, 1)
	go func() { env.runDone <- eng.Run(ctx) }()
	waitReady(b, eng)
	return env
}

// collect runs the timed region: the producer appends, the sink tap signals
// the last accepted batch. A failed producer, a stopped engine or a budget
// overrun aborts the benchmark.
func (env *collectBenchEnv) collect(b *testing.B) time.Duration {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	writeDone := make(chan error, 1)
	go func() { writeDone <- writeCollectLines(ctx, env.path, env.cfg) }()

	budget := collectBenchBudget(env.cfg)
	timeout := time.NewTimer(budget)
	defer timeout.Stop()
	writerDone := false
	for {
		select {
		case <-env.delivered:
			if !writerDone {
				if err := <-writeDone; err != nil {
					b.Fatalf("bench producer: %v", err)
				}
			}
			return time.Since(start)
		case err := <-writeDone:
			if err != nil {
				b.Fatalf("bench producer: %v", err)
			}
			writerDone = true
			writeDone = nil // a second receive would block forever
		case err := <-env.runDone:
			b.Fatalf("engine stopped before the run completed: %v", err)
		case <-timeout.C:
			b.Fatalf("collection did not deliver %d lines within %s (delivered=%d rows, %d requests, %d uncommitted)",
				env.cfg.lines, budget, env.deliveredRows.Load(), env.requests.Load(), env.cfg.lines-int(env.eng.Metrics.CommittedCount.Load()))
		}
	}
}

// writeCollectLines appends the run's lines, in one write when unthrottled or
// in rate/10-line chunks every ~100 ms otherwise (the producer models an app
// writing to the file; the collector must keep up). Rates below 10 lines/s
// quantize to one line per 100 ms.
func writeCollectLines(ctx context.Context, path string, cfg collectBenchConfig) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	chunk, interval := cfg.lines, time.Duration(0)
	if cfg.rate > 0 {
		chunk = cfg.rate / collectBenchChunksPerS
		if chunk < 1 {
			chunk = 1
		}
		interval = time.Duration(float64(time.Second) * float64(chunk) / float64(cfg.rate))
	}
	buf := make([]byte, 0, chunk*(cfg.lineBytes+1))
	for seq := 0; seq < cfg.lines; {
		n := min(cfg.lines-seq, chunk)
		buf = buf[:0]
		for i := 0; i < n; i++ {
			buf = appendCollectLine(buf, cfg.lineBytes, seq+i)
			buf = append(buf, '\n')
		}
		if _, err := f.Write(buf); err != nil {
			_ = f.Close()
			return err
		}
		seq += n
		if seq < cfg.lines && interval > 0 {
			select {
			case <-ctx.Done():
				_ = f.Close()
				return ctx.Err()
			case <-time.After(interval):
			}
		}
	}
	return f.Close()
}

// collectBenchBudget bounds one run generously: at least two minutes, or ten
// times the producer's own duration when rate-limited.
func collectBenchBudget(cfg collectBenchConfig) time.Duration {
	budget := 2 * time.Minute
	if cfg.rate > 0 {
		if scaled := time.Duration(float64(time.Second) * float64(cfg.lines) / float64(cfg.rate) * 10); scaled > budget {
			budget = scaled
		}
	}
	return budget
}

// assert settles the commit accounting outside the timed region and fails the
// benchmark on any lost, skipped or dead-lettered line: a silent collection
// failure must not read as a fast run.
func (env *collectBenchEnv) assert(b *testing.B) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for env.eng.Metrics.CommittedCount.Load() < int64(env.cfg.lines) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := env.eng.Metrics.CommittedCount.Load(); got != int64(env.cfg.lines) {
		b.Fatalf("committed %d, want %d", got, env.cfg.lines)
	}
	if got := env.eng.Metrics.MessagesIn.Load(); got != int64(env.cfg.lines) {
		b.Fatalf("accepted %d, want %d", got, env.cfg.lines)
	}
	if got := env.eng.Metrics.DeadLettered.Load(); got != 0 {
		b.Fatalf("dead-lettered %d lines during the benchmark run", got)
	}
	if errs := env.eng.SourceErrors(); len(errs) > 0 {
		b.Fatalf("source errors: %v", errs)
	}
	if got := env.rows.Load(); got != int64(env.cfg.lines) {
		b.Fatalf("httptest server received %d lines, want %d", got, env.cfg.lines)
	}
	if got := env.deliveredRows.Load(); got != int64(env.cfg.lines) {
		b.Fatalf("sink accepted %d lines, want %d", got, env.cfg.lines)
	}
}

// closeEnv stops the engine (everything is committed by now) and releases the
// store and the server.
func (env *collectBenchEnv) closeEnv(b *testing.B) {
	b.Helper()
	env.cancel()
	select {
	case <-env.runDone:
	case <-time.After(10 * time.Second):
		b.Error("engine did not stop within 10s after the run")
	}
	if err := env.st.Close(); err != nil {
		b.Errorf("store close: %v", err)
	}
	env.srv.Close()
}

package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// openGroupCommitStore opens a real SQLite store with explicit batch bounds
// (the Runtime knobs are exercised through OpenSQLiteWithOptions).
func openGroupCommitStore(t *testing.T, rows int, wait time.Duration) *SQLite {
	t.Helper()
	st, err := OpenSQLiteWithOptions(filepath.Join(t.TempDir(), "writer.db"), WriteOptions{MaxRows: rows, MaxWait: wait})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func writerTestMessage(id string) registry.Message {
	return registry.Message{ID: id, Codec: "json", Raw: []byte(`{"id":"` + id + `"}`), Meta: map[string]any{"m": id}}
}

func spoolCount(t *testing.T, st *SQLite) int {
	t.Helper()
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM spool`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func writerQueueLen(w *writer) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.queue)
}

// waitQueueLen blocks until the writer's pending queue holds at least want
// requests. It is the deterministic synchronization the batching tests need:
// a writer parked in a test hook cannot drain, so the queue only grows.
func waitQueueLen(t *testing.T, w *writer, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if writerQueueLen(w) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("writer queue never reached %d (stuck at %d)", want, writerQueueLen(w))
}

// TestAppendSpoolGroupCommitSeqContiguous: concurrent appends through the
// group-commit writer assign each row exactly one seq, the seqs form the
// contiguous 1..N range (multi-row INSERT + last_insert_rowid arithmetic),
// each caller sees its own queue-order seq, and every row replays back with
// its own message.
func TestAppendSpoolGroupCommitSeqContiguous(t *testing.T) {
	const workers, per = 8, 25
	st := openGroupCommitStore(t, 4, time.Millisecond)
	now := time.Now()

	var mu sync.Mutex
	byID := map[string]int64{}
	bySeq := map[int64]string{}
	perWorker := make([][]int64, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := fmt.Sprintf("m-%d-%d", w, i)
				seq, err := st.AppendSpool("p", writerTestMessage(id), now)
				if err != nil {
					t.Errorf("worker %d append %d: %v", w, i, err)
					return
				}
				mu.Lock()
				perWorker[w] = append(perWorker[w], seq)
				if prev, dup := bySeq[seq]; dup {
					t.Errorf("seq %d handed out twice (%s and %s)", seq, prev, id)
				}
				bySeq[seq] = id
				byID[id] = seq
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(bySeq) != workers*per {
		t.Fatalf("distinct seqs = %d, want %d", len(bySeq), workers*per)
	}
	for seq := int64(1); seq <= int64(workers*per); seq++ {
		if _, ok := bySeq[seq]; !ok {
			t.Fatalf("seq %d missing: the group-commit range is not contiguous", seq)
		}
	}
	for w, list := range perWorker {
		for i := 1; i < len(list); i++ {
			if list[i] <= list[i-1] {
				t.Errorf("worker %d seqs not in queue order: %v", w, list)
			}
		}
	}

	got := map[int64]string{}
	if err := st.ReplayFrom("p", 0, func(seq int64, m registry.Message, _ time.Time) error {
		got[seq] = m.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, bySeq) {
		t.Fatalf("replayed seq->id differs from the returned pairs")
	}
	for id, seq := range byID {
		if got[seq] != id {
			t.Fatalf("row %d is %q, want %q", seq, got[seq], id)
		}
	}
}

// TestGroupCommitBatchesQueuedAppends: requests that pile up behind an
// in-flight transaction are committed by one multi-row INSERT, not one
// transaction each. The hook parks the writer before its first commit so the
// test can queue a known number of appends deterministically.
func TestGroupCommitBatchesQueuedAppends(t *testing.T) {
	st := openGroupCommitStore(t, 1024, time.Second)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var mu sync.Mutex
	var sizes []int
	hook := func(appends int) error {
		mu.Lock()
		sizes = append(sizes, appends)
		mu.Unlock()
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	st.w.testBatchHook.Store(&hook)

	now := time.Now()
	first := make(chan error, 1)
	go func() {
		_, err := st.AppendSpool("p", writerTestMessage("first"), now)
		first <- err
	}()
	<-started // the writer is parked before its first (single-request) commit

	const n = 32
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = st.AppendSpool("p", writerTestMessage(fmt.Sprintf("m-%d", i)), now)
		}(i)
	}
	waitQueueLen(t, st.w, n)
	close(release)
	wg.Wait()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("queued append %d: %v", i, err)
		}
	}

	mu.Lock()
	got := append([]int(nil), sizes...)
	mu.Unlock()
	if len(got) < 2 || got[1] != n {
		t.Fatalf("transaction sizes = %v, want [1 %d ...]: queued requests must share one group commit", got, n)
	}
	if rows := spoolCount(t, st); rows != n+1 {
		t.Fatalf("spool rows = %d, want %d", rows, n+1)
	}
}

// TestGroupCommitMaxRowsCapsBatch: max_rows is a hard cap on one transaction;
// a larger pile-up spills into several transactions without losing a row or
// breaking seq order.
func TestGroupCommitMaxRowsCapsBatch(t *testing.T) {
	const maxRows = 4
	st := openGroupCommitStore(t, maxRows, time.Second)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var mu sync.Mutex
	var sizes []int
	hook := func(appends int) error {
		mu.Lock()
		sizes = append(sizes, appends)
		mu.Unlock()
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	st.w.testBatchHook.Store(&hook)

	now := time.Now()
	go func() { _, _ = st.AppendSpool("p", writerTestMessage("first"), now) }()
	<-started

	const n = 10
	seqs := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seqs[i], _ = st.AppendSpool("p", writerTestMessage(fmt.Sprintf("m-%d", i)), now)
		}(i)
	}
	waitQueueLen(t, st.w, n)
	close(release)
	wg.Wait()

	mu.Lock()
	got := append([]int(nil), sizes...)
	mu.Unlock()
	// The first group holds the lone first request; the pile-up spills 4+4+2.
	want := []int{1, 4, 4, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transaction sizes = %v, want %v", got, want)
	}
	seen := map[int64]bool{}
	for _, seq := range seqs {
		seen[seq] = true
	}
	for seq := int64(2); seq <= n+1; seq++ {
		if !seen[seq] {
			t.Fatalf("seq %d missing from the spill batches", seq)
		}
	}
}

// TestGroupCommitFailureFailsWholeBatch: a failing group-commit transaction
// refuses every waiter it carried (the refusal contract: nothing durable, the
// source re-emits), and leaves no row behind.
func TestGroupCommitFailureFailsWholeBatch(t *testing.T) {
	st := openGroupCommitStore(t, 16, time.Second)
	injected := errors.New("injected batch failure")
	hook := func(int) error { return injected }
	st.w.testBatchHook.Store(&hook)

	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = st.AppendSpool("p", writerTestMessage(fmt.Sprintf("m-%d", i)), time.Now())
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, injected) {
			t.Fatalf("waiter %d: err = %v, want the batch failure", i, err)
		}
	}
	if rows := spoolCount(t, st); rows != 0 {
		t.Fatalf("failed batches left %d rows behind", rows)
	}
	// A coalesced non-append write riding a failing group is refused too.
	if err := st.SetCheckpoint("p", 5); !errors.Is(err, injected) {
		t.Fatalf("checkpoint in a failing group: err = %v, want the batch failure", err)
	}
	// The store is intact: a later success commits normally.
	st.w.testBatchHook.Store(nil)
	if _, err := st.AppendSpool("p", writerTestMessage("after"), time.Now()); err != nil {
		t.Fatalf("append after a failed batch: %v", err)
	}
	if rows := spoolCount(t, st); rows != 1 {
		t.Fatalf("spool rows after recovery = %d, want 1", rows)
	}
}

// TestCloseRefusesInFlightAppends: Close must never leave a writer caller
// blocked. Requests queued when Close begins are refused immediately; a
// transaction already open is rolled back and refused too; the writer
// goroutine exits.
func TestCloseRefusesInFlightAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close.db")
	st, err := OpenSQLiteWithOptions(path, WriteOptions{MaxRows: 1024, MaxWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	hook := func(int) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	st.w.testBatchHook.Store(&hook)

	now := time.Now()
	inflight := make(chan error, 1)
	go func() {
		_, err := st.AppendSpool("p", writerTestMessage("inflight"), now)
		inflight <- err
	}()
	<-started // the writer is inside the first transaction

	const n = 3
	queued := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, queued[i] = st.AppendSpool("p", writerTestMessage(fmt.Sprintf("q-%d", i)), now)
		}(i)
	}
	waitQueueLen(t, st.w, n)

	baseline := runtime.NumGoroutine()
	closed := make(chan error, 1)
	go func() { closed <- st.Close() }()

	// Queued waiters unblock even though the writer is still parked.
	wg.Wait()
	for i, err := range queued {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("queued append %d: err = %v, want ErrClosed", i, err)
		}
	}
	// The parked transaction is refused rather than committed.
	close(release)
	if err := <-inflight; !errors.Is(err, ErrClosed) {
		t.Fatalf("in-flight append: err = %v, want ErrClosed", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// The writer goroutine must be gone once Close returned.
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d live, baseline %d", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The refused transactions rolled back: reopening the file shows no rows.
	reopened, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen after Close: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if rows := spoolCount(t, reopened); rows != 0 {
		t.Fatalf("closed store kept %d rows", rows)
	}
}

// TestCheckpointAndSourceStateConcurrentMonotonic: concurrent same-key writes
// coalesce to the maximum within a group, and the store's high-water marks
// keep later groups from regressing an earlier value. The source state stays a
// (state, srcSeq) pair: the surviving row is the one with the highest frontier.
func TestCheckpointAndSourceStateConcurrentMonotonic(t *testing.T) {
	st := openGroupCommitStore(t, 1024, time.Millisecond)
	const n = 40
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if err := st.SetCheckpoint("p", int64(i)); err != nil {
				t.Errorf("checkpoint %d: %v", i, err)
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			state := []byte(fmt.Sprintf(`{"w":%d}`, i))
			if err := st.SetSourceState("p", "in", state, int64(i)); err != nil {
				t.Errorf("source state %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	cp, err := st.Checkpoint("p")
	if err != nil {
		t.Fatal(err)
	}
	if cp != n {
		t.Fatalf("checkpoint = %d, want %d (max of the concurrent submits)", cp, n)
	}
	state, srcSeq, err := st.SourceState("p", "in")
	if err != nil {
		t.Fatal(err)
	}
	if srcSeq != n || string(state) != fmt.Sprintf(`{"w":%d}`, n) {
		t.Fatalf("source state = %s/%d, want the pair for frontier %d", state, srcSeq, n)
	}
	if cpOther, _ := st.Checkpoint("other"); cpOther != 0 {
		t.Fatalf("another pipeline's checkpoint was touched: %d", cpOther)
	}
}

// TestPlanWriteBatchFolds is the pure unit test of the fold rules: same-key
// requests collapse to one statement each (checkpoint → max seq, source state
// → highest frontier with its state), appends stay in queue order and are the
// only thing maxRows chunks, and the rest keeps queue order.
func TestPlanWriteBatchFolds(t *testing.T) {
	append1 := &writeReq{kind: writeAppend, pipeline: "p", msg: registry.Message{ID: "a1"}}
	append2 := &writeReq{kind: writeAppend, pipeline: "p", msg: registry.Message{ID: "a2"}}
	cp5 := &writeReq{kind: writeCheckpoint, pipeline: "p", cpSeq: 5}
	cp2 := &writeReq{kind: writeCheckpoint, pipeline: "p", cpSeq: 2}
	cpQ := &writeReq{kind: writeCheckpoint, pipeline: "q", cpSeq: 9}
	ss3 := &writeReq{kind: writeSourceState, pipeline: "p", source: "in", state: []byte("s3"), srcSeq: 3}
	ss7 := &writeReq{kind: writeSourceState, pipeline: "p", source: "in", state: []byte("s7"), srcSeq: 7}
	ssOther := &writeReq{kind: writeSourceState, pipeline: "p", source: "other", state: []byte("x"), srcSeq: 1}
	dl := &writeReq{kind: writeDeadLetter, pipeline: "p"}

	batch := []*writeReq{append1, cp5, ss3, append2, cp2, ss7, ssOther, dl, cpQ}
	p := planWriteBatch(batch, 256)

	if len(p.appends) != 1 || !reflect.DeepEqual(p.appends[0], []*writeReq{append1, append2}) {
		t.Fatalf("append chunks = %v, want one chunk in queue order", p.appends)
	}
	if len(p.stmts.checkpoints) != 2 {
		t.Fatalf("checkpoint statements = %d, want 2 (p and q)", len(p.stmts.checkpoints))
	}
	if p.stmts.checkpoints[0].pipeline != "p" || p.stmts.checkpoints[0].seq != 5 {
		t.Errorf("checkpoint p = %d, want max 5", p.stmts.checkpoints[0].seq)
	}
	if !reflect.DeepEqual(p.stmts.checkpoints[0].waiters, []*writeReq{cp5, cp2}) {
		t.Errorf("checkpoint p waiters = %v, want both requests", p.stmts.checkpoints[0].waiters)
	}
	if p.stmts.checkpoints[1].pipeline != "q" || p.stmts.checkpoints[1].seq != 9 {
		t.Errorf("checkpoint q = %+v, want 9", p.stmts.checkpoints[1])
	}
	if len(p.stmts.sourceStates) != 2 {
		t.Fatalf("source-state statements = %d, want 2", len(p.stmts.sourceStates))
	}
	pin := p.stmts.sourceStates[0]
	if pin.source != "in" || pin.srcSeq != 7 || string(pin.state) != "s7" {
		t.Errorf("source state (p,in) = %s/%d, want the latest pair s7/7", pin.state, pin.srcSeq)
	}
	if !reflect.DeepEqual(pin.waiters, []*writeReq{ss3, ss7}) {
		t.Errorf("source state waiters = %v, want both requests", pin.waiters)
	}
	if p.stmts.sourceStates[1].source != "other" {
		t.Errorf("second source-state statement = %q", p.stmts.sourceStates[1].source)
	}
	if !reflect.DeepEqual(p.stmts.rest, []*writeReq{dl}) {
		t.Errorf("rest = %v, want the dead letter", p.stmts.rest)
	}

	// maxRows chunks appends only; every request still appears exactly once.
	p2 := planWriteBatch(batch, 1)
	if len(p2.appends) != 2 {
		t.Fatalf("chunks with maxRows=1 = %d, want 2", len(p2.appends))
	}
	if !reflect.DeepEqual(p2.appends[0], []*writeReq{append1}) || !reflect.DeepEqual(p2.appends[1], []*writeReq{append2}) {
		t.Fatalf("chunk contents = %v", p2.appends)
	}
	seen := map[*writeReq]bool{}
	for _, chunk := range p2.appends {
		for _, r := range chunk {
			if seen[r] {
				t.Fatalf("request planned twice: %+v", r)
			}
			seen[r] = true
		}
	}
	for i := range p2.stmts.checkpoints {
		for _, r := range p2.stmts.checkpoints[i].waiters {
			if seen[r] {
				t.Fatalf("request planned twice: %+v", r)
			}
			seen[r] = true
		}
	}
	for i := range p2.stmts.sourceStates {
		for _, r := range p2.stmts.sourceStates[i].waiters {
			if seen[r] {
				t.Fatalf("request planned twice: %+v", r)
			}
			seen[r] = true
		}
	}
	for _, r := range p2.stmts.rest {
		if seen[r] {
			t.Fatalf("request planned twice: %+v", r)
		}
		seen[r] = true
	}
	if len(seen) != len(batch) {
		t.Fatalf("planned %d requests, want %d", len(seen), len(batch))
	}
}

// TestWriteOptionsNormalized pins the option defaults: a zero MaxRows means
// the 256-row default, a negative MaxWait means write-through.
func TestWriteOptionsNormalized(t *testing.T) {
	if got := (WriteOptions{}).normalized(); got.MaxRows != 256 || got.MaxWait != 0 {
		t.Fatalf("zero options = %+v", got)
	}
	if got := (WriteOptions{MaxRows: 8, MaxWait: -time.Second}).normalized(); got.MaxRows != 8 || got.MaxWait != 0 {
		t.Fatalf("negative wait = %+v", got)
	}
	if got := DefaultWriteOptions(); got.MaxRows != 256 || got.MaxWait != 2*time.Millisecond {
		t.Fatalf("default options = %+v", got)
	}
}

// TestWriteOptionsOnBatch: the optional observer reports every committed
// group's spool-row count and a non-negative commit duration. It is the
// batch-size seam the ops side can bridge to metrics; the store itself stays
// telemetry-free.
func TestWriteOptionsOnBatch(t *testing.T) {
	var (
		mu     sync.Mutex
		groups []int
		total  time.Duration
	)
	st, err := OpenSQLiteWithOptions(filepath.Join(t.TempDir(), "onbatch.db"), WriteOptions{
		MaxRows: 64,
		MaxWait: time.Second,
		OnBatch: func(rows int, waited time.Duration) {
			if waited < 0 {
				// Cannot call t.Fatal from the writer goroutine.
				panic("OnBatch reported a negative duration")
			}
			mu.Lock()
			groups = append(groups, rows)
			total += waited
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	// Park the writer before its first commit so a known group queues behind
	// it (same deterministic pattern as the batching tests).
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	hook := func(int) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	st.w.testBatchHook.Store(&hook)

	now := time.Now()
	go func() { _, _ = st.AppendSpool("p", writerTestMessage("first"), now) }()
	<-started

	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := st.AppendSpool("p", writerTestMessage(fmt.Sprintf("m-%d", i)), now); err != nil {
				t.Errorf("append %d: %v", i, err)
			}
		}(i)
	}
	waitQueueLen(t, st.w, n)
	close(release)
	wg.Wait()

	mu.Lock()
	got := append([]int(nil), groups...)
	waited := total
	mu.Unlock()
	var sum, widest int
	for _, rows := range got {
		sum += rows
		if rows > widest {
			widest = rows
		}
	}
	if sum != n+1 {
		t.Fatalf("observer counted %d spool rows across groups %v, want %d", sum, got, n+1)
	}
	if widest < n {
		t.Fatalf("widest observed group = %d rows, want the queued %d-row group (groups=%v)", widest, n, got)
	}
	if waited <= 0 {
		t.Fatalf("observer total duration = %v, want > 0", waited)
	}
}

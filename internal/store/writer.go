package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// ErrClosed is returned to every caller whose write could not run because the
// store was closed. It is explicit and retryable: the caller re-emits later,
// so no acknowledged-but-uncommitted state is lost (the refusal contract).
var ErrClosed = errors.New("store: closed")

// Defaults of the group-commit write path (design 2026-09-24 §2.2, §2.6.2).
const (
	defaultWriteBatchRows = 256
	defaultWriteBatchWait = 2 * time.Millisecond
	// maxWriteBatchRows caps MaxRows: one spool row binds 9 SQL variables,
	// and a multi-row INSERT past the driver's variable limit fails the whole
	// transaction, rejecting every waiter in it. The refused source re-emits,
	// the same oversized batch forms again and fails again — a livelock, not
	// a transient error. 2000 rows is 18000 bindings, measured to commit; the
	// driver rejects 36000 (4000 rows). runtimecfg validates the same bound
	// before the value reaches the store; normalized clamps as the defense
	// for options built directly in code.
	maxWriteBatchRows = 2000
)

// WriteOptions tunes the SQLite group-commit writer.
type WriteOptions struct {
	// MaxRows caps one group-commit transaction's spool rows (the
	// storage.write_batch.max_rows knob). Values <= 0 mean the default (256);
	// values above maxWriteBatchRows are clamped to it (18000 SQL bindings —
	// see maxWriteBatchRows).
	MaxRows int

	// MaxWait is the ceiling on how long a queued write may wait for
	// companions before its group commits (storage.write_batch.max_wait_ms;
	// 0 = write-through, i.e. no companion wait). The wait is realised as a
	// bounded scheduler-yield probe, never a sleeping timer — an OS timer's
	// floor (>500 µs on Windows) would cap a lone producer at ~1K rows/s
	// while gaining nothing. See collect below and "No artificial linger" in
	// docs/developer/02-engine.md.
	MaxWait time.Duration

	// OnBatch is an optional observation hook for committed groups: rows is
	// the group's spool-row count (0 for a statements-only group) and waited
	// is the group's transaction duration, from BeginTx to the commit return.
	// It runs on the writer goroutine — the store's single write path — so it
	// must not block; the store stays a leaf and never imports telemetry, the
	// ops side bridges it to metrics. Nil (default) means no observation.
	OnBatch func(rows int, waited time.Duration)
}

// DefaultWriteOptions returns the production group-commit settings (256 rows
// per transaction, 2 ms wait ceiling).
func DefaultWriteOptions() WriteOptions {
	return WriteOptions{MaxRows: defaultWriteBatchRows, MaxWait: defaultWriteBatchWait}
}

// normalized applies the defaults: a non-positive MaxRows means the default
// batch size; a negative MaxWait means write-through (0); a MaxRows beyond the
// SQL-variable budget is clamped rather than rejected, because an options
// value built in code (not through runtimecfg) must not be able to livelock
// the writer with a batch that can never commit.
func (o WriteOptions) normalized() WriteOptions {
	if o.MaxRows <= 0 {
		o.MaxRows = defaultWriteBatchRows
	}
	if o.MaxRows > maxWriteBatchRows {
		o.MaxRows = maxWriteBatchRows
	}
	if o.MaxWait < 0 {
		o.MaxWait = 0
	}
	return o
}

// isZero reports whether the options carry no explicit tuning at all. The
// struct holds a func field, so it is not comparable with ==; callers that
// treat "unset" specially (the store owner) must use this instead.
func (o WriteOptions) isZero() bool {
	return o.MaxRows == 0 && o.MaxWait == 0 && o.OnBatch == nil
}

// writeKind identifies one queued write request; the writer executes each kind
// on its dedicated connection.
type writeKind uint8

const (
	writeAppend writeKind = iota
	writeCheckpoint
	writeSourceState
	writeDeadLetter
	writeDeleteSpoolThrough
	writeDeleteDeadLetters
	writeDeleteDeadLettersBefore
	writeDeleteJobRunsBefore
	writeCreateJobRun
	writeUpdateJobRun
)

// writeReq is one write operation in flight. The caller enqueues it and blocks
// on done; the writer fills the result fields and closes done. All fields are
// written by the enqueuing goroutine before enqueue and read by the caller
// after <-done (the close is the happens-before edge), except the result
// fields (seq/err/n), which only the writer touches.
type writeReq struct {
	kind writeKind
	done chan struct{}
	err  error

	pipeline string
	seq      int64 // append result

	msg    registry.Message
	ingest time.Time

	cpSeq   int64 // checkpoint
	source  string
	state   []byte
	srcSeq  int64
	dl      DeadLetter
	ids     []int64
	cutoff  time.Time
	through int64
	jr      JobRun
	n       int64 // affected-row result (deletes)
}

func newWriteReq(kind writeKind) *writeReq {
	return &writeReq{kind: kind, done: make(chan struct{})}
}

// resolve closes the request with its outcome. Exactly one goroutine (the
// writer) calls it exactly once per request.
func (r *writeReq) resolve(err error) {
	r.err = err
	close(r.done)
}

// wait blocks until the writer has executed the request (successfully or not).
func (r *writeReq) wait() error {
	<-r.done
	return r.err
}

// writer is the store's single write path: one goroutine, one dedicated
// connection, every mutation serialized through it. Group commit collects the
// requests queued behind the current transaction and executes them together:
// one multi-row INSERT for the spool, coalesced checkpoints and source states,
// everything else in queue order. Callers block until their transaction
// commits (invariant 1 and the refusal contract are unchanged: a failed
// transaction fails every waiter in it, and the source re-emits).
type writer struct {
	conn *sql.Conn
	opts WriteOptions

	mu     sync.Mutex
	queue  []*writeReq
	closed bool

	wake chan struct{} // capacity 1; a hint that the queue may be non-empty
	done chan struct{} // closed when the writer goroutine has exited

	// High-water marks keep the persisted checkpoint/source state monotonic
	// across batches: a later transaction may only carry values at or above
	// what an earlier one wrote (a caller whose value is superseded still
	// succeeds — its value or a larger one is durable). Writer-goroutine
	// state only, so no lock.
	cpSeqs  map[string]int64
	srcSeqs map[string]int64

	// testBatchHook injects a failure or a pause into every group-commit
	// transaction (package tests only; nil in production). It runs on the
	// writer goroutine with the batch not yet begun.
	testBatchHook atomic.Pointer[func(appends int) error]
}

func newWriter(conn *sql.Conn, opts WriteOptions) *writer {
	w := &writer{
		conn:    conn,
		opts:    opts,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		cpSeqs:  map[string]int64{},
		srcSeqs: map[string]int64{},
	}
	go w.run()
	return w
}

// do enqueues one request and blocks until the writer resolved it.
func (w *writer) do(req *writeReq, op string) error {
	if err := w.enqueue(req); err != nil {
		return fmt.Errorf("store: %s: %w", op, err)
	}
	if err := req.wait(); err != nil {
		return fmt.Errorf("store: %s: %w", op, err)
	}
	return nil
}

func (w *writer) enqueue(req *writeReq) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.queue = append(w.queue, req)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default: // a wakeup is already pending; it will observe this request
	}
	return nil
}

// run is the writer goroutine: drain a group, give companions up to MaxWait to
// join it, execute it, repeat until the queue is empty and the store is
// closed.
func (w *writer) run() {
	defer close(w.done)
	for {
		batch := w.takeAll()
		if len(batch) == 0 {
			return // closed with an empty queue: shutdown drained everything else
		}
		batch = w.collect(batch)
		w.execute(batch)
	}
}

// collect gives companions released by the previous commit a chance to join
// the drained group, bounded by MaxWait and by the group reaching MaxRows.
//
// The probe is scheduler yields, not a timer: the callers are runnable but
// have not enqueued their next request yet (channel close woke them; their
// source loop has a few microseconds of bookkeeping to do), and each yield
// lets one of them make progress. A time-based sleep cannot be the primary
// probe because its floor is the system timer quantum — microseconds on
// Linux, but >500 µs on Windows, which would cap a lone producer at ~1K
// rows/s. MaxWait remains the ceiling: `max_wait_ms: 0` disables companion
// collection, and larger values tolerate a wider burst (companionProbes).
//
// The budget does not grow with the previous group's size. A wider budget
// was measured and rejected (2026-09-25): the engine's source loops take
// longer between appends than any affordable spin covers, so long budgets
// only added writer latency without raising the observed group size.
func (w *writer) collect(batch []*writeReq) []*writeReq {
	if w.opts.MaxWait <= 0 || len(batch) >= w.opts.MaxRows {
		return batch
	}
	probes := companionProbes(w.opts.MaxWait)
	for i := 0; i < probes && len(batch) < w.opts.MaxRows; i++ {
		runtime.Gosched()
		if w.isClosed() {
			return batch // Close: commit what we hold, then shut down
		}
		batch = append(batch, w.takeQueued()...)
	}
	return batch
}

// companionProbes maps the configured wait ceiling onto the yield budget: one
// yield per 250 µs of MaxWait (the default 2 ms is 8), at least one for any
// positive wait, capped at 16. Two constraints fix the mapping: yields cost
// tens of nanoseconds, so the cap bounds a lone producer's overhead to well
// under a microsecond per commit; and a larger budget cannot help anyway
// (see collect). The mapping keeps the knob's documented meaning — 0 disables
// companion collection, larger values tolerate a wider burst — without
// binding the writer to the platform's timer quantum.
func companionProbes(maxWait time.Duration) int {
	probes := int(maxWait / (250 * time.Microsecond))
	if probes < 1 {
		probes = 1
	}
	if probes > 16 {
		probes = 16
	}
	return probes
}

// takeQueued drains the queue without blocking.
func (w *writer) takeQueued() []*writeReq {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return nil
	}
	batch := w.queue
	w.queue = nil
	return batch
}

// takeAll blocks until requests are queued (or the store closes) and returns
// the whole queue as one group.
func (w *writer) takeAll() []*writeReq {
	for {
		w.mu.Lock()
		if len(w.queue) > 0 {
			batch := w.queue
			w.queue = nil
			w.mu.Unlock()
			return batch
		}
		closed := w.closed
		w.mu.Unlock()
		if closed {
			return nil
		}
		<-w.wake
	}
}

// shutdown stops the writer. Requests still queued are refused with err so
// their callers cannot block forever; a transaction already in flight is
// allowed to finish (the writer joins before shutdown returns), and the
// dedicated connection is released. Idempotent.
func (w *writer) shutdown(err error) {
	w.mu.Lock()
	w.closed = true
	pending := w.queue
	w.queue = nil
	w.mu.Unlock()
	for _, req := range pending {
		req.resolve(err)
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
	<-w.done
	_ = w.conn.Close()
}

// --- batch planning ---

// checkpointWrite is one coalesced checkpoint upsert.
type checkpointWrite struct {
	pipeline string
	seq      int64
	waiters  []*writeReq
}

// sourceStateWrite is one coalesced source-state upsert: the newest
// (state, srcSeq) pair of that source, never a field-level merge.
type sourceStateWrite struct {
	pipeline string
	source   string
	state    []byte
	srcSeq   int64
	waiters  []*writeReq
}

// writeStatements is the non-append half of a group: coalesced checkpoint and
// source-state upserts plus the requests executed individually, in queue
// order (dead letters, deletes, job-run records).
type writeStatements struct {
	checkpoints  []checkpointWrite
	sourceStates []sourceStateWrite
	rest         []*writeReq
}

// writePlan splits one drained group into append chunks (each at most maxRows
// rows, in queue order) and the coalesced/rest statements that ride the first
// chunk's transaction.
type writePlan struct {
	appends [][]*writeReq
	stmts   writeStatements
}

// planWriteBatch folds requests that touch the same key:
//
//   - checkpoints: one upsert per pipeline, carrying the largest seq. Every
//     waiter succeeds once that value (or a larger one from an earlier
//     transaction) is durable.
//   - source states: one upsert per (pipeline, source), carrying the request
//     with the largest srcSeq — the state and its frontier stay paired; a tie
//     keeps the later request.
//
// Appends are never folded; they keep queue order so their seqs are assigned
// in admission order.
func planWriteBatch(batch []*writeReq, maxRows int) writePlan {
	var p writePlan
	var chunk []*writeReq
	cpIndex := map[string]int{}
	ssIndex := map[string]int{}
	for _, r := range batch {
		switch r.kind {
		case writeAppend:
			chunk = append(chunk, r)
			if len(chunk) == maxRows {
				p.appends = append(p.appends, chunk)
				chunk = nil
			}
		case writeCheckpoint:
			if i, ok := cpIndex[r.pipeline]; ok {
				if r.cpSeq > p.stmts.checkpoints[i].seq {
					p.stmts.checkpoints[i].seq = r.cpSeq
				}
				p.stmts.checkpoints[i].waiters = append(p.stmts.checkpoints[i].waiters, r)
				continue
			}
			cpIndex[r.pipeline] = len(p.stmts.checkpoints)
			p.stmts.checkpoints = append(p.stmts.checkpoints, checkpointWrite{
				pipeline: r.pipeline, seq: r.cpSeq, waiters: []*writeReq{r},
			})
		case writeSourceState:
			key := sourceKey(r.pipeline, r.source)
			if i, ok := ssIndex[key]; ok {
				if r.srcSeq >= p.stmts.sourceStates[i].srcSeq {
					p.stmts.sourceStates[i].state = r.state
					p.stmts.sourceStates[i].srcSeq = r.srcSeq
				}
				p.stmts.sourceStates[i].waiters = append(p.stmts.sourceStates[i].waiters, r)
				continue
			}
			ssIndex[key] = len(p.stmts.sourceStates)
			p.stmts.sourceStates = append(p.stmts.sourceStates, sourceStateWrite{
				pipeline: r.pipeline, source: r.source, state: r.state, srcSeq: r.srcSeq,
				waiters: []*writeReq{r},
			})
		default:
			p.stmts.rest = append(p.stmts.rest, r)
		}
	}
	if len(chunk) > 0 {
		p.appends = append(p.appends, chunk)
	}
	return p
}

// execute commits one drained group. The non-append statements ride the first
// append chunk's transaction (one fsync/write per group); a group with no
// appends is one transaction of its own.
func (w *writer) execute(batch []*writeReq) {
	plan := planWriteBatch(batch, w.opts.MaxRows)
	ctx := context.Background()
	for i, chunk := range plan.appends {
		var stmts *writeStatements
		if i == 0 {
			stmts = &plan.stmts
		}
		w.commitGroup(ctx, chunk, stmts)
	}
	if len(plan.appends) == 0 {
		w.commitGroup(ctx, nil, &plan.stmts)
	}
}

// commitGroup runs one transaction and resolves every waiter it carried. A
// failed transaction fails all of them with the same error: the refusal
// contract (nothing durable, safe to re-emit).
func (w *writer) commitGroup(ctx context.Context, appends []*writeReq, stmts *writeStatements) {
	start := time.Now()
	if stmts != nil {
		// A waiter an earlier transaction already satisfied is resolved
		// BEFORE the transaction: its value is durable, so it must not be
		// rejected when an unrelated statement of this group fails and rolls
		// the transaction back (the coalescing promise: "a superseded caller
		// still succeeds").
		w.resolveSatisfied(stmts)
	}
	errTx := w.withTx(ctx, len(appends), func(tx *sql.Tx) error {
		if len(appends) > 0 {
			if err := insertSpoolRows(ctx, tx, appends); err != nil {
				return err
			}
		}
		if stmts == nil {
			return nil
		}
		return w.execStatements(ctx, tx, stmts)
	})
	if errTx != nil {
		errTx = fmt.Errorf("store: write batch: %w", errTx)
	} else if stmts != nil {
		w.applyMarks(stmts)
	}
	if errTx == nil && w.opts.OnBatch != nil {
		w.opts.OnBatch(len(appends), time.Since(start))
	}
	for _, r := range appends {
		r.resolve(errTx)
	}
	if stmts != nil {
		for i := range stmts.checkpoints {
			for _, r := range stmts.checkpoints[i].waiters {
				r.resolve(errTx)
			}
		}
		for i := range stmts.sourceStates {
			for _, r := range stmts.sourceStates[i].waiters {
				r.resolve(errTx)
			}
		}
		for _, r := range stmts.rest {
			r.resolve(errTx)
		}
	}
}

// resolveSatisfied succeeds and unlinks the coalesced waiters an earlier
// transaction already satisfied: a checkpoint at or below the durable high
// water mark, a source state at or below its stored frontier. They must not
// ride this transaction at all — a failing sibling statement (or the commit
// itself) would otherwise reject them with an error for a value that is
// already durable. Runs on the writer goroutine, before the transaction, so
// the high-water marks it reads are the writer's own. execStatements keeps
// its skip checks as defense for any entry that slips through.
func (w *writer) resolveSatisfied(stmts *writeStatements) {
	for i := range stmts.checkpoints {
		cp := &stmts.checkpoints[i]
		if cp.seq > w.cpSeqs[cp.pipeline] {
			continue
		}
		for _, r := range cp.waiters {
			r.resolve(nil)
		}
		cp.waiters = nil
	}
	for i := range stmts.sourceStates {
		ss := &stmts.sourceStates[i]
		if ss.srcSeq > w.srcSeqs[sourceKey(ss.pipeline, ss.source)] {
			continue
		}
		for _, r := range ss.waiters {
			r.resolve(nil)
		}
		ss.waiters = nil
	}
}

// execStatements runs the coalesced and individual non-append writes of one
// group inside tx, updating the monotonic high-water marks on success.
func (w *writer) execStatements(ctx context.Context, tx *sql.Tx, stmts *writeStatements) error {
	now := time.Now().UTC().Format(timeLayout)
	for i := range stmts.checkpoints {
		cp := &stmts.checkpoints[i]
		if cp.seq <= w.cpSeqs[cp.pipeline] {
			continue // superseded by an earlier transaction: still satisfied
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO checkpoint (pipeline, spool_seq, updated_at) VALUES (?, ?, ?)
			 ON CONFLICT(pipeline) DO UPDATE SET spool_seq = excluded.spool_seq, updated_at = excluded.updated_at`,
			cp.pipeline, cp.seq, now); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
	}
	for i := range stmts.sourceStates {
		ss := &stmts.sourceStates[i]
		if ss.srcSeq <= w.srcSeqs[sourceKey(ss.pipeline, ss.source)] {
			continue // superseded: the stored pair is at or beyond this frontier
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO source_state (pipeline, source, state, src_seq, updated_at) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(pipeline, source) DO UPDATE SET state = excluded.state, src_seq = excluded.src_seq, updated_at = excluded.updated_at`,
			ss.pipeline, ss.source, ss.state, ss.srcSeq, now); err != nil {
			return fmt.Errorf("source state: %w", err)
		}
	}
	for _, r := range stmts.rest {
		if err := execWriteRequest(ctx, tx, r); err != nil {
			return err
		}
	}
	return nil
}

// applyMarks advances the monotonic high-water marks of a committed group, so
// a later transaction's smaller value can never regress the persisted
// checkpoint or source state. It runs only after the transaction committed.
func (w *writer) applyMarks(stmts *writeStatements) {
	for i := range stmts.checkpoints {
		cp := &stmts.checkpoints[i]
		if cp.seq > w.cpSeqs[cp.pipeline] {
			w.cpSeqs[cp.pipeline] = cp.seq
		}
	}
	for i := range stmts.sourceStates {
		ss := &stmts.sourceStates[i]
		if ss.srcSeq > w.srcSeqs[sourceKey(ss.pipeline, ss.source)] {
			w.srcSeqs[sourceKey(ss.pipeline, ss.source)] = ss.srcSeq
		}
	}
}

// withTx runs fn in one transaction on the writer's connection. appends is
// the group's spool-row count, handed to the optional test hook only.
func (w *writer) withTx(ctx context.Context, appends int, fn func(tx *sql.Tx) error) error {
	if hook := w.testBatchHook.Load(); hook != nil {
		if err := (*hook)(appends); err != nil {
			return err
		}
	}
	tx, err := w.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	// A store that began closing while this transaction was open refuses it:
	// the rollback keeps the refusal honest (nothing landed) and every waiter
	// in the group sees ErrClosed instead of a write that raced Close.
	if w.isClosed() {
		_ = tx.Rollback()
		return ErrClosed
	}
	return tx.Commit()
}

func (w *writer) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// insertSpoolRows writes one group's spool rows in a single multi-row INSERT
// and hands every row its sequence. spool.seq is INTEGER PRIMARY KEY
// AUTOINCREMENT, so one statement assigns consecutive rowids: the last id
// minus len-1 is the first, and the queue order is the seq order.
func insertSpoolRows(ctx context.Context, tx *sql.Tx, reqs []*writeReq) error {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO spool (pipeline, message_id, codec, raw, meta, cursor, src_name, src_seq, ingest_time) VALUES `)
	args := make([]any, 0, len(reqs)*9)
	for i, r := range reqs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, r.pipeline, r.msg.ID, r.msg.Codec, r.msg.Raw, string(marshalMeta(r.msg.Meta)),
			r.msg.Cursor, r.msg.SrcName, r.msg.SrcSeq, r.ingest.UTC().Format(timeLayout))
	}
	res, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("append spool: %w", err)
	}
	last, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("append spool: last insert id: %w", err)
	}
	base := last - int64(len(reqs)) + 1
	for i, r := range reqs {
		r.seq = base + int64(i)
	}
	return nil
}

// execWriteRequest runs one non-append request on tx.
func execWriteRequest(ctx context.Context, tx *sql.Tx, r *writeReq) error {
	switch r.kind {
	case writeDeadLetter:
		return insertDeadLetter(ctx, tx, r.dl)
	case writeDeleteSpoolThrough:
		n, err := deleteSpoolThrough(ctx, tx, r.pipeline, r.through)
		r.n = n
		return err
	case writeDeleteDeadLetters:
		n, err := deleteDeadLetters(ctx, tx, r.pipeline, r.ids)
		r.n = n
		return err
	case writeDeleteDeadLettersBefore:
		n, err := deleteDeadLettersBefore(ctx, tx, r.pipeline, r.cutoff)
		r.n = n
		return err
	case writeDeleteJobRunsBefore:
		n, err := deleteJobRunsBefore(ctx, tx, r.pipeline, r.cutoff)
		r.n = n
		return err
	case writeCreateJobRun:
		return upsertJobRun(ctx, tx, r.jr, true)
	case writeUpdateJobRun:
		return upsertJobRun(ctx, tx, r.jr, false)
	default:
		return fmt.Errorf("store: unknown write kind %d", r.kind)
	}
}

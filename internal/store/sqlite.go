package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/eventboat/eventboat/internal/registry"
)

const schema = `
CREATE TABLE IF NOT EXISTS spool (
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline    TEXT    NOT NULL,
  message_id  TEXT    NOT NULL,
  codec       TEXT    NOT NULL,
  raw         BLOB    NOT NULL,
  meta        TEXT    NOT NULL,
  cursor      TEXT    NOT NULL DEFAULT '',
  src_name    TEXT    NOT NULL DEFAULT '',
  src_seq     INTEGER NOT NULL DEFAULT 0,
  ingest_time TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spool_pipeline ON spool(pipeline, seq);

CREATE TABLE IF NOT EXISTS checkpoint (
  pipeline   TEXT    NOT NULL,
  spool_seq  INTEGER NOT NULL,
  updated_at TEXT    NOT NULL,
  PRIMARY KEY (pipeline)
);

CREATE TABLE IF NOT EXISTS source_state (
  pipeline   TEXT    NOT NULL,
  source     TEXT    NOT NULL,
  state      BLOB,
  src_seq    INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT    NOT NULL,
  PRIMARY KEY (pipeline, source)
);

-- The origin_node and retry_count columns are deprecated and never written;
-- they only exist in databases created before review-2026-09 (kept there
-- harmlessly via their NOT NULL DEFAULTs, no DROP migration).
CREATE TABLE IF NOT EXISTS dead_letter (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline    TEXT    NOT NULL,
  message_id  TEXT    NOT NULL,
  job_run_id  TEXT    NOT NULL DEFAULT '',
  node        TEXT    NOT NULL,
  edge        TEXT    NOT NULL DEFAULT '',
  reason      TEXT    NOT NULL,
  class       TEXT    NOT NULL DEFAULT '',
  backtrace   TEXT    NOT NULL DEFAULT '',
  raw         BLOB    NOT NULL,
  codec       TEXT    NOT NULL DEFAULT '',
  meta        TEXT    NOT NULL DEFAULT '{}',
  cursor      TEXT    NOT NULL DEFAULT '',
  src_name    TEXT    NOT NULL DEFAULT '',
  src_seq     INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_time ON dead_letter(pipeline, id);
-- idx_dlq_pipeline_created serves DeadLettersSince (created_at >= ?) and the
-- DeleteDeadLettersBefore retention sweep (created_at < ?): without it the
-- first sweep over a large backlog is a per-pipeline table scan per batch.
CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_created ON dead_letter(pipeline, created_at);

CREATE TABLE IF NOT EXISTS job_run (
  run_id        TEXT PRIMARY KEY,
  pipeline      TEXT    NOT NULL,
  status        TEXT    NOT NULL,
  trigger_type  TEXT    NOT NULL,
  parameters    TEXT    NOT NULL DEFAULT '{}',
  scheduled_for TEXT    NOT NULL DEFAULT '',
  started_at    TEXT    NOT NULL DEFAULT '',
  ended_at      TEXT    NOT NULL DEFAULT '',
  rows_read     INTEGER NOT NULL DEFAULT 0,
  delivered     INTEGER NOT NULL DEFAULT 0,
  dead_lettered INTEGER NOT NULL DEFAULT 0,
  error         TEXT    NOT NULL DEFAULT '',
  updated_at    TEXT    NOT NULL
);
`

// timeLayout is the fixed-width RFC3339 formatter for every persisted
// timestamp. RFC3339Nano omits trailing zeros in the fraction, so
// "…T12:00:00Z" sorts AFTER "…T12:00:00.5Z" lexicographically while it is
// EARLIER in time — every range query (DeadLettersSince,
// DeleteDeadLettersBefore, DeleteJobRunsBefore) and ORDER BY (JobRuns,
// RunnableJobRuns) would silently misorder rows that differ only in
// sub-second precision. Padding the fraction to nine digits makes the text
// order equal the time order; the layout stays RFC3339-parsable (UTC "Z").
// The fix is forward-only: rows written by earlier versions keep their old
// text and are compared as-is (beta ruling: no migration).
const timeLayout = "2006-01-02T15:04:05.000000000Z"

// indexSchema runs after column migrations: idx_dlq_run references
// dead_letter.job_run_id, which only exists once an M1 database has been
// migrated (M2 review R6).
const indexSchema = `
CREATE INDEX IF NOT EXISTS idx_dlq_run ON dead_letter(pipeline, job_run_id);
CREATE INDEX IF NOT EXISTS idx_job_run_pipeline ON job_run(pipeline, started_at);
CREATE INDEX IF NOT EXISTS idx_job_run_sched ON job_run(pipeline, scheduled_for);
`

// migrate applies guarded column additions for databases created before M2
// (job_run_id) and before candidate 07 (class). Old rows read with the empty
// default; the beta ruling forbids migrating other old layouts, not adding a
// column in place.
func migrate(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(dead_letter)`)
	if err != nil {
		return err
	}
	hasRunID, hasClass := false, false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		switch name {
		case "job_run_id":
			hasRunID = true
		case "class":
			hasClass = true
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if !hasRunID {
		if _, err := db.Exec(`ALTER TABLE dead_letter ADD COLUMN job_run_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("store: migrate dead_letter.job_run_id: %w", err)
		}
	}
	if !hasClass {
		if _, err := db.Exec(`ALTER TABLE dead_letter ADD COLUMN class TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("store: migrate dead_letter.class: %w", err)
		}
	}
	return nil
}

// SQLite is the default durable store: one file, WAL mode, pure-Go driver.
//
// Every mutation runs on one dedicated connection through the writer
// goroutine (writer.go): write-write contention between pooled connections
// disappears, group commit batches the spool INSERT, and checkpoint/source
// state requests coalesce. Reads stay on the pool, which keeps status and
// replay queries moving while a transaction commits.
type SQLite struct {
	db *sql.DB
	w  *writer

	closeOnce sync.Once
	closeErr  error
}

// DB exposes the underlying handle (tests only).
func (s *SQLite) DB() *sql.DB { return s.db }

// OpenSQLite opens (creating if needed) the SQLite database at path with WAL
// journaling and a busy timeout.
//
// synchronous=NORMAL (the WAL pairing) trades one durability nuance for write
// throughput: commits stop fsyncing the WAL on every transaction and fsync
// at WAL checkpoints instead. At-least-once semantics hold — NORMAL still
// survives a process crash intact (the primary failure model: the OS keeps
// the WAL), so the uncommitted tail replayed on restart never loses
// acknowledged-but-uncommitted state ordering. A power loss (OS crash) can
// only lose the most recent WAL frames, which widens the replay window —
// duplicates on re-emit, never loss, the same contract as a checkpoint write
// that never made it to disk. FULL would restore per-commit fsync at the
// cost of the per-write convoy this pragma removes.
func OpenSQLite(path string) (*SQLite, error) {
	return OpenSQLiteWithOptions(path, DefaultWriteOptions())
}

// OpenSQLiteWithOptions is OpenSQLite with the group-commit tuning surface
// (storage.write_batch.*): MaxRows caps one multi-row INSERT, MaxWait is the
// ceiling on a request's wait for companions (see WriteOptions). The DSN
// contract (WAL, synchronous=NORMAL, busy_timeout, foreign_keys) is the same.
func OpenSQLiteWithOptions(path string, opts WriteOptions) (*SQLite, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite: %w", err)
	}
	// One dedicated connection belongs to the writer goroutine (below); the
	// remaining pool serves reads. WAL allows one writer plus concurrent
	// readers, and writer-writer contention is gone by construction: there is
	// exactly one write transaction at a time, on the writer's connection.
	// ReplayPage drains its rows before invoking callbacks, so nothing holds
	// a read across a callback that writes.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(indexSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: indexes: %w", err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: writer connection: %w", err)
	}
	s := &SQLite{db: db}
	s.w = newWriter(conn, opts.normalized())
	return s, nil
}

func (s *SQLite) AppendSpool(pipeline string, msg registry.Message, ingestTime time.Time) (int64, error) {
	req := newWriteReq(writeAppend)
	req.pipeline, req.msg, req.ingest = pipeline, msg, ingestTime
	if err := s.w.do(req, "append spool"); err != nil {
		return 0, err
	}
	return req.seq, nil
}

func (s *SQLite) ReplayFrom(pipeline string, afterSeq int64, fn func(int64, registry.Message, time.Time) error) error {
	for {
		last, more, err := s.ReplayPage(pipeline, afterSeq, 500, fn)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
		afterSeq = last
	}
}

// ReplayPage walks one bounded window of the spool. Rows are drained and the
// query connection released before callbacks run: replay dispatch can itself
// write (checkpoints, dead letters) on this single-connection store and
// would otherwise deadlock. Windowing keeps memory flat regardless of the
// replay span (M2 review R7).
func (s *SQLite) ReplayPage(pipeline string, afterSeq int64, limit int, fn func(int64, registry.Message, time.Time) error) (int64, bool, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(
		`SELECT seq, message_id, codec, raw, meta, cursor, src_name, src_seq, ingest_time
		 FROM spool WHERE pipeline = ? AND seq > ? ORDER BY seq LIMIT ?`, pipeline, afterSeq, limit)
	if err != nil {
		return afterSeq, false, fmt.Errorf("store: replay: %w", err)
	}
	type replayRow struct {
		seq    int64
		msg    registry.Message
		ingest time.Time
	}
	var collected []replayRow
	for rows.Next() {
		var (
			r         replayRow
			meta      string
			ingestStr string
		)
		if err := rows.Scan(&r.seq, &r.msg.ID, &r.msg.Codec, &r.msg.Raw, &meta, &r.msg.Cursor, &r.msg.SrcName, &r.msg.SrcSeq, &ingestStr); err != nil {
			_ = rows.Close()
			return afterSeq, false, fmt.Errorf("store: replay scan: %w", err)
		}
		r.msg.Meta = unmarshalMeta([]byte(meta))
		if ts, err := time.Parse(time.RFC3339Nano, ingestStr); err == nil {
			r.ingest = ts
		}
		collected = append(collected, r)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return afterSeq, false, fmt.Errorf("store: replay: %w", err)
	}
	_ = rows.Close()
	last := afterSeq
	for _, r := range collected {
		if err := fn(r.seq, r.msg, r.ingest); err != nil {
			return last, false, err
		}
		last = r.seq
	}
	return last, len(collected) == limit, nil
}

// DeleteSpoolThrough removes spooled messages with seq <= through (retention
// sweep; idx_spool_pipeline covers the range scan).
func (s *SQLite) DeleteSpoolThrough(pipeline string, through int64) (int64, error) {
	req := newWriteReq(writeDeleteSpoolThrough)
	req.pipeline, req.through = pipeline, through
	if err := s.w.do(req, "spool retention"); err != nil {
		return 0, err
	}
	return req.n, nil
}

// deleteSpoolThrough runs the retention delete inside the writer's
// transaction.
func deleteSpoolThrough(ctx context.Context, tx *sql.Tx, pipeline string, through int64) (int64, error) {
	res, err := tx.ExecContext(ctx, `DELETE FROM spool WHERE pipeline = ? AND seq <= ?`, pipeline, through)
	if err != nil {
		return 0, fmt.Errorf("spool retention: %w", err)
	}
	return res.RowsAffected()
}

func (s *SQLite) SetCheckpoint(pipeline string, seq int64) error {
	req := newWriteReq(writeCheckpoint)
	req.pipeline, req.cpSeq = pipeline, seq
	return s.w.do(req, "checkpoint")
}

func (s *SQLite) Checkpoint(pipeline string) (int64, error) {
	var seq int64
	err := s.db.QueryRow(`SELECT spool_seq FROM checkpoint WHERE pipeline = ?`, pipeline).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: read checkpoint: %w", err)
	}
	return seq, nil
}

func (s *SQLite) SetSourceState(pipeline, source string, state []byte, srcSeq int64) error {
	req := newWriteReq(writeSourceState)
	req.pipeline, req.source, req.state, req.srcSeq = pipeline, source, state, srcSeq
	return s.w.do(req, "source state")
}

func (s *SQLite) SourceState(pipeline, source string) ([]byte, int64, error) {
	var (
		state  []byte
		srcSeq int64
	)
	err := s.db.QueryRow(`SELECT state, src_seq FROM source_state WHERE pipeline = ? AND source = ?`,
		pipeline, source).Scan(&state, &srcSeq)
	if err == sql.ErrNoRows {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("store: read source state: %w", err)
	}
	return state, srcSeq, nil
}

func (s *SQLite) WriteDeadLetter(dl DeadLetter) error {
	// Stamp at enqueue time: memStore stamps at call time too, so the
	// observable CreatedAt ordering is the caller's.
	if dl.CreatedAt.IsZero() {
		dl.CreatedAt = time.Now()
	}
	req := newWriteReq(writeDeadLetter)
	req.dl = dl
	return s.w.do(req, "dead letter")
}

// insertDeadLetter writes one dead letter inside the writer's transaction.
func insertDeadLetter(ctx context.Context, tx *sql.Tx, dl DeadLetter) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO dead_letter
		   (pipeline, message_id, job_run_id, node, edge, reason, class, backtrace, raw, codec, meta, cursor, src_name, src_seq, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dl.Pipeline, dl.MessageID, dl.RunID, dl.Node, dl.Edge, dl.Reason, dl.Class, dl.Backtrace,
		dl.Raw, dl.Codec, string(marshalMeta(dl.Meta)), dl.Cursor, dl.SrcName, dl.SrcSeq,
		dl.CreatedAt.UTC().Format(timeLayout))
	if err != nil {
		return fmt.Errorf("dead letter: %w", err)
	}
	return nil
}

// dlqColumns excludes the deprecated origin_node/retry_count columns, which
// only exist (empty) in pre-review-2026-09 databases.
const dlqColumns = `id, pipeline, message_id, job_run_id, node, edge, reason, class, backtrace, raw, codec, meta, cursor, src_name, src_seq, created_at`

func (s *SQLite) scanDeadLetters(query string, args ...any) ([]DeadLetter, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []DeadLetter
	for rows.Next() {
		var (
			dl      DeadLetter
			meta    string
			created string
		)
		if err := rows.Scan(&dl.ID, &dl.Pipeline, &dl.MessageID, &dl.RunID, &dl.Node, &dl.Edge, &dl.Reason, &dl.Class, &dl.Backtrace,
			&dl.Raw, &dl.Codec, &meta, &dl.Cursor, &dl.SrcName, &dl.SrcSeq, &created); err != nil {
			return nil, fmt.Errorf("store: scan dead letter: %w", err)
		}
		dl.Meta = unmarshalMeta([]byte(meta))
		if ts, err := time.Parse(time.RFC3339Nano, created); err == nil {
			dl.CreatedAt = ts
		}
		out = append(out, dl)
	}
	return out, rows.Err()
}

func (s *SQLite) DeadLetters(pipeline string) ([]DeadLetter, error) {
	return s.scanDeadLetters(
		`SELECT `+dlqColumns+` FROM dead_letter WHERE pipeline = ? ORDER BY id DESC`, pipeline)
}

func (s *SQLite) DeadLettersSince(pipeline string, since time.Time) ([]DeadLetter, error) {
	if since.IsZero() {
		return s.DeadLetters(pipeline)
	}
	return s.scanDeadLetters(
		`SELECT `+dlqColumns+` FROM dead_letter WHERE pipeline = ? AND created_at >= ? ORDER BY id DESC`,
		pipeline, since.UTC().Format(timeLayout))
}

func (s *SQLite) DeadLettersForRun(pipeline, runID string) ([]DeadLetter, error) {
	return s.scanDeadLetters(
		`SELECT `+dlqColumns+` FROM dead_letter WHERE pipeline = ? AND job_run_id = ? ORDER BY id DESC`,
		pipeline, runID)
}

func (s *SQLite) DeleteDeadLetters(pipeline string, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	req := newWriteReq(writeDeleteDeadLetters)
	req.pipeline, req.ids = pipeline, ids
	if err := s.w.do(req, "delete dead letters"); err != nil {
		return 0, err
	}
	return req.n, nil
}

// deleteDeadLetters removes the listed dead letters inside the writer's
// transaction. small fixed batches only (callers reinject bounded sets).
func deleteDeadLetters(ctx context.Context, tx *sql.Tx, pipeline string, ids []int64) (int64, error) {
	args := make([]any, 0, len(ids)+1)
	args = append(args, pipeline)
	placeholders := ""
	for i, id := range ids {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args = append(args, id)
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM dead_letter WHERE pipeline = ? AND id IN (`+placeholders+`)`, args...)
	if err != nil {
		return 0, fmt.Errorf("delete dead letters: %w", err)
	}
	return res.RowsAffected()
}

// dlqRetentionBatch bounds one DELETE of the dlq.retention sweep: a first
// sweep over a large backlog must stay a series of small fixed transactions,
// not one giant one ("small fixed batches only", as in DeleteDeadLetters).
const dlqRetentionBatch = 10_000

// DeleteDeadLettersBefore removes the pipeline's dead letters created before
// cutoff (dlq.retention sweep), batch after batch until none remain, and
// returns the total. created_at is fixed-width RFC3339 UTC text compared
// lexicographically (timeLayout) — the text order equals the time order, the
// same comparison the in-memory implementation makes with time.Time.
func (s *SQLite) DeleteDeadLettersBefore(pipeline string, cutoff time.Time) (int64, error) {
	req := newWriteReq(writeDeleteDeadLettersBefore)
	req.pipeline, req.cutoff = pipeline, cutoff
	if err := s.w.do(req, "dlq retention"); err != nil {
		return 0, err
	}
	return req.n, nil
}

// deleteDeadLettersBefore runs the retention sweep inside the writer's
// transaction, batch after batch until none remain.
func deleteDeadLettersBefore(ctx context.Context, tx *sql.Tx, pipeline string, cutoff time.Time) (int64, error) {
	cutoffText := cutoff.UTC().Format(timeLayout)
	var total int64
	for {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM dead_letter WHERE pipeline = ? AND id IN (
			   SELECT id FROM dead_letter WHERE pipeline = ? AND created_at < ? LIMIT ?)`,
			pipeline, pipeline, cutoffText, dlqRetentionBatch)
		if err != nil {
			return total, fmt.Errorf("dlq retention: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("dlq retention: %w", err)
		}
		total += n
		if n < dlqRetentionBatch {
			return total, nil
		}
	}
}

// --- job run history ---

func (s *SQLite) CreateJobRun(jr JobRun) error {
	req := newWriteReq(writeCreateJobRun)
	req.jr = jr
	return s.w.do(req, "job run "+jr.RunID)
}

func (s *SQLite) UpdateJobRun(jr JobRun) error {
	req := newWriteReq(writeUpdateJobRun)
	req.jr = jr
	return s.w.do(req, "job run "+jr.RunID)
}

// upsertJobRun runs the insert/update inside the writer's transaction.
func upsertJobRun(ctx context.Context, tx *sql.Tx, jr JobRun, insert bool) error {
	now := time.Now().UTC().Format(timeLayout)
	params, err := marshalParams(jr.Parameters)
	if err != nil {
		return fmt.Errorf("job run %s: %w", jr.RunID, err)
	}
	var res sql.Result
	var err2 error
	if insert {
		res, err2 = tx.ExecContext(ctx,
			`INSERT INTO job_run
			   (run_id, pipeline, status, trigger_type, parameters, scheduled_for, started_at, ended_at, rows_read, delivered, dead_lettered, error, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			jr.RunID, jr.Pipeline, jr.Status, jr.TriggerType, params, jr.ScheduledFor,
			fmtTime(jr.StartedAt), fmtTime(jr.EndedAt), jr.RowsRead, jr.Delivered, jr.DeadLettered, jr.Error, now)
	} else {
		res, err2 = tx.ExecContext(ctx,
			`UPDATE job_run SET status = ?, trigger_type = ?, parameters = ?, scheduled_for = ?, started_at = ?, ended_at = ?,
			   rows_read = ?, delivered = ?, dead_lettered = ?, error = ?, updated_at = ?
			 WHERE run_id = ? AND pipeline = ?`,
			jr.Status, jr.TriggerType, params, jr.ScheduledFor,
			fmtTime(jr.StartedAt), fmtTime(jr.EndedAt), jr.RowsRead, jr.Delivered, jr.DeadLettered, jr.Error, now,
			jr.RunID, jr.Pipeline)
	}
	if err2 != nil {
		return fmt.Errorf("job run %s: %w", jr.RunID, err2)
	}
	if !insert {
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("store: job run %q not found", jr.RunID)
		}
	}
	return nil
}

// fmtTime formats a persisted job-run timestamp; a zero time becomes the
// empty string so the NOT NULL DEFAULT columns stay meaningful (an empty
// ended_at means not finished).
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeLayout)
}

func marshalParams(m map[string]any) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

const jobRunColumns = `run_id, pipeline, status, trigger_type, parameters, scheduled_for, started_at, ended_at, rows_read, delivered, dead_lettered, error, updated_at`

func scanJobRun(rows *sql.Rows) (*JobRun, error) {
	var (
		jr      JobRun
		params  string
		started string
		ended   string
		updated string
	)
	if err := rows.Scan(&jr.RunID, &jr.Pipeline, &jr.Status, &jr.TriggerType, &params, &jr.ScheduledFor,
		&started, &ended, &jr.RowsRead, &jr.Delivered, &jr.DeadLettered, &jr.Error, &updated); err != nil {
		return nil, err
	}
	if len(params) > 0 {
		_ = json.Unmarshal([]byte(params), &jr.Parameters)
	}
	if jr.Parameters == nil {
		jr.Parameters = map[string]any{}
	}
	jr.StartedAt = parseTime(started)
	jr.EndedAt = parseTime(ended)
	jr.UpdatedAt = parseTime(updated)
	return &jr, nil
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return ts
	}
	return time.Time{}
}

func (s *SQLite) GetJobRun(pipeline, runID string) (*JobRun, error) {
	rows, err := s.db.Query(`SELECT `+jobRunColumns+` FROM job_run WHERE pipeline = ? AND run_id = ?`, pipeline, runID)
	if err != nil {
		return nil, fmt.Errorf("store: get job run: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return nil, fmt.Errorf("store: job run %q not found", runID)
	}
	return scanJobRun(rows)
}

func (s *SQLite) JobRuns(pipeline string, limit int) ([]JobRun, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT `+jobRunColumns+` FROM job_run WHERE pipeline = ? ORDER BY started_at DESC, run_id DESC LIMIT ?`, pipeline, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list job runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []JobRun
	for rows.Next() {
		jr, err := scanJobRun(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan job run: %w", err)
		}
		out = append(out, *jr)
	}
	return out, rows.Err()
}

func (s *SQLite) RunnableJobRuns(pipeline string) ([]JobRun, error) {
	// The SQL set mirrors store.IsRunnableStatus literally ('committing' was
	// deleted in candidate 08 — the reserved state had no writer).
	rows, err := s.db.Query(
		`SELECT `+jobRunColumns+` FROM job_run WHERE pipeline = ? AND status IN ('pending','running') ORDER BY started_at`, pipeline)
	if err != nil {
		return nil, fmt.Errorf("store: runnable job runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []JobRun
	for rows.Next() {
		jr, err := scanJobRun(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan job run: %w", err)
		}
		out = append(out, *jr)
	}
	return out, rows.Err()
}

func (s *SQLite) HasSuccessfulRunFor(pipeline, scheduledFor string) (bool, error) {
	var one int
	err := s.db.QueryRow(
		`SELECT 1 FROM job_run WHERE pipeline = ? AND scheduled_for = ? AND status = 'success' LIMIT 1`,
		pipeline, scheduledFor).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: skip_if_successful check: %w", err)
	}
	return true, nil
}

func (s *SQLite) LastScheduledFor(pipeline string) (string, error) {
	var last string
	err := s.db.QueryRow(
		`SELECT COALESCE(MAX(scheduled_for), '') FROM job_run WHERE pipeline = ? AND scheduled_for != ''`, pipeline).Scan(&last)
	if err != nil {
		return "", fmt.Errorf("store: last scheduled_for: %w", err)
	}
	return last, nil
}

func (s *SQLite) DeleteJobRunsBefore(pipeline string, cutoff time.Time) (int64, error) {
	req := newWriteReq(writeDeleteJobRunsBefore)
	req.pipeline, req.cutoff = pipeline, cutoff
	if err := s.w.do(req, "job run retention"); err != nil {
		return 0, err
	}
	return req.n, nil
}

// deleteJobRunsBefore prunes finished runs inside the writer's transaction.
func deleteJobRunsBefore(ctx context.Context, tx *sql.Tx, pipeline string, cutoff time.Time) (int64, error) {
	res, err := tx.ExecContext(ctx,
		`DELETE FROM job_run WHERE pipeline = ? AND ended_at != '' AND ended_at < ? AND status NOT IN ('pending','running')`,
		pipeline, cutoff.UTC().Format(timeLayout))
	if err != nil {
		return 0, fmt.Errorf("job run retention: %w", err)
	}
	return res.RowsAffected()
}

// Close stops the writer and closes the database. Every write that has not
// committed by the time Close begins is refused with ErrClosed: requests
// still queued are resolved by shutdown, and a transaction in flight when
// Close wins the race is rolled back rather than committed, so each caller
// gets a definite error (and a refusal is always safe to re-emit). Close is
// idempotent and waits for the writer goroutine, so no goroutine is left
// behind.
func (s *SQLite) Close() error {
	s.closeOnce.Do(func() {
		s.w.shutdown(ErrClosed)
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

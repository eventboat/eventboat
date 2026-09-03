package store

import (
	"database/sql"
	"fmt"
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

CREATE TABLE IF NOT EXISTS dead_letter (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline    TEXT    NOT NULL,
  message_id  TEXT    NOT NULL,
  node        TEXT    NOT NULL,
  edge        TEXT    NOT NULL DEFAULT '',
  reason      TEXT    NOT NULL,
  backtrace   TEXT    NOT NULL DEFAULT '',
  origin_node TEXT    NOT NULL DEFAULT '',
  raw         BLOB    NOT NULL,
  codec       TEXT    NOT NULL DEFAULT '',
  meta        TEXT    NOT NULL DEFAULT '{}',
  cursor      TEXT    NOT NULL DEFAULT '',
  src_name    TEXT    NOT NULL DEFAULT '',
  src_seq     INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_time ON dead_letter(pipeline, id);
`

// SQLite is the default durable store: one file, WAL mode, pure-Go driver.
type SQLite struct {
	db *sql.DB
}

// OpenSQLite opens (creating if needed) the SQLite database at path with WAL
// journaling and a busy timeout.
func OpenSQLite(path string) (*SQLite, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite: %w", err)
	}
	// modernc sqlite is happiest with a single connection; the engine's store
	// access is modest and serialized in practice.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return &SQLite{db: db}, nil
}

func (s *SQLite) AppendSpool(pipeline string, msg registry.Message, ingestTime time.Time) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO spool (pipeline, message_id, codec, raw, meta, cursor, src_name, src_seq, ingest_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pipeline, msg.ID, msg.Codec, msg.Raw, string(marshalMeta(msg.Meta)), msg.Cursor, msg.SrcName, msg.SrcSeq,
		ingestTime.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("store: append spool: %w", err)
	}
	return res.LastInsertId()
}

func (s *SQLite) ReplayFrom(pipeline string, afterSeq int64, fn func(int64, registry.Message, time.Time) error) error {
	rows, err := s.db.Query(
		`SELECT seq, message_id, codec, raw, meta, cursor, src_name, src_seq, ingest_time
		 FROM spool WHERE pipeline = ? AND seq > ? ORDER BY seq`, pipeline, afterSeq)
	if err != nil {
		return fmt.Errorf("store: replay: %w", err)
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
			rows.Close()
			return fmt.Errorf("store: replay scan: %w", err)
		}
		r.msg.Meta = unmarshalMeta([]byte(meta))
		if ts, err := time.Parse(time.RFC3339Nano, ingestStr); err == nil {
			r.ingest = ts
		}
		collected = append(collected, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("store: replay: %w", err)
	}
	rows.Close()
	// Callbacks run only after the query connection is released: replay
	// dispatch can itself write (checkpoints, dead letters) on this
	// single-connection store and would otherwise deadlock.
	for _, r := range collected {
		if err := fn(r.seq, r.msg, r.ingest); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLite) SetCheckpoint(pipeline string, seq int64) error {
	_, err := s.db.Exec(
		`INSERT INTO checkpoint (pipeline, spool_seq, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT(pipeline) DO UPDATE SET spool_seq = excluded.spool_seq, updated_at = excluded.updated_at`,
		pipeline, seq, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("store: checkpoint: %w", err)
	}
	return nil
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
	_, err := s.db.Exec(
		`INSERT INTO source_state (pipeline, source, state, src_seq, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(pipeline, source) DO UPDATE SET state = excluded.state, src_seq = excluded.src_seq, updated_at = excluded.updated_at`,
		pipeline, source, state, srcSeq, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("store: source state: %w", err)
	}
	return nil
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
	_, err := s.db.Exec(
		`INSERT INTO dead_letter
		   (pipeline, message_id, node, edge, reason, backtrace, origin_node, raw, codec, meta, cursor, src_name, src_seq, retry_count, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dl.Pipeline, dl.MessageID, dl.Node, dl.Edge, dl.Reason, dl.Backtrace, dl.OriginNode,
		dl.Raw, dl.Codec, string(marshalMeta(dl.Meta)), dl.Cursor, dl.SrcName, dl.SrcSeq, dl.RetryCount,
		time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("store: dead letter: %w", err)
	}
	return nil
}

func (s *SQLite) DeadLetters(pipeline string) ([]DeadLetter, error) {
	rows, err := s.db.Query(
		`SELECT id, pipeline, message_id, node, edge, reason, backtrace, origin_node, raw, codec, meta, cursor, src_name, src_seq, retry_count, created_at
		 FROM dead_letter WHERE pipeline = ? ORDER BY id DESC`, pipeline)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: %w", err)
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var (
			dl      DeadLetter
			meta    string
			created string
		)
		if err := rows.Scan(&dl.ID, &dl.Pipeline, &dl.MessageID, &dl.Node, &dl.Edge, &dl.Reason, &dl.Backtrace,
			&dl.OriginNode, &dl.Raw, &dl.Codec, &meta, &dl.Cursor, &dl.SrcName, &dl.SrcSeq, &dl.RetryCount, &created); err != nil {
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

func (s *SQLite) Close() error { return s.db.Close() }

package builtin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/eventboat/eventboat/internal/registry"
)

type sqlCursorConfig struct {
	Column string `json:"column" schema:"optional,minLen=1"`
}

type sqlPaginationConfig struct {
	Key      []string `json:"key" schema:"optional,minItems=1"`
	PageSize int      `json:"page_size" schema:"min=1,default=1000"`
}

type sqlSourceConfig struct {
	Driver     string              `json:"driver" schema:"enum=mysql|postgres|sqlite,desc=database driver (all pure Go)"`
	DSN        string              `json:"dsn" schema:"minLen=1,desc=data source name"`
	Query      string              `json:"query" schema:"minLen=1,desc=base query; :name placeholders bind args; key columns must be selected"`
	Args       map[string]any      `json:"args" schema:"optional,desc=named argument bindings; values may reference ${parameters.x}"`
	Cursor     sqlCursorConfig     `json:"cursor" schema:"optional,desc=watermark column for from: cursor resume"`
	Pagination sqlPaginationConfig `json:"pagination" schema:"optional,desc=keyset pagination key columns and page size"`
	Emit       string              `json:"emit" schema:"enum=row|page,default=row,desc=one message per row, or one per page (array payload for transform.split)"`
}

func registerSQLSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "sql", 1, []string{"pull"}, func(c sqlSourceConfig) (registry.Source, error) {
		if strings.TrimSpace(c.Query) == "" {
			return nil, fmt.Errorf("sql source: query is required")
		}
		key := c.Pagination.Key
		for _, k := range key {
			if k == "" {
				return nil, fmt.Errorf("sql source: pagination.key columns must be non-empty")
			}
		}
		cursorCol := c.Cursor.Column
		if cursorCol == "" && len(key) == 0 {
			return nil, fmt.Errorf("sql source: cursor.column or pagination.key is required (the source needs a resume watermark)")
		}
		if len(key) == 0 {
			key = []string{cursorCol}
		}
		return &sqlSource{
			driver: c.Driver, dsn: c.DSN, query: c.Query, args: c.Args,
			cursor: cursorCol, key: key, pageSize: c.Pagination.PageSize, emit: c.Emit,
			pending: map[int64]pendingRow{},
		}, nil
	})
}

type pendingRow struct {
	cursor string
	keys   []any
}

// sqlSource pulls rows over database/sql with keyset pagination
// (redesign-v3.md §5.8, M2 review R8). Commit state is {watermark, last_key}:
// Init restores it (resume), Commit advances it to the contiguous committed
// frontier (invariant 7 — the watermark never exceeds committed rows).
//
// Concurrency: Pull runs in the source goroutine while the engine calls
// Commit from the commit path; shared fields are guarded by mu. Pagination
// tracks the last PULLED key locally; only the COMMITTED key is persisted.
type sqlSource struct {
	driver   string
	dsn      string
	query    string
	args     map[string]any
	cursor   string // watermark column ("" = use key[0])
	key      []string
	pageSize int
	emit     string

	mu        sync.Mutex
	watermark string // max committed cursor value ("" = start)
	lastKey   []any  // key values of the committed frontier row
	pending   map[int64]pendingRow
	nextSeq   int64
}

type pullState struct {
	Watermark string `json:"watermark"`
	LastKey   []any  `json:"last_key,omitempty"`
}

func (s *sqlSource) Init(state []byte) error {
	if len(state) == 0 {
		return nil
	}
	var st pullState
	if err := json.Unmarshal(state, &st); err != nil {
		return fmt.Errorf("sql source: bad state: %w", err)
	}
	s.mu.Lock()
	s.watermark = st.Watermark
	s.lastKey = st.LastKey
	s.mu.Unlock()
	return nil
}

// Watermark returns the committed cursor watermark (jobs runner binds it to
// ${parameters.from} when the declared value is the sentinel "cursor").
func (s *sqlSource) Watermark() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark
}

// Pull pages through the query until exhaustion (nil) or failure. Backpressure
// comes for free: emit blocks in the engine's admission gate, pausing
// pagination between pages (§5.8 semantics point 4).
func (s *sqlSource) Pull(ctx context.Context, emit func(registry.Message)) error {
	db, err := sql.Open(driverName(s.driver), s.dsn)
	if err != nil {
		return fmt.Errorf("sql source: open: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Page statements are built with synthetic :__eb_* placeholders so the
	// rewriter assigns driver numbering consistently with the user's :args
	// (text order: user args, then key values, then the page limit).
	augmented := make(map[string]any, len(s.args)+len(s.key)+1)
	for k, v := range s.args {
		augmented[k] = v
	}
	for i := range s.key {
		augmented[fmt.Sprintf("__eb_key_%d", i)] = nil
	}
	augmented["__eb_limit"] = nil
	firstSQL, firstOrder, err := rewriteNamedArgs(s.driver, firstPageQuery(s.driver, s.query, s.key), augmented)
	if err != nil {
		return err
	}
	nextSQL, nextOrder, err := rewriteNamedArgs(s.driver, nextPageQuery(s.driver, s.query, s.key), augmented)
	if err != nil {
		return err
	}

	s.mu.Lock()
	resumeKey := s.lastKey
	s.nextSeq = 0
	s.pending = map[int64]pendingRow{}
	s.mu.Unlock()

	pageKeys := resumeKey // nil = from the beginning of the range
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rows *sql.Rows
		if pageKeys == nil {
			vals := make([]any, len(firstOrder))
			for i, name := range firstOrder {
				vals[i] = argValueAt(name, s.args, nil, s.pageSize)
			}
			rows, err = db.QueryContext(ctx, firstSQL, vals...)
		} else {
			vals := make([]any, len(nextOrder))
			for i, name := range nextOrder {
				vals[i] = argValueAt(name, s.args, pageKeys, s.pageSize)
			}
			rows, err = db.QueryContext(ctx, nextSQL, vals...)
		}
		if err != nil {
			return fmt.Errorf("sql source: query page: %w", err)
		}
		count := 0
		var pagePayload []map[string]any
		var pageCursor string
		var lastPulled []any
		for rows.Next() {
			row, rerr := scanRow(rows)
			if rerr != nil {
				_ = rows.Close()
				return fmt.Errorf("sql source: scan: %w", rerr)
			}
			count++
			cur := cursorString(row[s.cursorCol()])
			keys := keyValues(row, s.key)
			lastPulled = keys
			if s.emit == "row" {
				s.emitRow(emit, row, cur, keys)
			} else {
				pagePayload = append(pagePayload, row)
				pageCursor = cur
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("sql source: rows: %w", err)
		}
		_ = rows.Close()
		if s.emit == "page" && count > 0 {
			s.mu.Lock()
			s.nextSeq++
			seq := s.nextSeq
			s.mu.Unlock()
			raw, merr := json.Marshal(pagePayload)
			if merr != nil {
				return fmt.Errorf("sql source: marshal page: %w", merr)
			}
			emit(registry.Message{Raw: raw, Codec: "json", SrcName: "sql", SrcSeq: seq, Cursor: pageCursor})
			s.mu.Lock()
			s.pending[seq] = pendingRow{cursor: pageCursor, keys: lastPulled}
			s.mu.Unlock()
		}
		if count < s.pageSize {
			return nil // exhausted
		}
		pageKeys = lastPulled
	}
}

func (s *sqlSource) emitRow(emit func(registry.Message), row map[string]any, cur string, keys []any) {
	s.mu.Lock()
	s.nextSeq++
	seq := s.nextSeq
	s.mu.Unlock()
	raw, _ := json.Marshal(row)
	emit(registry.Message{Raw: raw, Codec: "json", SrcName: "sql", SrcSeq: seq, Cursor: cur})
	s.mu.Lock()
	s.pending[seq] = pendingRow{cursor: cur, keys: keys}
	s.mu.Unlock()
}

// Commit advances the watermark to the contiguous committed frontier
// (invariant 7). Rows are emitted in key order, so the frontier row is the
// emission with the highest srcSeq within the committed prefix — its cursor
// IS the watermark (a string-max would misorder numeric cursors: "9" > "24").
func (s *sqlSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxSeq int64
	var frontier pendingRow
	for seq := int64(1); seq <= throughSrcSeq; seq++ {
		if p, ok := s.pending[seq]; ok {
			if seq > maxSeq {
				maxSeq = seq
				frontier = p
			}
			delete(s.pending, seq)
		}
	}
	if maxSeq > 0 {
		s.watermark = frontier.cursor
		s.lastKey = frontier.keys
	}
	st, _ := json.Marshal(pullState{Watermark: s.watermark, LastKey: s.lastKey})
	return st, nil
}

// Run is the continuous-mode fallback (lint-warned): one pull at startup,
// then idle until the engine stops.
func (s *sqlSource) Run(ctx context.Context, emit func(registry.Message)) {
	_ = s.Pull(ctx, emit)
	<-ctx.Done()
}

func (s *sqlSource) Close() error { return nil }

func (s *sqlSource) cursorCol() string {
	if s.cursor != "" {
		return s.cursor
	}
	return s.key[0]
}

func driverName(driver string) string {
	switch driver {
	case "mysql":
		return "mysql"
	case "postgres":
		return "pgx"
	default:
		return "sqlite"
	}
}

// firstPageQuery wraps the user query for the first page: order by key,
// limit — no key comparison (the range itself comes from the user's :from /
// :to bindings). The first page of a RESUMED pull uses nextPageQuery with
// the restored key. Placeholders are synthetic :__eb_* names; the rewriter
// assigns driver numbering.
func firstPageQuery(dialect, query string, key []string) string {
	return fmt.Sprintf("SELECT * FROM (\n%s\n) AS _eb_page ORDER BY %s LIMIT :__eb_limit",
		query, strings.Join(quoteCols(dialect, key), ", "))
}

// nextPageQuery wraps the user query with the keyset comparison for every
// page after the first.
func nextPageQuery(dialect, query string, key []string) string {
	placeholders := make([]string, len(key))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":__eb_key_%d", i)
	}
	return fmt.Sprintf("SELECT * FROM (\n%s\n) AS _eb_page WHERE (%s) > (%s) ORDER BY %s LIMIT :__eb_limit",
		query,
		strings.Join(quoteCols(dialect, key), ", "),
		strings.Join(placeholders, ", "),
		strings.Join(quoteCols(dialect, key), ", "))
}

// scanRow reads one row as a column-name map with normalized values
// ([]byte → string, time.Time → RFC3339) so JSON encoding is stable.
func scanRow(rows *sql.Rows) (map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(cols))
	for i, c := range cols {
		out[c] = normalizeDBValue(raw[i])
	}
	return out, nil
}

func normalizeDBValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

func cursorString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func keyValues(row map[string]any, key []string) []any {
	out := make([]any, len(key))
	for i, k := range key {
		out[i] = row[k]
	}
	return out
}

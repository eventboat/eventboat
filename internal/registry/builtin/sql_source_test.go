package builtin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/eventboat/eventboat/internal/registry"
)

func openTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY, order_no TEXT, amount REAL, region TEXT, updated_at TEXT)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func insertOrders(t *testing.T, db *sql.DB, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		updated := fmt.Sprintf("2026-09-%02dT%02d:00:00Z", 1+i/24, i%24)
		if _, err := db.Exec(`INSERT INTO orders (id, order_no, amount, region, updated_at) VALUES (?, ?, ?, ?, ?)`,
			i, fmt.Sprintf("ord-%04d", i), float64(i)*1.5, "eu", updated); err != nil {
			t.Fatal(err)
		}
	}
}

func newSQLSource(t *testing.T, cfg map[string]any) registry.PullSource {
	t.Helper()
	reg := registry.New()
	if err := registerSQLSource(reg); err != nil {
		t.Fatal(err)
	}
	src, err := reg.NewSource("sql", cfg)
	if err != nil {
		t.Fatal(err)
	}
	ps, ok := src.(registry.PullSource)
	if !ok {
		t.Fatal("sql source does not implement PullSource")
	}
	return ps
}

// Real sqlite round trip: keyset pagination, row emission, watermark commit.
func TestSQLSourcePullPagesAndCommits(t *testing.T) {
	path := t.TempDir() + "/orders.db"
	db := openTestDB(t, path)
	insertOrders(t, db, 0, 25) // 25 rows, page size 10 → 3 pages

	src := newSQLSource(t, map[string]any{
		"driver": "sqlite", "dsn": "file:" + path,
		"query":      "SELECT id, order_no, amount FROM orders WHERE updated_at >= :from AND updated_at < :to",
		"args":       map[string]any{"from": "2026-09-01", "to": "2026-09-10"},
		"cursor":     map[string]any{"column": "id"},
		"pagination": map[string]any{"key": []any{"id"}, "page_size": 10},
	})

	var got []map[string]any
	err := src.Pull(context.Background(), func(m registry.Message) {
		var row map[string]any
		if err := json.Unmarshal(m.Raw, &row); err != nil {
			t.Error(err)
		}
		got = append(got, row)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 25 {
		t.Fatalf("pulled %d rows, want 25", len(got))
	}
	for i, row := range got {
		if row["id"].(float64) != float64(i) {
			t.Fatalf("row %d out of order: %v", i, row)
		}
	}

	// Settled at the full frontier commits the watermark.
	state, err := src.Settled(context.Background(), 25)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), `"watermark":"24"`) {
		t.Errorf("watermark state = %s", state)
	}
}

// Resume: a restored key pulls only rows after the settled frontier.
func TestSQLSourceResumesFromState(t *testing.T) {
	path := t.TempDir() + "/orders.db"
	db := openTestDB(t, path)
	insertOrders(t, db, 0, 20)

	src := newSQLSource(t, map[string]any{
		"driver": "sqlite", "dsn": "file:" + path,
		"query":      "SELECT id FROM orders",
		"cursor":     map[string]any{"column": "id"},
		"pagination": map[string]any{"key": []any{"id"}, "page_size": 100},
	})
	// Pretend rows 0..9 were pulled and settled in a previous life.
	if err := src.Init([]byte(`{"watermark":"9","last_key":[9]}`)); err != nil {
		t.Fatal(err)
	}
	var ids []float64
	err := src.Pull(context.Background(), func(m registry.Message) {
		var row map[string]any
		_ = json.Unmarshal(m.Raw, &row)
		ids = append(ids, row["id"].(float64))
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 10 || ids[0] != 10 || ids[9] != 19 {
		t.Fatalf("resumed ids = %v, want 10..19", ids)
	}
}

// emit: page produces one array-payload message per page (for split).
func TestSQLSourceEmitPage(t *testing.T) {
	path := t.TempDir() + "/orders.db"
	db := openTestDB(t, path)
	insertOrders(t, db, 0, 12)

	src := newSQLSource(t, map[string]any{
		"driver": "sqlite", "dsn": "file:" + path,
		"query":      "SELECT id FROM orders",
		"cursor":     map[string]any{"column": "id"},
		"pagination": map[string]any{"key": []any{"id"}, "page_size": 5},
		"emit":       "page",
	})
	var pages int
	err := src.Pull(context.Background(), func(m registry.Message) {
		pages++
		var rows []map[string]any
		if err := json.Unmarshal(m.Raw, &rows); err != nil {
			t.Error(err)
		}
		if pages < 3 && len(rows) != 5 {
			t.Errorf("page %d has %d rows, want 5", pages, len(rows))
		}
		if pages == 3 && len(rows) != 2 {
			t.Errorf("last page has %d rows, want 2", len(rows))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if pages != 3 {
		t.Fatalf("pages = %d, want 3", pages)
	}
}

// Factory validation paths.
func TestSQLSourceFactoryValidation(t *testing.T) {
	reg := registry.New()
	if err := registerSQLSource(reg); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		cfg  map[string]any
		want string
	}{
		{"no driver", map[string]any{"dsn": "x", "query": "SELECT 1"}, "schema validation"},
		{"bad driver", map[string]any{"driver": "oracle", "dsn": "x", "query": "SELECT 1"}, "schema validation"},
		{"no dsn", map[string]any{"driver": "sqlite", "query": "SELECT 1"}, "schema validation"},
		{"no query", map[string]any{"driver": "sqlite", "dsn": "x"}, "schema validation"},
		{"bad emit", map[string]any{"driver": "sqlite", "dsn": "x", "query": "SELECT 1", "cursor": map[string]any{"column": "id"}, "emit": "batch"}, "schema validation"},
		{"unknown field", map[string]any{"driver": "sqlite", "dsn": "x", "query": "SELECT 1", "cursor": map[string]any{"column": "id"}, "pool": 5}, "schema validation"},
	}
	for _, tc := range cases {
		_, err := reg.NewSource("sql", tc.cfg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.name, err, tc.want)
		}
	}

	// Cross-field rule the schema cannot express: a resume watermark
	// (cursor.column or pagination.key) is required.
	_, err := reg.NewSource("sql", map[string]any{"driver": "sqlite", "dsn": "file:x.db", "query": "SELECT 1 AS id"})
	if err == nil || !strings.Contains(err.Error(), "resume watermark") {
		t.Errorf("missing watermark: got %v", err)
	}
}

// Pull failure surfaces as an error (run failed, not silent exhaustion).
func TestSQLSourcePullError(t *testing.T) {
	path := t.TempDir() + "/bad.db"
	src := newSQLSource(t, map[string]any{
		"driver": "sqlite", "dsn": "file:" + path,
		"query":  "SELECT no_such_column FROM nothing",
		"cursor": map[string]any{"column": "id"},
	})
	err := src.Pull(context.Background(), func(registry.Message) {})
	if err == nil {
		t.Fatal("pull over a missing table must fail")
	}
}

// --- named-arg rewriter unit tests (R8 colon traps) ---

func TestRewriteNamedArgs(t *testing.T) {
	cases := []struct {
		name    string
		dialect string
		query   string
		args    map[string]any
		wantSQL string
		wantOrd []string
	}{
		{"mysql positional", "mysql", "WHERE updated_at >= :from AND updated_at < :to",
			map[string]any{"from": "a", "to": "b"},
			"WHERE updated_at >= ? AND updated_at < ?", []string{"from", "to"}},
		{"postgres numbered", "postgres", ":from < updated_at AND :to > updated_at",
			map[string]any{"from": 1, "to": 2},
			"$1 < updated_at AND $2 > updated_at", []string{"from", "to"}},
		{"string literal colon untouched", "mysql", "WHERE t = 'a:b' AND x >= :from",
			map[string]any{"from": 1},
			"WHERE t = 'a:b' AND x >= ?", []string{"from"}},
		{"pg cast untouched", "postgres", "WHERE ts ::text >= :from",
			map[string]any{"from": 1},
			"WHERE ts ::text >= $1", []string{"from"}},
		{"quoted identifier untouched", "mysql", "WHERE `we:ird` = 1 AND x = :v",
			map[string]any{"v": 1},
			"WHERE `we:ird` = 1 AND x = ?", []string{"v"}},
		{"lone colon untouched", "mysql", "VALUES ('12:30', :v)",
			map[string]any{"v": 1},
			"VALUES ('12:30', ?)", []string{"v"}},
		{"synthetic names", "mysql", "SELECT * FROM (q) p WHERE (id) > (:__eb_key_0) LIMIT :__eb_limit",
			map[string]any{"__eb_key_0": 5, "__eb_limit": 10},
			"SELECT * FROM (q) p WHERE (id) > (?) LIMIT ?", []string{"__eb_key_0", "__eb_limit"}},
	}
	for _, tc := range cases {
		gotSQL, gotOrd, err := rewriteNamedArgs(tc.dialect, tc.query, tc.args)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if gotSQL != tc.wantSQL {
			t.Errorf("%s: sql = %q, want %q", tc.name, gotSQL, tc.wantSQL)
		}
		if strings.Join(gotOrd, ",") != strings.Join(tc.wantOrd, ",") {
			t.Errorf("%s: order = %v, want %v", tc.name, gotOrd, tc.wantOrd)
		}
	}

	// Unbound name is an error.
	if _, _, err := rewriteNamedArgs("mysql", "WHERE x = :missing", map[string]any{}); err == nil {
		t.Error("unbound :missing accepted")
	}
}

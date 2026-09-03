// Command seed regenerates the example's sqlite source database
// (examples/job-sync/data/orders.db). Run from the repo root:
//
//	go run ./examples/job-sync/seed
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func main() {
	dir := filepath.Join("examples", "job-sync", "data")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fatal(err)
	}
	path := filepath.Join(dir, "orders.db")
	_ = os.Remove(path)
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		order_no TEXT NOT NULL,
		amount REAL NOT NULL,
		region TEXT,
		updated_at TEXT NOT NULL)`); err != nil {
		fatal(err)
	}
	// Six orders across two days; region NULL on one row so the transform's
	// default kicks in. Timestamps sort lexicographically (text cursors).
	rows := []struct {
		id, orderNo string
		amount      float64
		region      any
		updatedAt   string
	}{
		{"1", "ord-0001", 120.5, "eu", "2026-09-01T00:10:00Z"},
		{"2", "ord-0002", 45.0, "us", "2026-09-01T06:30:00Z"},
		{"3", "ord-0003", 999.99, nil, "2026-09-01T12:00:00Z"},
		{"4", "ord-0004", 12.25, "eu", "2026-09-02T01:15:00Z"},
		{"5", "ord-0005", 310.0, "apac", "2026-09-02T09:00:00Z"},
		{"6", "ord-0006", 78.4, "us", "2026-09-02T15:45:00Z"},
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO orders (id, order_no, amount, region, updated_at) VALUES (?, ?, ?, ?, ?)`,
			r.id, r.orderNo, r.amount, r.region, r.updatedAt); err != nil {
			fatal(err)
		}
	}
	fmt.Printf("seeded %s (%d orders)\n", path, len(rows))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "seed:", err)
	os.Exit(1)
}

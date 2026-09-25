package builtin

import (
	"context"
	"path/filepath"
	"testing"
)

// Truncation detection beyond the size check. The adversarial review found
// that copytruncate is only detected when the rewritten file is still shorter
// than the read offset at the next scan: a rewrite that regrows past it left
// the descriptor mid-file, silently losing the new content's first lines (the
// `size < offset` check cannot see it). The source now fingerprints the last
// consumed bytes (tailFingerprintBytes) and resets to 0 on a mismatch, so the
// documented "copytruncate is handled (duplicates, never loss)" holds for the
// regrown case too. Polls are driven manually here so the intermediate
// truncated state is never observable, exactly like the production race.

// newManualV2 wires a source for manual poll calls (no Run ticker).
func newManualV2(t *testing.T, path string) (*fileSource, *sourceCollector, context.Context) {
	t.Helper()
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	fs.mu.Lock()
	fs.ensureMapsLocked()
	fs.pending = map[int64]filePending{}
	fs.mu.Unlock()
	return fs, &sourceCollector{}, context.Background()
}

func pollNow(t *testing.T, fs *fileSource, c *sourceCollector, ctx context.Context, first bool) {
	t.Helper()
	if _, err := fs.poll(ctx, c.emit, first); err != nil {
		t.Fatal(err)
	}
}

// A copytruncate rewrite that regrows past the old read offset: the size check
// is blind, the tail fingerprint catches it, and both new lines are delivered
// (the old content's phantom is not).
func TestFileSourceTruncationRegrownPastOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "aaaaaaaaaaaaaaaa\nbbbbbbbbbbbbbbbb\n") // 34 bytes, 2 lines
	fs, c, ctx := newManualV2(t, path)
	pollNow(t, fs, c, ctx, true)
	if got := c.count(); got != 2 {
		t.Fatalf("initial count = %d, want 2", got)
	}

	// The rewrite is larger than the old offset (68 > 34): size < offset is
	// false, so only the fingerprint can see the rewrite.
	writeV2File(t, path, "cccccccccccccccccccccccccccccccccc\ndddddddddddddddddddddddddddddddddd\n")
	pollNow(t, fs, c, ctx, false)
	pollNow(t, fs, c, ctx, false)

	want := []string{
		"aaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddddd",
	}
	if got := c.sortedLines(); !sortedEqual(got, want) {
		t.Fatalf("delivered lines = %v, want %v", got, want)
	}
}

// A truncation that lands inside the buffered partial line: the consumed
// extent is offset+partialBytes, and the stale partial must not be
// concatenated with the rewritten content.
func TestFileSourceTruncationIntoPartial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "aaaa\nbb") // 7 bytes: one line + a partial
	fs, c, ctx := newManualV2(t, path)
	pollNow(t, fs, c, ctx, true)
	if got := c.sortedLines(); !sortedEqual(got, []string{"aaaa"}) {
		t.Fatalf("after the first poll = %v, want [aaaa]", got)
	}

	// Rewrite with more bytes than the consumed extent (9 > 7): the partial
	// "bb" is gone from disk, and the fingerprint sees the rewrite.
	writeV2File(t, path, "cccccccc\n")
	pollNow(t, fs, c, ctx, false)

	if got := c.sortedLines(); !sortedEqual(got, []string{"aaaa", "cccccccc"}) {
		t.Fatalf("delivered lines = %v, want [aaaa cccccccc] (the stale partial must not be emitted)", got)
	}
}

// Pure appends never touch consumed bytes: the fingerprint must not
// false-positive into a reset (which would duplicate lines).
func TestFileSourceAppendDoesNotResetFingerprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "aaaa\n")
	fs, c, ctx := newManualV2(t, path)
	pollNow(t, fs, c, ctx, true)

	appendV2File(t, path, "bbbb\n")
	pollNow(t, fs, c, ctx, false)
	appendV2File(t, path, "cccc\n")
	pollNow(t, fs, c, ctx, false)

	if got := c.sortedLines(); !sortedEqual(got, []string{"aaaa", "bbbb", "cccc"}) {
		t.Fatalf("delivered lines = %v, want exactly one copy of each", got)
	}
}

// A rewrite while the descriptor was closed (close_inactive) is caught on
// reopen: the fingerprint survives the close, and the reset happens before
// the first read.
func TestFileSourceReopenAfterRewriteResets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "aaaa\n")
	fs, c, ctx := newManualV2(t, path)
	pollNow(t, fs, c, ctx, true)

	fs.mu.Lock()
	e := fs.files[fs.byPath[path]]
	fs.closeLocked(e)
	fs.mu.Unlock()

	writeV2File(t, path, "cccccccc\ndddd\n")
	pollNow(t, fs, c, ctx, false)

	if got := c.sortedLines(); !sortedEqual(got, []string{"aaaa", "cccccccc", "dddd"}) {
		t.Fatalf("delivered lines = %v, want the rewritten content", got)
	}
}

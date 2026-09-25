package builtin

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// File source v2 scenario matrix at the source level (deterministic; the
// engine-level matrix lives in internal/engine/file_source_v2_test.go). The
// protected pre-v2 tests stay in file_source_test.go, untouched.

func newV2Source(t *testing.T, cfg map[string]any) *fileSource {
	t.Helper()
	reg := registry.New()
	if err := registerFileSource(reg); err != nil {
		t.Fatal(err)
	}
	src, err := reg.NewSource("file", cfg)
	if err != nil {
		t.Fatal(err)
	}
	fs, ok := src.(*fileSource)
	if !ok {
		t.Fatalf("file source factory returned %T, want *fileSource", src)
	}
	fs.warnf = func(string, ...any) {} // keep oversized-line warnings out of test output
	return fs
}

type sourceCollector struct {
	mu   sync.Mutex
	msgs []registry.Message
}

func (c *sourceCollector) emit(m registry.Message) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
	return nil
}

func (c *sourceCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func (c *sourceCollector) lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.msgs))
	for i, m := range c.msgs {
		out[i] = string(m.Raw)
	}
	return out
}

func (c *sourceCollector) sortedLines() []string {
	out := c.lines()
	sort.Strings(out)
	return out
}

func (c *sourceCollector) snapshot() []registry.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]registry.Message(nil), c.msgs...)
}

func waitV2(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func writeV2File(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendV2File(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// startV2Source runs the source in the background; the returned stop cancels
// and joins it, and test cleanup closes the descriptor.
func startV2Source(t *testing.T, fs *fileSource, c *sourceCollector) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fs.Run(ctx, c.emit) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("file source did not stop after cancellation")
			}
			_ = fs.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func sortedEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// Glob discovery plus metadata: every emission carries the path it was read
// from and the configured host.
func TestFileSourceV2GlobMetadataAndHost(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jsonl")
	b := filepath.Join(dir, "b.jsonl")
	writeV2File(t, a, "a1\na2\n")
	writeV2File(t, b, "b1\n")

	fs := newV2Source(t, map[string]any{
		"path":          filepath.ToSlash(filepath.Join(dir, "*.jsonl")),
		"poll_every_ms": 10,
		"host":          "test-host",
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "3 lines from 2 files", func() bool { return c.count() == 3 })
	if got := c.sortedLines(); !sortedEqual(got, []string{"a1", "a2", "b1"}) {
		t.Fatalf("lines = %v", got)
	}
	for _, m := range c.snapshot() {
		want := a
		if strings.HasPrefix(string(m.Raw), "b") {
			want = b
		}
		if m.Meta["file_path"] != want {
			t.Fatalf("message %q file_path = %v, want %v", m.Raw, m.Meta["file_path"], want)
		}
		if m.Meta["host"] != "test-host" {
			t.Fatalf("message %q host = %v, want test-host", m.Raw, m.Meta["host"])
		}
	}
}

// tail mode buffers an unterminated trailing line until its newline arrives.
func TestFileSourceV2HalfLineWaitsForNewline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "hel")
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	time.Sleep(120 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("half line emitted %d messages, want 0", n)
	}
	appendV2File(t, path, "lo\n")
	waitV2(t, "completed line", func() bool { return c.count() == 1 })
	if got := c.lines(); got[0] != "hello" {
		t.Fatalf("line = %q, want hello", got[0])
	}
}

// stop mode is for complete batch files: an unterminated last line is emitted
// as the final message and Run returns exhausted (nil).
func TestFileSourceV2StopEmitsUnterminatedLastLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.jsonl")
	writeV2File(t, path, "a\nb")
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10, "on_eof": "stop"})
	c := &sourceCollector{}
	if err := fs.Run(context.Background(), c.emit); err != nil {
		t.Fatalf("stop Run: %v", err)
	}
	_ = fs.Close()
	if got := c.sortedLines(); !sortedEqual(got, []string{"a", "b"}) {
		t.Fatalf("lines = %v, want [a b]", got)
	}
}

// stop + glob: zero matches on the first scan is a loud error.
func TestFileSourceV2StopGlobNoMatchErrors(t *testing.T) {
	fs := newV2Source(t, map[string]any{
		"path":          filepath.ToSlash(filepath.Join(t.TempDir(), "*.jsonl")),
		"poll_every_ms": 10,
		"on_eof":        "stop",
	})
	c := &sourceCollector{}
	err := fs.Run(context.Background(), c.emit)
	if err == nil || !strings.Contains(err.Error(), "no files match") {
		t.Fatalf("Run error = %v, want a no-files-match failure", err)
	}
}

// stop + glob: once matches exist and every tracked file is read to its end,
// Run returns nil.
func TestFileSourceV2StopGlobExhausts(t *testing.T) {
	dir := t.TempDir()
	writeV2File(t, filepath.Join(dir, "a.jsonl"), "a1\n")
	writeV2File(t, filepath.Join(dir, "b.jsonl"), "b1\nb2\n")
	fs := newV2Source(t, map[string]any{
		"path":          filepath.ToSlash(filepath.Join(dir, "*.jsonl")),
		"poll_every_ms": 10,
		"on_eof":        "stop",
	})
	c := &sourceCollector{}
	done := make(chan error, 1)
	go func() { done <- fs.Run(context.Background(), c.emit) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop glob source never exhausted")
	}
	_ = fs.Close()
	if got := c.sortedLines(); !sortedEqual(got, []string{"a1", "b1", "b2"}) {
		t.Fatalf("lines = %v", got)
	}
}

// tail + glob: zero matches waits, and a file appearing later is discovered.
func TestFileSourceV2TailGlobWaitsForMatch(t *testing.T) {
	dir := t.TempDir()
	fs := newV2Source(t, map[string]any{
		"path":          filepath.ToSlash(filepath.Join(dir, "*.jsonl")),
		"poll_every_ms": 10,
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	time.Sleep(60 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("emitted %d messages before any file existed", n)
	}
	writeV2File(t, filepath.Join(dir, "late.jsonl"), "late\n")
	waitV2(t, "late file line", func() bool { return c.count() == 1 })
	if got := c.lines(); got[0] != "late" {
		t.Fatalf("line = %q", got[0])
	}
}

// oversize: truncate emits the first max_line_bytes and counts the line.
func TestFileSourceV2OversizeTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "0123456789ABCDEF\nsecond\n")
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "max_line_bytes": 8, "oversize": "truncate",
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "2 lines", func() bool { return c.count() == 2 })
	if got := c.sortedLines(); !sortedEqual(got, []string{"01234567", "second"}) {
		t.Fatalf("lines = %v", got)
	}
	fs.mu.Lock()
	truncated, skipped := fs.oversizeTruncated, fs.oversizeSkipped
	fs.mu.Unlock()
	if truncated != 1 || skipped != 0 {
		t.Fatalf("oversize counters = truncated %d / skipped %d, want 1/0", truncated, skipped)
	}
}

// oversize: skip drops the line, counts it, and still advances past it.
func TestFileSourceV2OversizeSkip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "0123456789ABCDEF\nsecond\n")
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "max_line_bytes": 8, "oversize": "skip",
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "1 line", func() bool { return c.count() == 1 })
	if got := c.lines(); got[0] != "second" {
		t.Fatalf("lines = %v, want [second]", got)
	}
	fs.mu.Lock()
	truncated, skipped := fs.oversizeTruncated, fs.oversizeSkipped
	fs.mu.Unlock()
	if truncated != 0 || skipped != 1 {
		t.Fatalf("oversize counters = truncated %d / skipped %d, want 0/1", truncated, skipped)
	}
}

// The read buffer for one line is capped at max_line_bytes: a megabyte-long
// line must not be held in memory, yet the line is still consumed and the
// truncated prefix emitted.
func TestFileSourceV2OversizeReadBufferIsBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	const lineSize = 1 << 20
	writeV2File(t, path, strings.Repeat("x", lineSize)) // no newline: one huge half-line
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "max_line_bytes": 8, "oversize": "truncate",
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "the whole huge line consumed", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, e := range fs.files {
			return e.partialBytes == lineSize
		}
		return false
	})
	fs.mu.Lock()
	var buffered int
	for _, e := range fs.files {
		buffered = len(e.partial)
	}
	fs.mu.Unlock()
	if buffered != 8 {
		t.Fatalf("buffered %d bytes of a %d-byte line, want the 8-byte cap", buffered, lineSize)
	}

	appendV2File(t, path, "\n")
	waitV2(t, "truncated emission", func() bool { return c.count() == 1 })
	if got := c.lines()[0]; len(got) != 8 {
		t.Fatalf("emitted %d bytes, want 8", len(got))
	}
}

// v1 state: {"offset":N} is applied to the single path's first open, and the
// next Commit already writes the v2 document.
func TestFileSourceV2StateV1Compat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "one\ntwo\n")
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	if err := fs.Init([]byte(`{"offset":4}`)); err != nil {
		t.Fatal(err)
	}
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "the line after the v1 offset", func() bool { return c.count() == 1 })
	if got := c.lines(); got[0] != "two" {
		t.Fatalf("lines = %v, want [two] (v1 offset 4)", got)
	}
	state, err := fs.Commit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Version int                       `json:"version"`
		Files   map[string]fileStateEntry `json:"files"`
	}
	if err := json.Unmarshal(state, &doc); err != nil {
		t.Fatalf("state %s: %v", state, err)
	}
	if doc.Version != 2 || len(doc.Files) != 1 {
		t.Fatalf("state = %s, want one v2 file entry", state)
	}
	for _, e := range doc.Files {
		if e.Offset != 8 || e.Path != path {
			t.Fatalf("file state = %+v, want offset 8 and path %s", e, path)
		}
	}
}

// Crash resume: a source restarted from the v2 state of a partial commit
// re-reads exactly the uncommitted tail (duplicates are allowed, loss is not).
func TestFileSourceV2ResumeFromCommittedOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "one\ntwo\nthree\n")

	fs1 := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	c1 := &sourceCollector{}
	startV2Source(t, fs1, c1)
	waitV2(t, "3 lines", func() bool { return c1.count() == 3 })
	state, err := fs1.Commit(context.Background(), 2) // only "one" and "two" committed
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), `"offset":8`) {
		t.Fatalf("state = %s, want offset 8", state)
	}

	fs2 := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	if err := fs2.Init(state); err != nil {
		t.Fatal(err)
	}
	c2 := &sourceCollector{}
	startV2Source(t, fs2, c2)
	waitV2(t, "the uncommitted tail", func() bool { return c2.count() == 1 })
	if got := c2.lines(); got[0] != "three" {
		t.Fatalf("resumed lines = %v, want [three]", got)
	}
}

// A glob restarts per file: the v2 state carries one offset per file id, and
// a resumed source re-reads neither file's committed bytes.
func TestFileSourceV2ResumePerFile(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jsonl")
	b := filepath.Join(dir, "b.jsonl")
	writeV2File(t, a, "a1\na2\n")
	writeV2File(t, b, "b1\n")

	fs1 := newV2Source(t, map[string]any{
		"path": filepath.ToSlash(filepath.Join(dir, "*.jsonl")), "poll_every_ms": 10,
	})
	c1 := &sourceCollector{}
	stop1 := startV2Source(t, fs1, c1)
	waitV2(t, "3 lines", func() bool { return c1.count() == 3 })
	state, err := fs1.Commit(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Version int                       `json:"version"`
		Files   map[string]fileStateEntry `json:"files"`
	}
	if err := json.Unmarshal(state, &doc); err != nil {
		t.Fatalf("state %s: %v", state, err)
	}
	if doc.Version != 2 || len(doc.Files) != 2 {
		t.Fatalf("state = %s, want two v2 file entries", state)
	}
	stop1()

	fs2 := newV2Source(t, map[string]any{
		"path": filepath.ToSlash(filepath.Join(dir, "*.jsonl")), "poll_every_ms": 10,
	})
	if err := fs2.Init(state); err != nil {
		t.Fatal(err)
	}
	c2 := &sourceCollector{}
	startV2Source(t, fs2, c2)
	time.Sleep(80 * time.Millisecond) // committed bytes must not be re-read
	if n := c2.count(); n != 0 {
		t.Fatalf("resumed source re-read %d committed lines: %v", n, c2.lines())
	}
	appendV2File(t, a, "a3\n")
	appendV2File(t, b, "b2\n")
	waitV2(t, "appended lines", func() bool { return c2.count() == 2 })
	if got := c2.sortedLines(); !sortedEqual(got, []string{"a3", "b2"}) {
		t.Fatalf("resumed lines = %v, want [a3 b2]", got)
	}
}

// ignore_older: an old file is skipped at first discovery; once it grows a
// fresh mtime it is picked up from its beginning.
func TestFileSourceV2IgnoreOlder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "old.jsonl")
	writeV2File(t, path, "old\n")
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "ignore_older_ms": 60_000,
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	time.Sleep(150 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("old file emitted %d messages, want 0", n)
	}
	appendV2File(t, path, "fresh\n")
	waitV2(t, "refreshed file lines", func() bool { return c.count() == 2 })
	if got := c.sortedLines(); !sortedEqual(got, []string{"fresh", "old"}) {
		t.Fatalf("lines = %v", got)
	}
}

// clean_removed (default true): a removed file's state is dropped once it is
// drained and idle.
func TestFileSourceV2CleanRemovedDropsState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "one\ntwo\n")
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "close_inactive_ms": 30,
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "2 lines", func() bool { return c.count() == 2 })
	if _, err := fs.Commit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	waitV2(t, "removed file's state to be pruned", func() bool {
		state, _ := fs.Commit(context.Background(), 2)
		var doc struct {
			Files map[string]fileStateEntry `json:"files"`
		}
		if err := json.Unmarshal(state, &doc); err != nil {
			t.Fatal(err)
		}
		return len(doc.Files) == 0
	})
}

// clean_removed: false keeps the drained file's offset in the state.
func TestFileSourceV2CleanRemovedFalseKeepsState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "one\n")
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "close_inactive_ms": 30, "clean_removed": false,
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "1 line", func() bool { return c.count() == 1 })
	state, _ := fs.Commit(context.Background(), 1)
	if !strings.Contains(string(state), `"offset":4`) {
		t.Fatalf("state = %s, want offset 4", state)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	state, _ = fs.Commit(context.Background(), 1)
	if !strings.Contains(string(state), `"offset":4`) {
		t.Fatalf("state = %s after removal, want the offset kept", state)
	}
}

// close_inactive closes an idle descriptor; the file is reopened and read on
// its next growth.
func TestFileSourceV2CloseInactiveClosesAndReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "one\n")
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "close_inactive_ms": 30,
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "1 line", func() bool { return c.count() == 1 })
	waitV2(t, "the idle descriptor to close", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, e := range fs.files {
			return e.f == nil
		}
		return false
	})
	appendV2File(t, path, "two\n")
	waitV2(t, "the reopened file's new line", func() bool { return c.count() == 2 })
	if got := c.lines(); !sortedEqual([]string{got[0], got[1]}, []string{"one", "two"}) {
		t.Fatalf("lines = %v", got)
	}
}

// Rotation: rename + recreate at the same path. The old descriptor is drained
// to EOF and the new file is read from its start.
func TestFileSourceV2RotationDrainsOldFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "one\ntwo\n")
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "first file", func() bool { return c.count() == 2 })

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeV2File(t, path, "three\nfour\n")
	waitV2(t, "rotated file", func() bool { return c.count() == 4 })
	if got := c.sortedLines(); !sortedEqual(got, []string{"four", "one", "three", "two"}) {
		t.Fatalf("lines = %v", got)
	}
}

// copytruncate: the file is rewritten in place, so the read offset is beyond
// the new size; the source restarts at 0 and keeps going (no stall).
func TestFileSourceV2CopytruncateRestartsAtZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2File(t, path, "aaaaaaaaaaaaaaaa\nbbbbbbbbbbbbbbbb\n") // 34 bytes
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "old content", func() bool { return c.count() == 2 })

	writeV2File(t, path, "cc\ndd\n") // 6 bytes < 34
	waitV2(t, "new content", func() bool { return c.count() == 4 })
	if got := c.sortedLines(); !sortedEqual(got, []string{"aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cc", "dd"}) {
		t.Fatalf("lines = %v", got)
	}
}

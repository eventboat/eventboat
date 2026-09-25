package builtin

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// Multiline aggregation (P3, design §2.4) at the source level: after/before
// classification, the three caps, timeout, rotation/truncation/stop flushes,
// the oversize interaction and the health counters. The helpers live in
// file_source_v2_test.go (same package).

// mlConfig builds the file source config for a multiline scenario.
func mlConfig(path string, ml map[string]any) map[string]any {
	return map[string]any{"path": path, "poll_every_ms": 10, "multiline": ml}
}

func sortCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// after + negate: the pattern matches group-START lines, so a negated hit is
// a continuation — the idiomatic Java stack-trace shape.
func TestFileSourceMultilineAfterNegateStack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, strings.Join([]string{
		"2026-01-01 ERROR boom",
		"    at com.foo.Bar.baz(Bar.java:1)",
		"    at com.foo.Baz.qux(Baz.java:2)",
		"2026-01-01 INFO ok",
		"    at com.foo.Info.call(Info.java:3)",
	}, "\n")+"\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "2 aggregated groups", func() bool { return c.count() == 2 })
	want := []string{
		"2026-01-01 ERROR boom\n    at com.foo.Bar.baz(Bar.java:1)\n    at com.foo.Baz.qux(Baz.java:2)",
		"2026-01-01 INFO ok\n    at com.foo.Info.call(Info.java:3)",
	}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("groups = %q, want %q", got, want)
	}
	if merges := fs.Counters()["multiline_merges"]; merges != 3 {
		t.Fatalf("multiline_merges = %d, want 3", merges)
	}
}

// before: the pattern matches the first line of a group; anything else is a
// continuation, including blank lines.
func TestFileSourceMultilineBeforeGroups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, strings.Join([]string{
		"2026-01-01 ERROR boom",
		"    at foo",
		"",
		"    at bar",
		"2026-01-01 INFO ok",
	}, "\n")+"\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\d{4}-\d{2}-\d{2}`, "match": "before", "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "2 before-groups", func() bool { return c.count() == 2 })
	want := []string{
		"2026-01-01 ERROR boom\n    at foo\n\n    at bar",
		"2026-01-01 INFO ok",
	}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("groups = %q, want %q", got, want)
	}
}

// timeout_ms flushes an open group even when the file produced no new data.
func TestFileSourceMultilineTimeoutFlushesWithoutNewData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "s1\n  c1\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 200,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "the group to be open", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, e := range fs.files {
			return e.groupOpen && e.groupLines == 2
		}
		return false
	})
	time.Sleep(50 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("group emitted before timeout: %d messages", n)
	}
	waitV2(t, "the timeout flush", func() bool { return c.count() == 1 })
	if got := c.lines()[0]; got != "s1\n  c1" {
		t.Fatalf("group = %q, want %q", got, "s1\n  c1")
	}
}

// An explicit timeout_ms: 0 means no timeout flush (the lint's target): the
// group is held until a new group-starting line arrives.
func TestFileSourceMultilineTimeoutZeroHoldsGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "s1\n  c1\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 0,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "the group to be open", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, e := range fs.files {
			return e.groupOpen && e.groupLines == 2
		}
		return false
	})
	time.Sleep(150 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("timeout_ms: 0 flushed the group: %d messages", n)
	}
	appendV2File(t, path, "s2\n")
	waitV2(t, "the new-group flush", func() bool { return c.count() == 1 })
	if got := c.lines()[0]; got != "s1\n  c1" {
		t.Fatalf("group = %q, want %q", got, "s1\n  c1")
	}
}

// max_lines caps a group: the next continuation flushes it and starts a new
// group with that line.
func TestFileSourceMultilineMaxLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "s1\n  c1\n  c2\n  c3\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "max_lines": 2, "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "2 capped groups", func() bool { return c.count() == 2 })
	want := []string{"s1\n  c1", "  c2\n  c3"}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("groups = %q, want %q", got, want)
	}
}

// max_bytes caps a group by accumulated size; a single over-limit line still
// forms its own group.
func TestFileSourceMultilineMaxBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	long := "  " + strings.Repeat("x", 30)
	writeV2File(t, path, "s1\n"+long+"\n  tail\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "max_bytes": 16, "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "3 capped groups", func() bool { return c.count() == 3 })
	want := []string{"s1", long, "  tail"}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("groups = %q, want %q", got, want)
	}
}

// Rotation flushes the open group before the old file is drained away, so the
// old content is never lost to clean_removed.
func TestFileSourceMultilineRotationFlushesGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "s1\n  c1\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "the group to be open", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, e := range fs.files {
			return e.groupOpen && e.groupLines == 2
		}
		return false
	})
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeV2File(t, path, "s2\n  c2\n")
	waitV2(t, "both groups", func() bool { return c.count() == 2 })
	want := []string{"s1\n  c1", "s2\n  c2"}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("groups = %q, want %q", got, want)
	}
	if rotations := fs.Counters()["rotations"]; rotations != 1 {
		t.Fatalf("rotations = %d, want 1", rotations)
	}
}

// copytruncate flushes the open group before the offset resets, then reads
// the rewritten content from 0.
func TestFileSourceMultilineTruncationFlushesGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "s1\n  c1\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "the group to be open", func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, e := range fs.files {
			return e.groupOpen && e.groupLines == 2
		}
		return false
	})
	writeV2File(t, path, "tiny\n") // shorter than the read offset: truncation
	waitV2(t, "the pre-truncation group and the new line", func() bool { return c.count() == 2 })
	want := []string{"s1\n  c1", "tiny"}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("groups = %q, want %q", got, want)
	}
}

// stop mode reads complete files: a trailing group is flushed at EOF so the
// batch run commits every line before it reports exhausted.
func TestFileSourceMultilineStopFlushesTrailingGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.log")
	writeV2File(t, path, "s1\n  c1") // no trailing newline
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "on_eof": "stop",
		"multiline": map[string]any{"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 5000},
	})
	c := &sourceCollector{}
	if err := fs.Run(context.Background(), c.emit); err != nil {
		t.Fatalf("stop Run: %v", err)
	}
	_ = fs.Close()
	if got := c.lines(); len(got) != 1 || got[0] != "s1\n  c1" {
		t.Fatalf("groups = %q, want [s1\\n  c1]", got)
	}
}

// Without multiline, a multi-line file stays one message per line (the v1
// behavior is untouched by the new block's absence).
func TestFileSourceMultilineDisabledIsPerLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "s1\n  c1\n  c2\n")
	fs := newV2Source(t, map[string]any{"path": path, "poll_every_ms": 10})
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "3 separate lines", func() bool { return c.count() == 3 })
	want := []string{"s1", "  c1", "  c2"}
	if got := c.sortedLines(); !sortedEqual(got, sortCopy(want)) {
		t.Fatalf("lines = %q, want %q", got, want)
	}
	if merges := fs.Counters()["multiline_merges"]; merges != 0 {
		t.Fatalf("multiline_merges without multiline = %d, want 0", merges)
	}
}

// An oversize line under skip is dropped and does NOT break the group; under
// truncate the truncated content joins it.
func TestFileSourceMultilineOversizeInteraction(t *testing.T) {
	t.Run("skip", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		writeV2File(t, path, "s1\n  123456789012345\n  c1\n")
		fs := newV2Source(t, map[string]any{
			"path": path, "poll_every_ms": 10, "max_line_bytes": 8, "oversize": "skip",
			"multiline": map[string]any{"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 50},
		})
		c := &sourceCollector{}
		startV2Source(t, fs, c)
		waitV2(t, "the surviving group", func() bool { return c.count() == 1 })
		if got := c.lines()[0]; got != "s1\n  c1" {
			t.Fatalf("group = %q, want %q", got, "s1\n  c1")
		}
		counters := fs.Counters()
		if counters["lines_read"] != 3 || counters["lines_skipped"] != 1 ||
			counters["lines_truncated"] != 0 || counters["multiline_merges"] != 1 {
			t.Fatalf("counters = %v", counters)
		}
	})
	t.Run("truncate", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		writeV2File(t, path, "s1\n  123456789012345\n")
		fs := newV2Source(t, map[string]any{
			"path": path, "poll_every_ms": 10, "max_line_bytes": 8, "oversize": "truncate",
			"multiline": map[string]any{"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 50},
		})
		c := &sourceCollector{}
		startV2Source(t, fs, c)
		waitV2(t, "the truncated group", func() bool { return c.count() == 1 })
		if got := c.lines()[0]; got != "s1\n  123456" {
			t.Fatalf("group = %q, want %q", got, "s1\n  123456")
		}
		counters := fs.Counters()
		if counters["lines_truncated"] != 1 || counters["lines_skipped"] != 0 ||
			counters["multiline_merges"] != 1 {
			t.Fatalf("counters = %v", counters)
		}
	})
}

// A group made only of whitespace lines is consumed without a message.
func TestFileSourceMultilineWhitespaceOnlyGroupSkipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeV2File(t, path, "   \nnext\n")
	fs := newV2Source(t, mlConfig(path, map[string]any{
		"pattern": `^\S`, "negate": true, "match": "after", "timeout_ms": 50,
	}))
	c := &sourceCollector{}
	startV2Source(t, fs, c)

	waitV2(t, "the non-blank group", func() bool { return c.count() == 1 })
	if got := c.lines()[0]; got != "next" {
		t.Fatalf("message = %q, want next", got)
	}
	counters := fs.Counters()
	if counters["lines_read"] != 2 || counters["multiline_merges"] != 0 {
		t.Fatalf("counters = %v", counters)
	}
}

// The group watermark is the end offset of the LAST line: a committed group
// is never re-read on restart (at-least-once).
func TestFileSourceMultilineWatermarkIsGroupEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.log")
	writeV2File(t, path, "s1\n  c1\n")
	fs := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "on_eof": "stop",
		"multiline": map[string]any{"pattern": `^\S`, "negate": true, "match": "after"},
	})
	c := &sourceCollector{}
	if err := fs.Run(context.Background(), c.emit); err != nil {
		t.Fatalf("stop Run: %v", err)
	}
	state, err := fs.Commit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.Close()
	if c.count() != 1 {
		t.Fatalf("first run emitted %d messages, want 1", c.count())
	}

	fs2 := newV2Source(t, map[string]any{
		"path": path, "poll_every_ms": 10, "on_eof": "stop",
		"multiline": map[string]any{"pattern": `^\S`, "negate": true, "match": "after"},
	})
	if err := fs2.Init(state); err != nil {
		t.Fatal(err)
	}
	c2 := &sourceCollector{}
	if err := fs2.Run(context.Background(), c2.emit); err != nil {
		t.Fatalf("resumed stop Run: %v", err)
	}
	_ = fs2.Close()
	if c2.count() != 0 {
		t.Fatalf("resumed source re-emitted %d messages, want 0", c2.count())
	}
}

// An invalid pattern is a construction (verify) error, not a silent no-op.
func TestFileSourceMultilineInvalidPattern(t *testing.T) {
	reg := registry.New()
	if err := registerFileSource(reg); err != nil {
		t.Fatal(err)
	}
	_, err := reg.NewSource("file", map[string]any{
		"path":      "x.log",
		"multiline": map[string]any{"pattern": "("},
	})
	if err == nil || !strings.Contains(err.Error(), "multiline.pattern") {
		t.Fatalf("invalid pattern error = %v, want a multiline.pattern failure", err)
	}
}

// The container-log basename is parsed into pod/namespace/container without
// any k8s API; a non-matching basename gets no extra fields.
func TestFileSourceContainerPathMetadata(t *testing.T) {
	dir := t.TempDir()
	longID := strings.Repeat("a", 64)
	container := filepath.Join(dir, "web-1_default_nginx-"+longID+".log")
	short := filepath.Join(dir, "shop-2_kube-system_api-deadbeef.log")
	plain := filepath.Join(dir, "app.log")
	underscored := filepath.Join(dir, "bad_pod_ns_ctr-deadbeef.log")
	writeV2File(t, container, "c\n")
	writeV2File(t, short, "s\n")
	writeV2File(t, plain, "p\n")
	writeV2File(t, underscored, "u\n")

	fs := newV2Source(t, map[string]any{
		"path": filepath.ToSlash(filepath.Join(dir, "*.log")), "poll_every_ms": 10,
	})
	c := &sourceCollector{}
	startV2Source(t, fs, c)
	waitV2(t, "4 lines", func() bool { return c.count() == 4 })

	byLine := map[string]registry.Message{}
	for _, m := range c.snapshot() {
		byLine[string(m.Raw)] = m
	}
	assertMeta := func(line, pod, namespace, cont string) {
		t.Helper()
		m, ok := byLine[line]
		if !ok {
			t.Fatalf("message %q missing", line)
		}
		if m.Meta["pod"] != pod || m.Meta["namespace"] != namespace || m.Meta["container"] != cont {
			t.Fatalf("meta of %q = %v, want pod=%q namespace=%q container=%q",
				line, m.Meta, pod, namespace, cont)
		}
	}
	assertMeta("c", "web-1", "default", "nginx")
	assertMeta("s", "shop-2", "kube-system", "api")
	for _, line := range []string{"p", "u"} {
		m := byLine[line]
		for _, k := range []string{"pod", "namespace", "container"} {
			if _, ok := m.Meta[k]; ok {
				t.Fatalf("meta of %q has %s=%v, want no container fields", line, k, m.Meta[k])
			}
		}
	}
}

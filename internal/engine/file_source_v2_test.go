package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/store"
)

// End-to-end scenario matrix for the P2 file source v2: glob, rotation,
// copytruncate, link retargeting, deletion, half lines, crash resume and fd
// lifecycle, driven through the real engine. The deterministic source-level
// matrix lives in internal/registry/builtin/file_source_v2_test.go.

func fileV2Pipeline(name, fileCfg string) string {
	return fmt.Sprintf(`
apiVersion: eventboat/v1
kind: Pipeline
metadata: {name: %s}
sources:
  in:
    decoder: json
    file: %s
sinks:
  out:
    depends_on: [in]
    mem: {id: out}
`, name, fileCfg)
}

func fileV2Cfg(path string, extra ...string) string {
	parts := append([]string{fmt.Sprintf("path: %q", filepath.ToSlash(path)), "poll_every_ms: 10"}, extra...)
	return "{" + strings.Join(parts, ", ") + "}"
}

func writeV2Lines(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendV2Raw(t *testing.T, path, raw string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(raw); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func sinkLines(h *harness) []string {
	msgs, _, _ := h.sink("out").snapshot()
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Raw)
	}
	return out
}

func uniqueSorted(lines []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out
}

func waitDeliveredUnique(t *testing.T, h *harness, want int) []string {
	t.Helper()
	var unique []string
	waitFor(t, func() bool {
		unique = uniqueSorted(sinkLines(h))
		return len(unique) >= want
	})
	return unique
}

func assertLines(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delivered lines = %v, want %v", got, want)
	}
}

// Rotation: rename + recreate at the same path delivers the old file's
// remainder and the new file's lines; the glob keeps following the path.
func TestFileSourceV2EngineRotation(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2Lines(t, path, `{"i":1}`, `{"i":2}`)
	pip := h.build(fileV2Pipeline("fsrot", fileV2Cfg(path)))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 2)

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeV2Lines(t, path, `{"i":3}`, `{"i":4}`)
	assertLines(t, waitDeliveredUnique(t, h, 4),
		[]string{`{"i":1}`, `{"i":2}`, `{"i":3}`, `{"i":4}`})
	waitCommit(t, eng)
	stop()
}

// copytruncate: the file is rewritten in place with less data than the read
// offset; the source resets to 0 and keeps going (no stall).
func TestFileSourceV2EngineCopytruncate(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2Lines(t, path, `{"line":1}`, `{"line":2}`) // 24 bytes
	pip := h.build(fileV2Pipeline("fsct", fileV2Cfg(path)))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 2)

	writeV2Lines(t, path, `{"n":3}`, `{"n":4}`) // 16 bytes < 24: truncation visible
	assertLines(t, waitDeliveredUnique(t, h, 4),
		[]string{`{"line":1}`, `{"line":2}`, `{"n":3}`, `{"n":4}`})
	waitCommit(t, eng)
	stop()
}

// A link whose target is replaced is rotation: the link path stays the
// metadata identity while the target's file id decides old vs new. A symlink
// is used where the platform permits creating one, a hardlink otherwise.
func TestFileSourceV2EngineLinkTargetReplaced(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	target1 := filepath.Join(dir, "target1.jsonl")
	writeV2Lines(t, target1, `{"i":1}`, `{"i":2}`)
	link := filepath.Join(dir, "app.jsonl")
	makeLink := func(target string) {
		t.Helper()
		symlinkErr := os.Symlink(target, link)
		if symlinkErr == nil {
			t.Logf("linked %s (symlink)", link)
			return
		}
		if err := os.Link(target, link); err != nil {
			t.Skipf("neither symlink (%v) nor hardlink (%v) available", symlinkErr, err)
		}
		t.Logf("symlinks unavailable (%v); linked %s as a hardlink", symlinkErr, link)
	}
	makeLink(target1)

	pip := h.build(fileV2Pipeline("fslink", fileV2Cfg(link)))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 2)

	target2 := filepath.Join(dir, "target2.jsonl")
	writeV2Lines(t, target2, `{"i":3}`, `{"i":4}`)
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	makeLink(target2)

	assertLines(t, waitDeliveredUnique(t, h, 4),
		[]string{`{"i":1}`, `{"i":2}`, `{"i":3}`, `{"i":4}`})
	msgs, _, _ := h.sink("out").snapshot()
	for _, m := range msgs {
		// The matched path is the configured (slash) form, not the resolved
		// target and not re-normalized.
		if m.Meta["file_path"] != filepath.ToSlash(link) {
			t.Fatalf("file_path = %v, want the link path %s", m.Meta["file_path"], filepath.ToSlash(link))
		}
	}
	waitCommit(t, eng)
	stop()
}

// A file deleted after its descriptor was opened is still read to its end
// from the held descriptor; nothing after it is needed for the line to land.
func TestFileSourceV2EngineDeletedFileKeepsReading(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2Lines(t, path, `{"i":1}`)
	pip := h.build(fileV2Pipeline("fsdel", fileV2Cfg(path)))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 1)

	// One more line, then the name disappears before the next poll: the
	// held descriptor is the only way the line can still be delivered.
	appendV2Raw(t, path, "{\"i\":2}\n")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	assertLines(t, waitDeliveredUnique(t, h, 2), []string{`{"i":1}`, `{"i":2}`})
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file still exists after removal: %v", err)
	}
	waitCommit(t, eng)
	stop()
}

// Only newline-terminated lines are emitted in tail mode; the half line waits
// and completes into exactly one message.
func TestFileSourceV2EngineHalfLine(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	if err := os.WriteFile(path, []byte("{\"i\":1}\n{\"i\":2}"), 0o644); err != nil {
		t.Fatal(err)
	}
	pip := h.build(fileV2Pipeline("fshalf", fileV2Cfg(path)))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 1)

	time.Sleep(100 * time.Millisecond)
	if got := sinkLines(h); len(got) != 1 {
		t.Fatalf("half line emitted early: %v", got)
	}
	appendV2Raw(t, path, "\n")
	assertLines(t, waitDeliveredUnique(t, h, 2), []string{`{"i":1}`, `{"i":2}`})
	waitCommit(t, eng)
	stop()
}

// Crash resume: the persisted v2 source state lands in the store, and a fresh
// engine resumes from the committed offset instead of re-reading the file.
func TestFileSourceV2EngineResumeAfterRestart(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2Lines(t, path, `{"i":1}`, `{"i":2}`, `{"i":3}`)
	pip := h.build(fileV2Pipeline("fsresume", fileV2Cfg(path)))

	db := filepath.Join(t.TempDir(), "fs.db")
	st1, err := store.OpenSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st1.Close() })
	eng1, stop1 := runEngine(t, pip, st1, h.reg, fastOptions())
	waitDeliveredUnique(t, h, 3)
	waitCommit(t, eng1)
	stop1()

	state, seq, err := st1.SourceState("fsresume", "in")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3 || !strings.Contains(string(state), `"version":2`) {
		t.Fatalf("persisted source state = %s (frontier %d), want a v2 state at 3", state, seq)
	}
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}

	// Down for a while, then the file grows: the restart resumes at the
	// persisted offset.
	appendV2Raw(t, path, "{\"i\":4}\n{\"i\":5}\n")
	st2, err := store.OpenSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	_, stop2 := runEngine(t, pip, st2, h.reg, fastOptions())
	assertLines(t, waitDeliveredUnique(t, h, 5),
		[]string{`{"i":1}`, `{"i":2}`, `{"i":3}`, `{"i":4}`, `{"i":5}`})
	stop2()
}

// ignore_older skips a file that is already old at first discovery; once it
// grows a fresh mtime it is read.
func TestFileSourceV2EngineIgnoreOlder(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "old.jsonl")
	writeV2Lines(t, path, `{"i":1}`)
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}
	pip := h.build(fileV2Pipeline("fsolder", fileV2Cfg(path, "ignore_older_ms: 60000")))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())

	time.Sleep(150 * time.Millisecond)
	if got := sinkLines(h); len(got) != 0 {
		t.Fatalf("old file was read despite ignore_older: %v", got)
	}
	appendV2Raw(t, path, "{\"i\":2}\n")
	assertLines(t, waitDeliveredUnique(t, h, 2), []string{`{"i":1}`, `{"i":2}`})
	waitCommit(t, eng)
	stop()
}

// close_inactive releases an idle descriptor (on Windows the rename below
// only succeeds once it is closed) and the file is reopened and read on its
// next growth.
func TestFileSourceV2EngineCloseInactiveReopen(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	writeV2Lines(t, path, `{"i":1}`)
	pip := h.build(fileV2Pipeline("fsclose", fileV2Cfg(path, "close_inactive_ms: 50")))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 1)

	rotated := path + ".rot"
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := os.Rename(path, rotated)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle descriptor never released: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.Rename(rotated, path); err != nil {
		t.Fatal(err)
	}
	appendV2Raw(t, path, "{\"i\":2}\n")
	assertLines(t, waitDeliveredUnique(t, h, 2), []string{`{"i":1}`, `{"i":2}`})
	waitCommit(t, eng)
	stop()
}

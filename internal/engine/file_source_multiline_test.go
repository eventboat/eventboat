package engine

import (
	"path/filepath"
	"testing"

	"github.com/eventboat/eventboat/internal/store"
)

// End-to-end: the file source's multiline aggregation arrives as ONE engine
// message (one decode, one commit), and the group's watermark is its last
// line's end offset (the restart test in the builtin package pins the
// watermark itself).
func TestFileSourceMultilineEngineAggregation(t *testing.T) {
	h := newHarness(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.jsonl")
	// One JSON document split across lines (the continuation is indented so
	// the after+negate classifier aggregates it) plus a second document.
	writeV2Lines(t, path, `{"msg":"boom",`, `  "detail":"stack"}`, `{"msg":"ok"}`)
	extra := `multiline: {pattern: '^\S', negate: true, match: after, timeout_ms: 100}`
	pip := h.build(fileV2Pipeline("fsml", fileV2Cfg(path, extra)))
	eng, stop := runEngine(t, pip, store.NewMemory(), h.reg, fastOptions())
	waitDeliveredUnique(t, h, 2)
	assertLines(t, sinkLines(h), []string{
		"{\"msg\":\"boom\",\n  \"detail\":\"stack\"}",
		`{"msg":"ok"}`,
	})
	waitCommit(t, eng)
	stop()
}

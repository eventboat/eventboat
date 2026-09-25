package builtin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

type fileSourceConfig struct {
	Path          string `json:"path" schema:"minLen=1,desc=glob (*, ?, [...]) of files to tail; one message per line"`
	PollEvery     int    `json:"poll_every_ms" schema:"min=10,default=250,desc=scan interval: glob rescan plus per-file poll"`
	StartAt       string `json:"start_at" schema:"enum=beginning|end,default=beginning,desc=where a newly discovered file starts"`
	OnEOF         string `json:"on_eof" schema:"enum=tail|stop,default=tail,desc=tail keeps polling for appended lines; stop finishes the source once every tracked file is read to its end (complete batch files)"`
	CloseInactive int    `json:"close_inactive_ms" schema:"min=0,default=300000,desc=close a file's descriptor after this much idle time (0 = never); it reopens when the file changes"`
	IgnoreOlder   int    `json:"ignore_older_ms" schema:"min=0,default=0,desc=skip files whose mtime is older than this when first discovered (0 = off)"`
	CleanRemoved  *bool  `json:"clean_removed" schema:"default=true,desc=drop the state of files that no longer match the glob once they are drained and closed"`
	MaxLineBytes  int    `json:"max_line_bytes" schema:"min=1,default=262144,desc=maximum line length in bytes (aligned with VictoriaLogs' default)"`
	Oversize      string `json:"oversize" schema:"enum=truncate|skip,default=truncate,desc=truncate emits the first max_line_bytes of an oversized line; skip drops it and counts"`
	Host          string `json:"host" schema:"optional,desc=value for meta.host; defaults to os.Hostname()"`
}

func registerFileSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "file", 1, []string{"pull", "finite"}, func(c fileSourceConfig) (registry.Source, error) {
		cleanRemoved := true
		if c.CleanRemoved != nil {
			cleanRemoved = *c.CleanRemoved
		}
		host := c.Host
		if host == "" {
			host, _ = os.Hostname()
		}
		return &fileSource{
			path:          c.Path,
			pollEvery:     time.Duration(c.PollEvery) * time.Millisecond,
			startAt:       c.StartAt,
			onEOF:         c.OnEOF,
			closeInactive: time.Duration(c.CloseInactive) * time.Millisecond,
			ignoreOlder:   time.Duration(c.IgnoreOlder) * time.Millisecond,
			cleanRemoved:  cleanRemoved,
			maxLineBytes:  c.MaxLineBytes,
			oversize:      c.Oversize,
			host:          host,
			warnf:         log.Printf,
		}, nil
	})
}

// fileSource tails one file or a glob of files line by line. Commit state is
// per-file: the v2 document {"version":2,"files":{"<id>":{"path":...,"offset":N}}}
// where <id> is the platform file identity (device+inode on Unix, volume+file
// index on Windows; path on unsupported platforms), so a restart resumes every
// file at its own committed byte offset (at-least-once: duplicates after a
// crash, never loss). The v1 single-offset document {"offset":N} is still
// accepted for a meta-free path and applied to that file's first open.
//
// Discovery happens on every poll: the glob is rescanned, new files are opened
// and each tracked file is drained to EOF. Identity changes at one path are
// rotation (the old descriptor is drained to EOF before it is closed), a size
// below the read position is truncation (restart from 0), and a file that
// stops matching the glob keeps its descriptor until it is drained and idle.
// Only newline-terminated lines are emitted in tail mode; an unterminated
// trailing line waits in memory and is completed by the next write (the stop
// mode emits it as its final message when the EOF is the end of a complete
// batch file).
//
// Completion (v1.24 contract): with on_eof:stop the source returns nil once
// every tracked file is at EOF and a full scan produced nothing — the shape
// that makes a batch run or a job run finish. stop is for COMPLETE files
// (written before the run starts); the default on_eof:tail never returns on
// its own. A glob with zero matches is an error under stop (a batch run must
// not sit silent) and a wait-under-poll under tail.
//
// Lock-across-emit is deliberate (candidate 01): pump holds s.mu while
// emitting, and Commit takes the same lock, so this source deadlocks against
// a commit path that calls Commit synchronously — the regression fixture
// proving the engine's per-source committer tolerates it.
type fileSource struct {
	path          string
	pollEvery     time.Duration
	startAt       string
	onEOF         string
	closeInactive time.Duration
	ignoreOlder   time.Duration
	cleanRemoved  bool
	maxLineBytes  int
	oversize      string
	host          string
	warnf         func(format string, args ...any)

	mu            sync.Mutex
	scratch       []byte
	files         map[string]*trackedFile   // file id -> live state
	byPath        map[string]string         // currently matched path -> its file id
	ignored       map[string]bool           // ids skipped by ignore_older (in-memory only)
	carry         map[string]fileStateEntry // persisted state for files not yet rediscovered
	legacyOffset  int64                     // v1 {"offset":N} for a meta-free path
	legacyPending bool
	pending       map[int64]filePending // srcSeq -> (file id, emitted end offset)
	nextSeq       int64
	lastCommitted int64 // highest srcSeq already folded by Commit

	oversizeTruncated int64
	oversizeSkipped   int64
}

// trackedFile is one discovered file. offset is the next unread byte (it only
// advances over complete lines); committed is the watermark persisted through
// Commit. The unterminated trailing line lives in partial (capped at
// maxLineBytes) plus the partialBytes counter, which keeps the read buffer
// bounded even for multi-megabyte lines.
type trackedFile struct {
	id           string
	path         string
	f            *os.File
	offset       int64
	committed    int64
	partial      []byte
	partialBytes int64
	oversize     bool
	size         int64
	mtime        time.Time
	lastRead     time.Time
	removed      bool // no longer matched by the glob
	reopen       bool // a read error dropped the descriptor; force a reopen
}

type filePending struct {
	id  string
	end int64
}

// fileStateEntry is one file in the v2 state document.
type fileStateEntry struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
}

const fileReadChunk = 64 * 1024

func (s *fileSource) Init(state []byte) error {
	if len(state) == 0 {
		return nil
	}
	var doc struct {
		Version int                       `json:"version"`
		Offset  *int64                    `json:"offset"`
		Files   map[string]fileStateEntry `json:"files"`
	}
	if err := json.Unmarshal(state, &doc); err != nil {
		return fmt.Errorf("file source: bad state: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureMapsLocked()
	if doc.Version >= 2 {
		for id, e := range doc.Files {
			s.carry[id] = e
		}
		return nil
	}
	if doc.Offset != nil {
		// v1 wrote one offset for one path. It is applied to the first file
		// discovered under a meta-free path; a glob pattern cannot be mapped
		// onto a single file (v1 could not glob), so it is ignored there.
		if !hasGlobMeta(s.path) {
			s.legacyOffset, s.legacyPending = *doc.Offset, true
		}
	}
	return nil
}

func (s *fileSource) Run(ctx context.Context, emit func(registry.Message) error) error {
	s.mu.Lock()
	s.ensureMapsLocked()
	s.pending = map[int64]filePending{}
	s.mu.Unlock()

	// The first scan runs immediately (not after one poll interval) so a
	// missing file under stop fails at startup, exactly as the single-file
	// source always did, and tail reads existing data without the initial lag.
	if _, err := s.poll(ctx, emit, true); err != nil {
		if ctx.Err() != nil {
			return nil // cancelled: a voluntary stop, not a failure
		}
		return err
	}
	tick := time.NewTicker(s.pollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil // cancelled: a voluntary stop, not a failure
		case <-tick.C:
			emitted, err := s.poll(ctx, emit, false)
			if err != nil {
				// Refusal: the framework reports it as a failed source (the
				// source watermark is the no-loss safety net). The engine
				// shutting down under the emit is a voluntary stop instead.
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if s.onEOF == "stop" && !emitted && s.allAtEOF() {
				return nil // every tracked file read to its end: exhausted
			}
		}
	}
}

func (s *fileSource) ensureMapsLocked() {
	if s.files == nil {
		s.files = map[string]*trackedFile{}
	}
	if s.byPath == nil {
		s.byPath = map[string]string{}
	}
	if s.ignored == nil {
		s.ignored = map[string]bool{}
	}
	if s.carry == nil {
		s.carry = map[string]fileStateEntry{}
	}
	if s.pending == nil {
		s.pending = map[int64]filePending{}
	}
	if s.scratch == nil {
		s.scratch = make([]byte, fileReadChunk)
	}
}

// poll is one scan: re-glob, discover (and maybe open) every match, drain
// every open descriptor to EOF, then close idle descriptors. It deliberately
// holds s.mu across emit (the candidate-01 regression fixture).
func (s *fileSource) poll(ctx context.Context, emit func(registry.Message) error, first bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	matches, err := filepath.Glob(s.path)
	if err != nil {
		return false, fmt.Errorf("file source: bad glob %q: %w", s.path, err)
	}
	if first && len(matches) == 0 && s.onEOF == "stop" {
		return false, fmt.Errorf("file source: no files match %s", s.path)
	}

	seen := make(map[string]bool, len(matches))
	failed := make(map[string]bool, len(matches))
	for _, p := range matches {
		e := s.discoverLocked(p)
		if e == nil {
			failed[p] = true
			continue
		}
		seen[e.id] = true
	}

	now := time.Now()
	emitted := false
	for _, id := range s.sortedIDsLocked() {
		e := s.files[id]
		if !seen[id] && !failed[e.path] {
			e.removed = true
		} else if seen[id] {
			e.removed = false
		}
		if e.f == nil {
			continue
		}
		em, err := s.drainLocked(e, emit, now)
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
	}
	s.reapLocked(now)
	return emitted, nil
}

// discoverLocked resolves path to its current file, opening it when needed.
// A nil return means the path could not be used this scan (transient open
// error, or ignore_older skipped it); the caller keeps the previous state.
func (s *fileSource) discoverLocked(path string) *trackedFile {
	// A closed descriptor whose file did not change since it was closed is
	// left closed: this is what makes close_inactive reduce open descriptors
	// instead of reopening every poll.
	if id, ok := s.byPath[path]; ok {
		if e, ok := s.files[id]; ok && e.f == nil && !e.reopen {
			if fi, err := os.Stat(path); err == nil && fi.Size() == e.size && fi.ModTime().Equal(e.mtime) {
				return e
			}
		}
	}

	f, err := openReadShared(path)
	if err != nil {
		return nil // transient (locked, permission, replaced): retry next scan
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil
	}
	if !fi.Mode().IsRegular() {
		// A glob can match directories (and named pipes, which would block a
		// read); only regular files are tailed.
		_ = f.Close()
		return nil
	}
	id, _ := fileIdentity(f)

	// ignore_older applies at first discovery only: a file already tracked
	// (e.g. resumed from state) is never dropped because it looks old, and an
	// ignored file that becomes fresh again is picked up from its start.
	if s.ignoreOlder > 0 {
		if s.ignored[id] {
			if fi.ModTime().Before(time.Now().Add(-s.ignoreOlder)) {
				_ = f.Close()
				return nil // still old
			}
			delete(s.ignored, id)
		} else if _, tracked := s.files[id]; !tracked {
			if fi.ModTime().Before(time.Now().Add(-s.ignoreOlder)) {
				s.ignored[id] = true
				_ = f.Close()
				return nil
			}
		}
	}

	e, tracked := s.files[id]
	if !tracked {
		e = s.newTrackedLocked(id, path, fi.Size())
		s.files[id] = e
	}
	if oldID, ok := s.byPath[path]; ok && oldID != id {
		if old, ok := s.files[oldID]; ok {
			old.removed = true // rotated away: drain the old descriptor, then reap
		}
	}
	s.byPath[path] = id
	if e.path != path {
		e.path = path
	}
	if e.f == nil {
		if e.offset > fi.Size() {
			// Truncated while the descriptor was closed: restart from 0
			// (copytruncate; duplicates are acceptable).
			e.offset, e.committed = 0, 0
		}
		if _, err := f.Seek(e.offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil
		}
		e.f = f
		e.partial, e.partialBytes, e.oversize = nil, 0, false
		e.lastRead = time.Now()
	} else {
		_ = f.Close() // same id already open: keep the existing descriptor
	}
	e.size, e.mtime, e.reopen = fi.Size(), fi.ModTime(), false
	return e
}

// newTrackedLocked creates the state for a newly discovered file: a persisted
// per-file offset resumes it, the v1 offset applies to a meta-free path's
// first file, and otherwise start_at decides (beginning, or end = skip
// everything already written).
func (s *fileSource) newTrackedLocked(id, path string, size int64) *trackedFile {
	e := &trackedFile{id: id, path: path, size: size}
	switch {
	case s.carry[id].Offset > 0:
		e.offset, e.committed = s.carry[id].Offset, s.carry[id].Offset
		delete(s.carry, id)
	case s.legacyPending && !hasGlobMeta(s.path):
		e.offset, e.committed = s.legacyOffset, s.legacyOffset
		s.legacyPending = false
	case s.startAt == "end":
		e.offset = size
	}
	if e.offset > size {
		// The watermark is beyond the file: it was truncated while the
		// process was down. Re-read from 0 (duplicates, never loss).
		e.offset, e.committed = 0, 0
	}
	return e
}

// drainLocked reads one open file to EOF, emitting complete lines. It returns
// whether anything was emitted and the first refusal error.
func (s *fileSource) drainLocked(e *trackedFile, emit func(registry.Message) error, now time.Time) (bool, error) {
	if fi, err := e.f.Stat(); err == nil {
		if fi.Size() < e.offset {
			// copytruncate: the file was rewritten in place. Restart from 0
			// (duplicates are acceptable, a stalled tail is not).
			e.offset, e.committed = 0, 0
			e.partial, e.partialBytes, e.oversize = nil, 0, false
			if _, err := e.f.Seek(0, io.SeekStart); err != nil {
				return false, nil
			}
		}
		e.size, e.mtime = fi.Size(), fi.ModTime()
	}

	emitted := false
	for {
		n, rerr := e.f.Read(s.scratch)
		if n > 0 {
			e.lastRead = now
			em, err := s.consumeLocked(e, s.scratch[:n], emit)
			if em {
				emitted = true
			}
			if err != nil {
				return emitted, err
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			// Read failure (file replaced mid-read, lock violation): drop the
			// descriptor and let the next scan reopen it from the watermark.
			s.closeLocked(e)
			e.reopen = true
			return emitted, nil
		}
	}

	if s.onEOF == "stop" && e.partialBytes > 0 {
		// stop reads COMPLETE files: an unterminated last line is emitted as
		// the final message at EOF (tail would wait for its newline).
		oversize := e.oversize
		lineBytes := e.partialBytes
		e.offset += lineBytes
		em, err := s.emitLineLocked(e, e.partial, lineBytes, oversize, emit)
		e.partial, e.partialBytes, e.oversize = nil, 0, false
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
	}
	return emitted, nil
}

// consumeLocked parses one read chunk into lines. The unterminated tail stays
// in e.partial (capped at maxLineBytes; the full line length is tracked in
// partialBytes) so memory stays bounded no matter how long a single line is.
func (s *fileSource) consumeLocked(e *trackedFile, chunk []byte, emit func(registry.Message) error) (bool, error) {
	emitted := false
	for _, b := range chunk {
		if b != '\n' {
			e.partialBytes++
			if e.oversize {
				continue
			}
			if len(e.partial) < s.maxLineBytes {
				e.partial = append(e.partial, b)
			}
			if e.partialBytes > int64(s.maxLineBytes) {
				e.oversize = true
			}
			continue
		}
		// A complete line: advance the offset past it (newline included) and
		// emit it under the configured oversize policy.
		lineBytes := e.partialBytes + 1
		e.offset += lineBytes
		em, err := s.emitLineLocked(e, e.partial, lineBytes, e.oversize, emit)
		e.partial, e.partialBytes, e.oversize = nil, 0, false
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
	}
	return emitted, nil
}

// emitLineLocked applies the oversize policy and, when the line is emitted,
// records its end offset in pending so Commit folds it into the file's
// watermark. lineBytes is the full on-disk line length (the oversized
// remainder included), so a restart never re-reads an emitted line.
func (s *fileSource) emitLineLocked(e *trackedFile, line []byte, lineBytes int64, oversize bool, emit func(registry.Message) error) (bool, error) {
	if oversize {
		if s.oversize == "skip" {
			s.oversizeSkipped++
			s.warnOversize(e, lineBytes)
			return false, nil
		}
		s.oversizeTruncated++
		s.warnOversize(e, lineBytes)
	} else {
		line = bytes.TrimRight(line, "\r")
		if len(bytes.TrimSpace(line)) == 0 {
			return false, nil // blank line: consumed, not emitted (v1 behavior)
		}
	}
	end := e.offset
	s.nextSeq++
	seq := s.nextSeq
	s.pending[seq] = filePending{id: e.id, end: end}
	msg := registry.Message{
		Raw:     bytes.Clone(line),
		Meta:    map[string]any{"file_path": e.path, "host": s.host},
		SrcName: "file",
		SrcSeq:  seq,
	}
	if err := emit(msg); err != nil {
		return true, err
	}
	return true, nil
}

// warnOversize logs at the first few oversized lines and at powers of two
// after that, so a pathological file cannot turn the warning into a second
// data stream. The exact counts stay available for tests (S4 wires metrics).
func (s *fileSource) warnOversize(e *trackedFile, lineBytes int64) {
	if s.warnf == nil {
		return
	}
	n := s.oversizeTruncated + s.oversizeSkipped
	if n <= 3 || n&(n-1) == 0 {
		s.warnf("file source: %s: line of %d bytes exceeds max_line_bytes=%d (%s)",
			e.path, lineBytes, s.maxLineBytes, s.oversize)
	}
}

// reapLocked closes descriptors that have been idle for close_inactive and
// forgets removed files according to clean_removed. Closing discards the
// in-memory half-line; a reopen seeks back to the read watermark and re-reads
// it from the file, so nothing is lost while the file remains.
func (s *fileSource) reapLocked(now time.Time) {
	if s.closeInactive == 0 && !s.cleanRemoved {
		return
	}
	for _, e := range s.files {
		if e.f == nil {
			if e.removed && s.cleanRemoved {
				s.forgetLocked(e)
			}
			continue
		}
		if s.closeInactive == 0 || now.Sub(e.lastRead) < s.closeInactive {
			continue
		}
		s.closeLocked(e)
		if e.removed && s.cleanRemoved {
			s.forgetLocked(e)
		}
	}
}

func (s *fileSource) closeLocked(e *trackedFile) {
	if e.f != nil {
		_ = e.f.Close()
		e.f = nil
	}
	e.partial, e.partialBytes, e.oversize = nil, 0, false
}

// forgetLocked drops a file's state entirely (clean_removed). Pending
// emissions still reference the id; Commit skips ids that are no longer
// tracked, which is exactly what forgetting means.
func (s *fileSource) forgetLocked(e *trackedFile) {
	delete(s.files, e.id)
	delete(s.ignored, e.id)
	delete(s.carry, e.id)
	if s.byPath[e.path] == e.id {
		delete(s.byPath, e.path)
	}
}

// allAtEOF reports whether every tracked file has been read to its end under
// stop-mode semantics (the unterminated tail was already emitted). It is only
// consulted after a scan that emitted nothing, so a file with unread data
// keeps the source alive for one more poll.
func (s *fileSource) allAtEOF() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.files {
		if e.f == nil {
			if e.reopen || e.offset < e.size {
				return false
			}
			continue
		}
		if e.partialBytes > 0 || e.size > e.offset {
			return false
		}
	}
	return true
}

func (s *fileSource) sortedIDsLocked() []string {
	ids := make([]string, 0, len(s.files))
	for id := range s.files {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (s *fileSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureMapsLocked()
	// Watermark-bounded scan (see kafkaSource.Commit): Commit runs on every
	// frontier advance and pending entries are deleted during the scan, so
	// everything below lastCommitted is already drained.
	for seq := s.lastCommitted + 1; seq <= throughSrcSeq; seq++ {
		p, ok := s.pending[seq]
		if !ok {
			continue
		}
		delete(s.pending, seq)
		if e, ok := s.files[p.id]; ok && p.end > e.committed {
			e.committed = p.end
		}
	}
	if throughSrcSeq > s.lastCommitted {
		s.lastCommitted = throughSrcSeq
	}
	return s.marshalStateLocked(), nil
}

func (s *fileSource) marshalStateLocked() []byte {
	files := make(map[string]fileStateEntry, len(s.carry)+len(s.files))
	for id, st := range s.carry {
		if st.Offset > 0 {
			files[id] = st
		}
	}
	for id, e := range s.files {
		if e.committed > 0 {
			files[id] = fileStateEntry{Path: e.path, Offset: e.committed}
		}
	}
	st, _ := json.Marshal(struct {
		Version int                       `json:"version"`
		Files   map[string]fileStateEntry `json:"files"`
	}{Version: 2, Files: files})
	return st
}

func (s *fileSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.files {
		if e.f != nil {
			_ = e.f.Close()
			e.f = nil
		}
	}
	return nil
}

// hasGlobMeta reports whether path carries glob metacharacters. A meta-free
// path is a single file, which is the shape the v1 state compatibility and the
// pre-v2 tests rely on.
func hasGlobMeta(path string) bool { return strings.ContainsAny(path, "*?[") }

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
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	// Multiline is optional: without it every line is one message, exactly as
	// before (the nil path is the v1 behavior, unchanged).
	Multiline *fileMultilineConfig `json:"multiline" schema:"optional,desc=aggregate multi-line records (e.g. stack traces) into one message"`
	Host      string               `json:"host" schema:"optional,desc=value for meta.host; defaults to os.Hostname()"`
}

// fileMultilineConfig is the multiline aggregation contract (log-collection
// design §2.4): complete lines are classified by pattern (optionally negated)
// and joined into one message until a new group starts or a bound fires.
type fileMultilineConfig struct {
	Pattern   string `json:"pattern" schema:"minLen=1,desc=Go regexp classifying lines (see match/negate)"`
	Negate    bool   `json:"negate" schema:"default=false,desc=invert the pattern result before match is applied"`
	Match     string `json:"match" schema:"enum=after|before,default=after,desc=after: a hit is a continuation line; before: a hit starts a new group"`
	MaxLines  int    `json:"max_lines" schema:"min=1,default=500,desc=flush an open group at this many lines"`
	MaxBytes  int    `json:"max_bytes" schema:"min=1,default=1048576,desc=flush an open group at this many bytes"`
	TimeoutMs *int   `json:"timeout_ms" schema:"min=0,default=2000,desc=flush an open group this long after its last line (0 = no timeout)"`
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
		ml, err := buildMultiline(c.Multiline)
		if err != nil {
			return nil, err
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
			multiline:     ml,
			host:          host,
			warnf:         log.Printf,
		}, nil
	})
}

// buildMultiline compiles the multiline block at construction time: an
// invalid pattern is a factory error (verify reports it), and an explicitly
// zero timeout_ms stays zero — "no timeout" is a deliberate choice the
// lint_multiline_no_timeout lint warns about, not a value to silently default.
func buildMultiline(c *fileMultilineConfig) (*multilineConfig, error) {
	if c == nil {
		return nil, nil
	}
	re, err := regexp.Compile(c.Pattern)
	if err != nil {
		return nil, fmt.Errorf("file source: multiline.pattern %q: %w", c.Pattern, err)
	}
	timeoutMs := 2000
	if c.TimeoutMs != nil {
		timeoutMs = *c.TimeoutMs
	}
	return &multilineConfig{
		pattern:  re,
		negate:   c.Negate,
		before:   c.Match == "before",
		maxLines: c.MaxLines,
		maxBytes: c.MaxBytes,
		timeout:  time.Duration(timeoutMs) * time.Millisecond,
	}, nil
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
// Multiline (P3, design §2.4) is off unless configured: complete lines are
// classified by pattern/negate/match and joined with "\n" into one message.
// The watermark of a group is the end offset of its LAST line, so a restart
// never re-reads an emitted group and a crash re-reads an uncommitted one
// (at-least-once). A group flushes on a new group-starting line, timeout_ms,
// max_lines, max_bytes, rotation/truncation and stop-mode EOF — deliberately
// NOT on Close: an uncommitted group is re-read after a restart.
//
// Every message carries meta.file_path and meta.host; when the basename has
// the container-log shape <pod>_<namespace>_<container>-<id>.log, meta.pod /
// meta.namespace / meta.container are parsed from the path (no k8s API; a
// host file that happens to match the shape just gets harmless extra fields).
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
	multiline     *multilineConfig // nil = one message per line (v1 behavior)
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

	// Health counters (registry.CounterSource). All writes happen under mu;
	// the atomics exist so ops can poll Counters() without taking the lock
	// the poll loop holds across emit (backpressure must not block status).
	linesRead       atomic.Int64
	linesSkipped    atomic.Int64
	linesTruncated  atomic.Int64
	rotations       atomic.Int64
	multilineMerges atomic.Int64
}

// multilineConfig is the compiled multiline block.
type multilineConfig struct {
	pattern  *regexp.Regexp
	negate   bool
	before   bool // match == "before": a hit starts a group instead of continuing one
	maxLines int
	maxBytes int
	timeout  time.Duration // 0 = no timeout flush (explicit timeout_ms: 0)
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

	// tail is the fingerprint of the consumed stream: the last
	// tailFingerprintBytes bytes ending at offset+partialBytes. It detects an
	// in-place rewrite that regrows past the read offset (copytruncate): the
	// `size < consumed` check cannot see that, but the consumed bytes change,
	// so a mismatch resets the file to 0 (duplicates, never loss).
	tail []byte

	// Multiline group state (zero unless multiline is configured): the joined
	// lines, the end offset of the LAST line in the group (the watermark
	// recorded when the group flushes) and the time the last line was added.
	// The buffer is reused across groups; flush clones before emitting.
	group      []byte
	groupLines int
	groupBytes int64
	groupLast  time.Time
	groupEnd   int64
	groupOpen  bool
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

// tailFingerprintBytes is how many bytes of the consumed stream the file
// source remembers to detect an in-place rewrite that regrew past the read
// offset (copytruncate): the `size < consumed` check cannot see it, but the
// consumed bytes change. 64 bytes makes an accidental match on real log
// content vanishingly unlikely while keeping the per-poll verification read
// tiny.
const tailFingerprintBytes = 64

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
			// Groups are flushed before a descriptor closes, so an open group
			// here means a flush path was missed: flush it now rather than
			// leave read-but-uncommitted lines behind.
			if e.groupOpen {
				em, err := s.flushGroupLocked(e, emit)
				if em {
					emitted = true
				}
				if err != nil {
					return emitted, err
				}
			}
			continue
		}
		em, err := s.drainLocked(e, emit, now)
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
		if e.removed && e.groupOpen && e.partialBytes == 0 && e.offset >= e.size {
			// A file that no longer matches the glob is final at EOF: flush
			// its open group in the poll that noticed the rotation, so the
			// old content cannot be lost to clean_removed (design §2.4).
			em, err := s.flushGroupLocked(e, emit)
			if em {
				emitted = true
			}
			if err != nil {
				return emitted, err
			}
		}
	}
	em, err := s.reapLocked(now, emit)
	if em {
		emitted = true
	}
	if err != nil {
		return emitted, err
	}
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
			s.rotations.Add(1)
		}
	}
	s.byPath[path] = id
	if e.path != path {
		e.path = path
	}
	if e.f == nil {
		e.f = f
		e.partial, e.partialBytes, e.oversize = nil, 0, false
		if s.truncatedLocked(e, fi.Size()) {
			// Rewritten while the descriptor was closed (copytruncate):
			// restart from 0. The open group was flushed when the descriptor
			// closed, so nothing is carried across the reset (nil emitter).
			if _, err := s.resetTruncatedLocked(e, nil); err != nil {
				_ = f.Close()
				e.f = nil
				return nil
			}
		}
		if _, err := e.f.Seek(e.offset, io.SeekStart); err != nil {
			_ = f.Close()
			e.f = nil
			return nil
		}
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

// observeTail records the chunk in the rolling tail fingerprint: the last
// tailFingerprintBytes bytes of the consumed stream. Every byte read is
// consumed (into a complete line or the partial line), so appending the read
// chunks in order keeps the fingerprint aligned with offset+partialBytes.
func (e *trackedFile) observeTail(chunk []byte) {
	if len(chunk) >= tailFingerprintBytes {
		e.tail = append(e.tail[:0], chunk[len(chunk)-tailFingerprintBytes:]...)
		return
	}
	e.tail = append(e.tail, chunk...)
	if len(e.tail) > tailFingerprintBytes {
		e.tail = e.tail[len(e.tail)-tailFingerprintBytes:]
	}
}

// truncatedLocked reports whether the file was rewritten in place (copytruncate):
// its size fell below the consumed extent (offset+partialBytes), or — when the
// rewrite already regrew past that extent, which the size check cannot see —
// the consumed tail fingerprint no longer matches the bytes on disk. Either way
// the read position no longer describes the file and the source must reset to 0
// (duplicates, never loss).
func (s *fileSource) truncatedLocked(e *trackedFile, size int64) bool {
	end := e.offset + e.partialBytes
	if size < end {
		return true
	}
	if len(e.tail) == 0 || e.f == nil {
		return false
	}
	n := int64(len(e.tail))
	if end < n {
		return false // defensive: the tail can never exceed the consumed extent
	}
	buf := s.scratch[:n]
	if _, err := e.f.ReadAt(buf, end-n); err != nil {
		return false // cannot verify (transient read error): never reset on it
	}
	return !bytes.Equal(buf, e.tail)
}

// resetTruncatedLocked restarts a rewritten or truncated file from 0: the open
// multiline group is flushed first (its lines belong to the pre-truncation
// content), then the read position, the partial line and the tail fingerprint
// are cleared. A nil emit skips the group flush (the reopen path cannot have an
// open group: closing flushes it).
func (s *fileSource) resetTruncatedLocked(e *trackedFile, emit func(registry.Message) error) (bool, error) {
	var emitted bool
	if e.groupOpen {
		if emit != nil {
			em, err := s.flushGroupLocked(e, emit)
			if err != nil {
				return em, err
			}
			emitted = em
		} else {
			// No emitter available (the reopen path): closing already flushed
			// any group, so an open group here is defensive-only — drop it
			// rather than carry pre-truncation lines across the reset.
			e.group, e.groupLines, e.groupBytes, e.groupLast, e.groupEnd, e.groupOpen = nil, 0, 0, time.Time{}, 0, false
		}
	}
	e.offset, e.committed = 0, 0
	e.partial, e.partialBytes, e.oversize = nil, 0, false
	e.tail = e.tail[:0]
	if e.f != nil {
		if _, err := e.f.Seek(0, io.SeekStart); err != nil {
			return emitted, nil
		}
	}
	return emitted, nil
}

// drainLocked reads one open file to EOF, feeding complete lines through the
// aggregator (or emitting them one by one when multiline is off). It returns
// whether anything was emitted and the first refusal error.
func (s *fileSource) drainLocked(e *trackedFile, emit func(registry.Message) error, now time.Time) (bool, error) {
	if fi, err := e.f.Stat(); err == nil {
		if s.truncatedLocked(e, fi.Size()) {
			// copytruncate: the file was rewritten in place. Flush the open
			// group FIRST — its lines belong to the pre-truncation content
			// and the offset reset below would otherwise re-read them — then
			// restart from 0 (duplicates are acceptable, a stalled tail is
			// not).
			em, err := s.resetTruncatedLocked(e, emit)
			if err != nil {
				return em, err
			}
		}
		e.size, e.mtime = fi.Size(), fi.ModTime()
	}

	emitted := false
	for {
		n, rerr := e.f.Read(s.scratch)
		if n > 0 {
			e.lastRead = now
			e.observeTail(s.scratch[:n])
			em, err := s.consumeLocked(e, s.scratch[:n], now, emit)
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
			// Read failure (file replaced mid-read, lock violation): flush the
			// open group (its lines are read but uncommitted, and the reopen
			// seeks to the read watermark, not the group start), drop the
			// descriptor and let the next scan reopen it from the watermark.
			em, ferr := s.flushGroupLocked(e, emit)
			if ferr != nil {
				return emitted || em, ferr
			}
			s.closeLocked(e)
			e.reopen = true
			return emitted || em, nil
		}
	}

	if s.onEOF == "stop" && e.partialBytes > 0 {
		// stop reads COMPLETE files: an unterminated last line is fed as the
		// final line at EOF (tail would wait for its newline).
		oversize := e.oversize
		lineBytes := e.partialBytes
		e.offset += lineBytes
		em, err := s.feedLineLocked(e, e.partial, lineBytes, oversize, now, emit)
		e.partial, e.partialBytes, e.oversize = nil, 0, false
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
	}
	if s.onEOF == "stop" && e.partialBytes == 0 && e.offset >= e.size && e.groupOpen {
		// stop reads COMPLETE files: a trailing group is flushed at EOF so
		// the batch run commits every line before it reports exhausted.
		em, err := s.flushGroupLocked(e, emit)
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
	}
	if s.multiline != nil && e.groupOpen && s.multiline.timeout > 0 && now.Sub(e.groupLast) >= s.multiline.timeout {
		// The timeout flush runs on every poll: a group whose file produced
		// no new data must still be emitted (design §2.4).
		em, err := s.flushGroupLocked(e, emit)
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
func (s *fileSource) consumeLocked(e *trackedFile, chunk []byte, now time.Time, emit func(registry.Message) error) (bool, error) {
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
		// feed it through the policy and (when configured) the aggregator.
		lineBytes := e.partialBytes + 1
		e.offset += lineBytes
		em, err := s.feedLineLocked(e, e.partial, lineBytes, e.oversize, now, emit)
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

// feedLineLocked is the single entry for every complete line: it counts the
// line, applies the oversize policy and, when multiline is configured, routes
// it through the group classifier (design §2.4). A line dropped by
// oversize: skip has no effect on the open group.
func (s *fileSource) feedLineLocked(e *trackedFile, line []byte, lineBytes int64, oversize bool, now time.Time, emit func(registry.Message) error) (bool, error) {
	s.linesRead.Add(1)
	if oversize {
		if s.oversize == "skip" {
			s.linesSkipped.Add(1)
			s.warnOversize(e, lineBytes)
			return false, nil
		}
		s.linesTruncated.Add(1)
		s.warnOversize(e, lineBytes)
	} else {
		line = bytes.TrimRight(line, "\r")
	}

	if s.multiline == nil {
		if len(bytes.TrimSpace(line)) == 0 {
			return false, nil // blank line: consumed, not emitted (v1 behavior)
		}
		return s.emitMessageLocked(e, line, emit)
	}

	// Multiline: classify the line. A blank line is a candidate like any
	// other; a group made only of whitespace is dropped at flush.
	hit := s.multiline.pattern.Match(line)
	if s.multiline.negate {
		hit = !hit
	}
	continuation := hit
	if s.multiline.before {
		continuation = !hit
	}
	if e.groupOpen && continuation {
		// The caps flush the open group early and start a new one with this
		// line (design §2.4); a single line over max_bytes still becomes its
		// own group, because the next line triggers the same check.
		if e.groupLines >= s.multiline.maxLines || e.groupBytes+int64(len(line))+1 > int64(s.multiline.maxBytes) {
			em, err := s.flushGroupLocked(e, emit)
			if err != nil {
				return em, err
			}
			s.startGroupLocked(e, line, now)
			return em, nil
		}
		s.multilineMerges.Add(1)
		e.group = append(e.group, '\n')
		e.group = append(e.group, line...)
		e.groupLines++
		e.groupBytes = int64(len(e.group))
		e.groupLast = now
		e.groupEnd = e.offset
		return false, nil
	}
	// A new group starts here: flush the open one first.
	em, err := s.flushGroupLocked(e, emit)
	if err != nil {
		return em, err
	}
	s.startGroupLocked(e, line, now)
	return em, nil
}

func (s *fileSource) startGroupLocked(e *trackedFile, line []byte, now time.Time) {
	e.group = append(e.group[:0], line...)
	e.groupLines = 1
	e.groupBytes = int64(len(e.group))
	e.groupLast = now
	e.groupEnd = e.offset
	e.groupOpen = true
}

// flushGroupLocked emits the open group as ONE message and records the end
// offset of its LAST line in pending, so a restart resumes after the whole
// group. A group made only of whitespace lines is consumed without a message.
// Flush triggers: a new group-starting line, max_lines/max_bytes, timeout_ms,
// rotation/truncation and stop-mode EOF — never Close: an uncommitted group
// is re-read after a restart (at-least-once, never loss).
func (s *fileSource) flushGroupLocked(e *trackedFile, emit func(registry.Message) error) (bool, error) {
	if !e.groupOpen {
		return false, nil
	}
	buf, end := e.group, e.groupEnd
	e.group, e.groupLines, e.groupBytes, e.groupLast, e.groupEnd, e.groupOpen = nil, 0, 0, time.Time{}, 0, false
	if len(bytes.TrimSpace(buf)) == 0 {
		return false, nil
	}
	s.nextSeq++
	seq := s.nextSeq
	s.pending[seq] = filePending{id: e.id, end: end}
	msg := registry.Message{
		Raw:     bytes.Clone(buf),
		Meta:    s.messageMeta(e),
		SrcName: "file",
		SrcSeq:  seq,
	}
	if err := emit(msg); err != nil {
		return true, err
	}
	return true, nil
}

// emitMessageLocked emits one single-line message and records its end offset
// in pending so Commit folds it into the file's watermark.
func (s *fileSource) emitMessageLocked(e *trackedFile, line []byte, emit func(registry.Message) error) (bool, error) {
	end := e.offset
	s.nextSeq++
	seq := s.nextSeq
	s.pending[seq] = filePending{id: e.id, end: end}
	msg := registry.Message{
		Raw:     bytes.Clone(line),
		Meta:    s.messageMeta(e),
		SrcName: "file",
		SrcSeq:  seq,
	}
	if err := emit(msg); err != nil {
		return true, err
	}
	return true, nil
}

// messageMeta builds the per-message meta map: the matched path, the
// configured host and — when the basename has the container-log shape
// <pod>_<namespace>_<container>-<id>.log — the parsed Kubernetes fields. The
// parse is purely path-derived (no k8s API); a host file that happens to
// match the shape just gets harmless extra fields (design §2.3, P3).
func (s *fileSource) messageMeta(e *trackedFile) map[string]any {
	meta := map[string]any{"file_path": e.path, "host": s.host}
	if pod, namespace, container, ok := parseContainerPath(e.path); ok {
		meta["pod"], meta["namespace"], meta["container"] = pod, namespace, container
	}
	return meta
}

// containerLogName matches the container-log basename the kubelet writes:
// pod, namespace and container names cannot contain underscores, and the
// runtime id is lowercase hex (64 chars for Docker/containerd; any length of
// hex is accepted, so the id itself is not captured).
var containerLogName = regexp.MustCompile(`^([^_]+)_([^_]+)_([^_]+)-[0-9a-f]+\.log$`)

func parseContainerPath(path string) (pod, namespace, container string, ok bool) {
	m := containerLogName.FindStringSubmatch(filepath.Base(path))
	if m == nil {
		return "", "", "", false
	}
	return m[1], m[2], m[3], true
}

// warnOversize logs at the first few oversized lines and at powers of two
// after that, so a pathological file cannot turn the warning into a second
// data stream. The exact counts are exposed through Counters().
func (s *fileSource) warnOversize(e *trackedFile, lineBytes int64) {
	if s.warnf == nil {
		return
	}
	n := s.linesTruncated.Load() + s.linesSkipped.Load()
	if n <= 3 || n&(n-1) == 0 {
		s.warnf("file source: %s: line of %d bytes exceeds max_line_bytes=%d (%s)",
			e.path, lineBytes, s.maxLineBytes, s.oversize)
	}
}

// reapLocked closes descriptors that have been idle for close_inactive and
// forgets removed files according to clean_removed. An open multiline group
// is flushed before its descriptor closes (a group never outlives its
// descriptor: the timeout would flush it eventually, but close is the last
// moment the lines can be emitted without a re-read). Closing discards the
// in-memory half-line; a reopen seeks back to the read watermark and re-reads
// it from the file, so nothing is lost while the file remains.
func (s *fileSource) reapLocked(now time.Time, emit func(registry.Message) error) (bool, error) {
	if s.closeInactive == 0 && !s.cleanRemoved {
		return false, nil
	}
	emitted := false
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
		em, err := s.flushGroupLocked(e, emit)
		if em {
			emitted = true
		}
		if err != nil {
			return emitted, err
		}
		s.closeLocked(e)
		if e.removed && s.cleanRemoved {
			s.forgetLocked(e)
		}
	}
	return emitted, nil
}

func (s *fileSource) closeLocked(e *trackedFile) {
	if e.f != nil {
		_ = e.f.Close()
		e.f = nil
	}
	// Closing discards the in-memory half-line: a reopen seeks to the read
	// watermark and re-reads it, so the consumed extent shrinks to offset and
	// the tail fingerprint must shrink with it (its last partialBytes bytes
	// are beyond the new extent and would miscompare on reopen).
	drop := e.partialBytes
	e.partial, e.partialBytes, e.oversize = nil, 0, false
	if drop > 0 && len(e.tail) > 0 {
		if drop >= int64(len(e.tail)) {
			e.tail = e.tail[:0]
		} else {
			e.tail = e.tail[:len(e.tail)-int(drop)]
		}
	}
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

// Close releases every descriptor. It deliberately does NOT flush an open
// multiline group: the group is uncommitted, so a restart re-reads it from
// the persisted watermark (at-least-once, never loss; design §2.4).
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

// Counters implements registry.CounterSource: the monotonic health counters
// ops polls into telemetry. The names are the plugin's contract and are
// documented in docs/collector.md:
//
//	lines_read       complete lines parsed (blank and oversize included)
//	lines_skipped    oversize: skip drops
//	lines_truncated  oversize: truncate emissions
//	rotations        files replaced at a matched path (identity change)
//	multiline_merges continuation lines appended to an open group
func (s *fileSource) Counters() map[string]int64 {
	return map[string]int64{
		"lines_read":       s.linesRead.Load(),
		"lines_skipped":    s.linesSkipped.Load(),
		"lines_truncated":  s.linesTruncated.Load(),
		"rotations":        s.rotations.Load(),
		"multiline_merges": s.multilineMerges.Load(),
	}
}

// hasGlobMeta reports whether path carries glob metacharacters. A meta-free
// path is a single file, which is the shape the v1 state compatibility and the
// pre-v2 tests rely on.
func hasGlobMeta(path string) bool { return strings.ContainsAny(path, "*?[") }

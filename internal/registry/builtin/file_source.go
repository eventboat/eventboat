package builtin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

type fileSourceConfig struct {
	Path      string `json:"path" schema:"minLen=1,desc=file to tail, one message per line"`
	PollEvery int    `json:"poll_every_ms" schema:"min=10,default=250"`
	StartAt   string `json:"start_at" schema:"enum=beginning|end,default=beginning"`
	OnEOF     string `json:"on_eof" schema:"enum=tail|stop,default=tail,desc=tail keeps polling for appended lines; stop finishes the source once the file is read to its end (complete batch files)"`
}

func registerFileSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "file", 1, []string{"pull", "finite"}, func(c fileSourceConfig) (registry.Source, error) {
		return &fileSource{
			path:      c.Path,
			pollEvery: time.Duration(c.PollEvery) * time.Millisecond,
			startAt:   c.StartAt,
			onEOF:     c.OnEOF,
		}, nil
	})
}

// fileSource tails a file line by line. Commit state is the committed byte
// offset; the engine restores it via Init and advances it via Commit, which
// makes the file source genuinely at-least-once across restarts.
//
// Completion (v1.24 contract): with on_eof:stop the source returns nil once
// the file has been read to its end — the shape that makes a batch run or a
// job run finish. stop is for COMPLETE files (written before the run starts);
// the default on_eof:tail never returns on its own. A missing file is an
// error under stop (a batch run must not sit silent) and a wait-under-poll
// under tail.
type fileSource struct {
	path      string
	pollEvery time.Duration
	startAt   string
	onEOF     string

	mu            sync.Mutex
	f             *os.File
	reader        *bufio.Reader
	nextOffset    int64 // offset of the next unread byte
	committedOff  int64
	pending       map[int64]int64 // srcSeq -> emitted end offset
	nextSeq       int64
	lastCommitted int64 // highest srcSeq already folded by Commit
}

func (s *fileSource) Init(state []byte) error {
	if len(state) == 0 {
		return nil
	}
	var st struct {
		Offset int64 `json:"offset"`
	}
	if err := json.Unmarshal(state, &st); err != nil {
		return fmt.Errorf("file source: bad state: %w", err)
	}
	s.committedOff = st.Offset
	return nil
}

func (s *fileSource) Run(ctx context.Context, emit func(registry.Message)) error {
	if f, err := os.Open(s.path); err != nil {
		if s.onEOF == "stop" {
			// A batch/job run must not sit silent on a missing file.
			return fmt.Errorf("file source: open %s: %w", s.path, err)
		}
		// tail mode: nothing to tail yet — keep polling, pump reopens.
	} else {
		s.f = f
		switch {
		case s.startAt == "end" && s.committedOff == 0:
			if end, err := f.Seek(0, io.SeekEnd); err == nil {
				s.nextOffset = end
			}
		case s.committedOff > 0:
			if _, err := f.Seek(s.committedOff, io.SeekStart); err == nil {
				s.nextOffset = s.committedOff
			}
		}
		if _, err := f.Seek(s.nextOffset, io.SeekStart); err == nil {
			s.reader = bufio.NewReader(f)
		} else {
			s.reader = bufio.NewReader(f)
		}
	}
	s.pending = map[int64]int64{}

	tick := time.NewTicker(s.pollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil // cancelled: a voluntary stop, not a failure
		case <-tick.C:
			emitted := s.pump(ctx, emit)
			if s.onEOF == "stop" && !emitted && s.atEOF() {
				return nil // whole file read: exhausted
			}
		}
	}
}

// atEOF reports whether the file has been read to its end. A stat failure
// (the file transiently locked or replaced) skips the check this tick —
// stop mode assumes the file stays readable, not that it vanishes.
func (s *fileSource) atEOF() bool {
	fi, err := os.Stat(s.path)
	if err != nil {
		return false
	}
	return fi.Size() <= s.nextOffset
}

func (s *fileSource) pump(ctx context.Context, emit func(registry.Message)) (emitted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reader == nil {
		f, err := os.Open(s.path)
		if err != nil {
			return false
		}
		if _, err := f.Seek(s.nextOffset, io.SeekStart); err != nil {
			_ = f.Close()
			return false
		}
		s.f = f
		s.reader = bufio.NewReader(f)
	}
	for {
		line, err := s.reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimRight(line, "\r\n")
			if len(bytes.TrimSpace(trimmed)) > 0 {
				s.nextSeq++
				end := s.nextOffset + int64(len(line))
				s.pending[s.nextSeq] = end
				emit(registry.Message{Raw: trimmed, SrcName: "file", SrcSeq: s.nextSeq})
				emitted = true
			}
			s.nextOffset += int64(len(line))
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// Read error (possibly file replaced): reopen on next poll.
				_ = s.f.Close()
				s.f = nil
				s.reader = nil
			}
			return emitted
		}
	}
}

func (s *fileSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Watermark-bounded scan (see kafkaSource.Commit): Commit runs on every
	// frontier advance and pending entries are deleted during the scan, so
	// everything below lastCommitted is already drained.
	for seq := s.lastCommitted + 1; seq <= throughSrcSeq; seq++ {
		if end, ok := s.pending[seq]; ok {
			delete(s.pending, seq)
			if end > s.committedOff {
				s.committedOff = end
			}
		}
	}
	if throughSrcSeq > s.lastCommitted {
		s.lastCommitted = throughSrcSeq
	}
	st, _ := json.Marshal(struct {
		Offset int64 `json:"offset"`
	}{Offset: s.committedOff})
	return st, nil
}

func (s *fileSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

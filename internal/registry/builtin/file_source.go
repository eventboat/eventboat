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
}

func registerFileSource(reg *registry.Registry) error {
	return registry.RegisterSourceT(reg, "file", 1, nil, func(c fileSourceConfig) (registry.Source, error) {
		return &fileSource{
			path:      c.Path,
			pollEvery: time.Duration(c.PollEvery) * time.Millisecond,
			startAt:   c.StartAt,
		}, nil
	})
}

// fileSource tails a file line by line. Commit state is the committed byte
// offset; the engine restores it via Init and advances it via Settled, which
// makes the file source genuinely at-least-once across restarts.
type fileSource struct {
	path      string
	pollEvery time.Duration
	startAt   string

	mu           sync.Mutex
	f            *os.File
	reader       *bufio.Reader
	nextOffset   int64 // offset of the next unread byte
	committedOff int64
	pending      map[int64]int64 // srcSeq -> emitted end offset
	nextSeq      int64
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

func (s *fileSource) Run(ctx context.Context, emit func(registry.Message)) {
	f, err := os.Open(s.path)
	if err != nil {
		return // missing file: nothing to tail yet; a future poll could reopen
	}
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
	s.pending = map[int64]int64{}

	tick := time.NewTicker(s.pollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.pump(ctx, emit)
		}
	}
}

func (s *fileSource) pump(ctx context.Context, emit func(registry.Message)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reader == nil {
		f, err := os.Open(s.path)
		if err != nil {
			return
		}
		if _, err := f.Seek(s.nextOffset, io.SeekStart); err != nil {
			_ = f.Close()
			return
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
			return
		}
	}
}

func (s *fileSource) Settled(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for seq := int64(1); seq <= throughSrcSeq; seq++ {
		if end, ok := s.pending[seq]; ok {
			delete(s.pending, seq)
			if end > s.committedOff {
				s.committedOff = end
			}
		}
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

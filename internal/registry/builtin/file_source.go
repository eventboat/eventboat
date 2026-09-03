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

const fileSourceSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["path"],
  "properties": {
    "path":       { "type": "string", "minLength": 1, "description": "file to tail, one message per line" },
    "poll_every_ms": { "type": "integer", "minimum": 10, "default": 250 },
    "start_at":   { "type": "string", "enum": ["beginning", "end"], "default": "beginning" }
  },
  "additionalProperties": false
}`

func registerFileSource(reg *registry.Registry) error {
	return reg.RegisterSource("file", fileSourceSchema, nil, func(cfg map[string]any) (registry.Source, error) {
		path, _ := cfg["path"].(string)
		poll := intMs(cfg["poll_every_ms"], 250)
		startAt, _ := cfg["start_at"].(string)
		if startAt == "" {
			startAt = "beginning"
		}
		return &fileSource{path: path, pollEvery: time.Duration(poll) * time.Millisecond, startAt: startAt}, nil
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
	sawEOF       bool
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
			f.Close()
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
				s.f.Close()
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

func intMs(v any, def int) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	}
	return def
}

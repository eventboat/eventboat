package builtin

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/eventboat/eventboat/internal/registry"
)

type fileSinkConfig struct {
	Path string `json:"path" schema:"desc=output file (JSON lines)"`
}

func registerFileSink(reg *registry.Registry) error {
	return registry.RegisterSinkT(reg, "file", 1, func(c fileSinkConfig) (registry.Sink, error) {
		return &fileSink{path: c.Path}, nil
	})
}

// fileSink appends each message as one line (the engine-encoded bytes plus a
// newline). Roll-over is a P1 concern (README records the trim).
type fileSink struct {
	path string

	mu     sync.Mutex
	f      *os.File
	writer *bufio.Writer
}

func (s *fileSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		if dir := filepath.Dir(s.path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("file sink: %w", err)
			}
		}
		f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("file sink: %w", err)
		}
		s.f = f
		s.writer = bufio.NewWriter(f)
	}
	for _, m := range msgs {
		if _, err := s.writer.Write(encodedBytes(m)); err != nil {
			return fmt.Errorf("file sink: %w", err)
		}
		if err := s.writer.WriteByte('\n'); err != nil {
			return fmt.Errorf("file sink: %w", err)
		}
	}
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("file sink: flush: %w", err)
	}
	return nil
}

func (s *fileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

type dropSinkConfig struct{}

func registerDropSink(reg *registry.Registry) error {
	return registry.RegisterSinkT(reg, "drop", 1, func(c dropSinkConfig) (registry.Sink, error) {
		return &dropSink{}, nil
	})
}

// dropSink discards every message: the explicit "send to /dev/null" edge.
type dropSink struct{}

func (s *dropSink) Write(ctx context.Context, msgs []registry.Message) error { return nil }
func (s *dropSink) Close() error                                             { return nil }

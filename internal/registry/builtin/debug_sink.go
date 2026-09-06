package builtin

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/eventboat/eventboat/internal/registry"
)

type debugSinkConfig struct {
	Prefix string `json:"prefix" schema:"optional,desc=label printed before each line (tells fan-out branches apart)"`
}

func registerDebugSink(reg *registry.Registry) error {
	return registry.RegisterSinkT(reg, "debug", 1, func(c debugSinkConfig) (registry.Sink, error) {
		return &debugSink{w: os.Stderr, prefix: c.Prefix}, nil
	})
}

// debugSink prints each message as one line on stderr — the "just show me the
// data" edge for pipeline debugging. stderr, not stdout, because the CLI
// contract keeps stdout for data; a sink must never interleave with it. Not a
// production edge: no roll-over, no buffering, and a closed stderr fails the
// batch like any sink error (dead letters / retries apply).
type debugSink struct {
	w      io.Writer
	prefix string

	mu sync.Mutex
}

func (s *debugSink) Write(ctx context.Context, msgs []registry.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := ""
	if s.prefix != "" {
		prefix = s.prefix + " "
	}
	for _, m := range msgs {
		if _, err := fmt.Fprintf(s.w, "%s%s\n", prefix, encodedBytes(m)); err != nil {
			return fmt.Errorf("debug sink: %w", err)
		}
	}
	return nil
}

func (s *debugSink) Close() error { return nil }

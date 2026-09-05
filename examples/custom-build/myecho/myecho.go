// Package myecho is a demo Eventboat sink plugin: it appends each event's
// payload as one line to a file. It registers itself through pkg/plugin —
// the compile-time extension surface — and deliberately imports nothing
// else of Eventboat, so packages that depend on it stay light.
package myecho

import (
	"context"
	"os"
	"path/filepath"

	"github.com/eventboat/eventboat/pkg/plugin"
)

// Config is the plugin block's contract: the JSON Schema is generated from
// these tags and the block is validated against it before the factory runs.
type Config struct {
	Path string `json:"path" schema:"minLen=1,desc=file to append one payload per line"`
}

type echoSink struct{ path string }

// Write appends one line per message. Batching is the engine's concern;
// the sink just persists the batch it receives.
func (s *echoSink) Write(ctx context.Context, msgs []plugin.Message) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, m := range msgs {
		payload := m.Out // the engine's encoded form; falls back at the caller's
		if len(payload) == 0 {
			payload = m.Raw
		}
		if _, err := f.Write(append(payload, '\n')); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (s *echoSink) Close() error { return nil }

func init() {
	if err := plugin.RegisterSink("myecho", 1, func(c Config) (*echoSink, error) {
		return &echoSink{path: c.Path}, nil
	}); err != nil {
		panic(err) // unreachable: the static name/version/schema are valid
	}
}

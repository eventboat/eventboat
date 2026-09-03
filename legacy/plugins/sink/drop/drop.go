package drop

import (
	"context"

	"github.com/riverpod/riverpod/internal/basestage"
	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/registry"
	"github.com/riverpod/riverpod/internal/stage"
)

func init() {
	registry.RegisterSink("drop", func(id string, cfg map[string]any) (stage.Sink, error) {
		return &Sink{
			Base: basestage.Base{IDVal: id, KindVal: stage.KindSink, TypeVal: "drop"},
		}, nil
	})
}

type Sink struct {
	basestage.Base
}

func (s *Sink) Write(context.Context, []*message.Message) error { return nil }
func (s *Sink) Flush(context.Context) error                     { return nil }

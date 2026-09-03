package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/riverpod/riverpod/internal/basestage"
	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/stage"
	"github.com/riverpod/riverpod/internal/topology"
)

// cancelAwareTransform fails processing once the context is cancelled, so a
// batch consumed during shutdown is nacked through the process-error path.
type cancelAwareTransform struct {
	basestage.Base
}

func (t *cancelAwareTransform) Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return batch, nil
}

func TestRunTransform_NacksQueuedBatchOnShutdown(t *testing.T) {
	p := &Pipeline{
		ir:             &topology.TopologyIR{},
		stageErrorMode: map[string]string{},
		stages: map[string]stage.Stage{
			"tr": &cancelAwareTransform{Base: basestage.Base{IDVal: "tr", KindVal: stage.KindTransform, TypeVal: "cancel_aware"}},
		},
		graph: &runtimeGraph{
			nodes:    map[string]*runtimeNode{},
			outgoing: map[string][]topology.EdgeIR{},
		},
	}
	node := &runtimeNode{batchIn: make(chan []*message.Message, 1)}

	var acks atomic.Int32
	var ackErr error
	msg := message.New([]byte("x"), nil)
	msg.SetAckFn(func(err error) {
		acks.Add(1)
		ackErr = err
	})
	node.batchIn <- []*message.Message{msg}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.runTransform(ctx, "tr", node)

	if acks.Load() != 1 {
		t.Fatalf("queued batch ack count = %d, want 1 (batch dropped on shutdown)", acks.Load())
	}
	if !errors.Is(ackErr, context.Canceled) {
		t.Fatalf("queued batch ack error = %v, want context.Canceled", ackErr)
	}
}

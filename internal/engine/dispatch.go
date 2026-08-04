package engine

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/edgesets/edgestream/internal/eql"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/topology"
)

type ackAggregator struct {
	pending   int32
	errStored int32
	firstErr  atomic.Value
	parent    *message.Message
}

// dispatchTransformOutputs routes transform outputs downstream and completes parent
// message Ack when all child branches finish (Benthos split model).
func (p *Pipeline) dispatchTransformOutputs(ctx context.Context, stageID string, inputs, outputs []*message.Message) {
	outCountByID := make(map[string]int, len(outputs))
	for _, m := range outputs {
		outCountByID[m.ID]++
	}

	passThroughDispatched := make(map[string]bool)
	aggs := make(map[string]*ackAggregator)

	for _, m := range inputs {
		count := outCountByID[m.ID]
		if count == 0 {
			m.Ack(nil)
			continue
		}
		if count == 1 {
			for _, o := range outputs {
				if o.ID == m.ID && o == m {
					p.dispatchFrom(ctx, stageID, o)
					passThroughDispatched[m.ID] = true
					break
				}
			}
		}
		if passThroughDispatched[m.ID] {
			continue
		}
		aggs[m.ID] = &ackAggregator{
			pending: int32(count),
			parent:  m,
		}
	}

	for _, o := range outputs {
		if passThroughDispatched[o.ID] {
			continue
		}
		agg := aggs[o.ID]
		if agg == nil {
			continue
		}
		child := o
		parent := agg.parent
		child.SetAckFn(func(err error) {
			if err != nil && atomic.CompareAndSwapInt32(&agg.errStored, 0, 1) {
				agg.firstErr.Store(err)
			}
			if atomic.AddInt32(&agg.pending, -1) == 0 {
				var ackErr error
				if stored := agg.firstErr.Load(); stored != nil {
					ackErr = stored.(error)
				}
				parent.Ack(ackErr)
			}
		})
		p.dispatchFrom(ctx, stageID, child)
	}
}

func (p *Pipeline) sendToInbound(ctx context.Context, edge topology.EdgeIR, msg *message.Message) {
	eb := p.graph.edgeInbounds[edgeKey(edge.From, edge.To)]
	if eb == nil {
		msg.Ack(fmt.Errorf("edge buffer %s->%s not found", edge.From, edge.To))
		return
	}
	dropped, reason, _ := eb.Enqueue(ctx, msg)
	if dropped && p.metrics != nil {
		p.metrics.IncEdgeDropped(p.ir.Name, edge.From, edge.To, reason)
	}
	if p.metrics != nil {
		size := eb.Len() + int(eb.DiskBytes())
		p.metrics.SetEdgeBuffer(p.ir.Name, edge.From, edge.To, size)
	}
}

func (p *Pipeline) evalCondition(ctx context.Context, prg *eql.Program, msg *message.Message) (bool, error) {
	if prg == nil {
		return true, nil
	}
	if err := p.ensureParsed(msg); err != nil {
		return false, err
	}
	_ = ctx
	return p.graph.evalCondition(prg, msg)
}

package engine

import (
	"context"
	"time"

	"github.com/riverpod/riverpod/internal/message"
	"github.com/riverpod/riverpod/internal/observability"
)

func (p *Pipeline) beginMessageLifecycle(ctx context.Context, msg *message.Message) error {
	if err := p.acquireInflight(ctx); err != nil {
		return err
	}
	start := time.Now()
	msg.WrapAckFn(func(err error) {
		p.releaseInflight(err, start)
	})
	return nil
}

func (p *Pipeline) acquireInflight(ctx context.Context) error {
	max := p.ir.Engine.MaxInflight
	for {
		cur := p.inflight.Load()
		if max <= 0 || cur < int32(max) {
			if p.inflight.CompareAndSwap(cur, cur+1) {
				if p.metrics != nil {
					p.metrics.IncInflight(p.ir.Name)
				}
				return nil
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (p *Pipeline) releaseInflight(err error, start time.Time) {
	p.inflight.Add(-1)
	if p.metrics == nil {
		return
	}
	p.metrics.DecInflight(p.ir.Name)
	p.metrics.RecordEvent(p.ir.Name, observability.EventStatus(err), time.Since(start))
}

func (p *Pipeline) InflightCount() int32 {
	return p.inflight.Load()
}

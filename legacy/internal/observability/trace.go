package observability

import "context"

// Tracer is a noop tracing skeleton for future OTLP integration (§10.4).
type Tracer struct{}

func NewTracer() *Tracer { return &Tracer{} }

func (t *Tracer) StartPipelineSpan(ctx context.Context, pipeline string) (context.Context, func()) {
	_ = pipeline
	return ctx, func() {}
}

func (t *Tracer) StartStageSpan(ctx context.Context, pipeline, stageID, kind string) (context.Context, func()) {
	_, _, _ = pipeline, stageID, kind
	return ctx, func() {}
}

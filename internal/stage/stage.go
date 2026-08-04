package stage

import (
	"context"
	"time"

	"github.com/edgesets/edgestream/internal/message"
)

type Kind int

const (
	KindSource Kind = iota
	KindTransform
	KindSink
)

func (k Kind) String() string {
	switch k {
	case KindSource:
		return "source"
	case KindTransform:
		return "transform"
	case KindSink:
		return "sink"
	default:
		return "unknown"
	}
}

type HealthStatus struct {
	Healthy bool
	Message string
	Since   time.Time
}

type Stage interface {
	ID() string
	Kind() Kind
	ComponentType() string
	Init(ctx context.Context) error
	Stop(ctx context.Context) error
	HealthCheck(ctx context.Context) HealthStatus
}

type Source interface {
	Stage
	Consume(ctx context.Context, out chan<- *message.Message) error
}

type AckingSource interface {
	Source
	OnAck(msg *message.Message, err error)
}

type PollingSource interface {
	Stage
	Poll(ctx context.Context) ([]*message.Message, error)
	Interval() time.Duration
}

type Transform interface {
	Stage
	Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error)
}

type Sink interface {
	Stage
	Write(ctx context.Context, msgs []*message.Message) error
	Flush(ctx context.Context) error
}

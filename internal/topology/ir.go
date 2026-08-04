package topology

import (
	"time"

	"github.com/edgesets/edgestream/internal/config"
)

// Stage kind constants — used for IR serialization and engine comparisons.
const (
	KindSource    = "source"
	KindTransform = "transform"
	KindSink      = "sink"
)

type TopologyIR struct {
	Name           string
	Engine         config.EngineConfig
	Stages         []StageIR
	Edges          []EdgeIR
	Codecs         map[string]CodecIR
	EdgeDefaults   config.EdgeAttrs
	DLQ            *config.DLQConfig
	Observability  config.ObservabilityConfig
	DeprecationWarnings []string
}

type StageIR struct {
	ID          string
	Kind        string
	Type        string
	Workers     int
	Predicate   string
	ErrorMode   string
	Decoder     *config.CodecRef
	Encoder     *config.CodecRef
	Batch       *config.BatchConfig
	Ordering    string
	MaxInFlight int
	Config      map[string]any
}

type CodecIR struct {
	Name   string
	Type   string
	Config map[string]any
}

type EdgeIR struct {
	From      string
	To        string
	Condition string
	Route     string
	Buffer    config.EdgeBufferConfig
	Delivery  *config.DeliverySpec
	Required  bool
}

func DefaultBuffer(cfg config.EdgeBufferConfig) config.EdgeBufferConfig {
	out := cfg
	if out.Type == "" {
		out.Type = "memory"
	}
	if out.Size == 0 {
		out.Size = 64
	}
	if out.Strategy == "" {
		out.Strategy = "block"
	}
	if out.DiskSyncInterval == "" {
		out.DiskSyncInterval = "500ms"
	}
	if out.DiskMaxSize == 0 {
		out.DiskMaxSize = 1 << 30
	}
	return out
}

func (e *EdgeIR) BufferSize() int {
	return DefaultBuffer(e.Buffer).Size
}

func (e *EdgeIR) BufferStrategy() string {
	return DefaultBuffer(e.Buffer).Strategy
}

func (s *StageIR) BatchTimeout() time.Duration {
	if s.Batch == nil || s.Batch.Timeout == "" {
		return 0
	}
	d, _ := time.ParseDuration(s.Batch.Timeout)
	return d
}

func (s *StageIR) BatchSize() int {
	if s.Batch == nil || s.Batch.Size == 0 {
		return 1
	}
	return s.Batch.Size
}

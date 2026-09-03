package config

import "time"

type PipelineConfig struct {
	APIVersion    string            `yaml:"apiVersion"`
	Kind          string            `yaml:"kind"`
	Metadata      map[string]string `yaml:"metadata"`
	Engine        EngineConfig      `yaml:"engine"`
	Steps         map[string]StepConfig `yaml:"steps"`
	Pipeline      []StageConfig     `yaml:"pipeline"`
	Codecs        []CodecConfig     `yaml:"codecs"`
	EdgeDefaults  EdgeAttrs         `yaml:"edgeDefaults"`
	DLQ           *DLQConfig        `yaml:"dlq"`
	Observability ObservabilityConfig `yaml:"observability"`
	Edges         []EdgeConfig      `yaml:"edges"`
}

type EngineConfig struct {
	MaxWorkers   int    `yaml:"max_workers"`
	MaxInflight  int    `yaml:"max_inflight"`
	ErrorMode    string `yaml:"error_mode"`
	DrainTimeout string `yaml:"drain_timeout"`
}

type DLQConfig struct {
	Sink                  string `yaml:"sink"`
	IncludeCurrentPayload bool   `yaml:"include_current_payload"`
}

type ObservabilityConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Health  HealthConfig  `yaml:"health"`
	Logging LoggingConfig `yaml:"logging"`
}

type HealthConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Port      int    `yaml:"port"`
	Liveness  string `yaml:"liveness"`
	Readiness string `yaml:"readiness"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Path    string `yaml:"path"`
}

type CodecConfig struct {
	Name   string         `yaml:"name"`
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

type CodecRef struct {
	Type   string         `yaml:"type"`
	Ref    string         `yaml:"ref"`
	Config map[string]any `yaml:"config"`
}

type BatchConfig struct {
	Size    int    `yaml:"size"`
	Timeout string `yaml:"timeout"`
	MaxBytes int   `yaml:"max_bytes"`
}

type StepConfig struct {
	StepType  string        `yaml:"step_type"`
	DependsOn DependsOnList `yaml:"depends_on"`
	Source    *SourceBlock  `yaml:"source"`
	Transform *TransformBlock `yaml:"transform"`
	Sink      *SinkBlock    `yaml:"sink"`
}

type StageConfig struct {
	ID        string        `yaml:"id"`
	Kind      string        `yaml:"kind"`
	Type      string        `yaml:"type"`
	DependsOn DependsOnList `yaml:"depends_on"`
	Decoder   *CodecRef     `yaml:"decoder"`
	Encoder   *CodecRef     `yaml:"encoder"`
	Workers     int           `yaml:"workers"`
	Predicate   string        `yaml:"predicate"`
	ErrorMode   string        `yaml:"error_mode"`
	Batch       *BatchConfig  `yaml:"batch"`
	Ordering  string        `yaml:"ordering"`
	MaxInFlight int         `yaml:"max_in_flight"`
	Config    map[string]any `yaml:"config"`
}

type SourceBlock struct {
	Type    string         `yaml:"type"`
	Decoder *CodecRef      `yaml:"decoder"`
	Config  map[string]any `yaml:"config"`
}

type TransformBlock struct {
	Type      string         `yaml:"type"`
	Predicate string         `yaml:"predicate"`
	Workers   int            `yaml:"workers"`
	ErrorMode string         `yaml:"error_mode"`
	Config    map[string]any `yaml:"config"`
}

type SinkBlock struct {
	Type        string         `yaml:"type"`
	Encoder     *CodecRef      `yaml:"encoder"`
	Batch       *BatchConfig   `yaml:"batch"`
	Ordering    string         `yaml:"ordering"`
	MaxInFlight int            `yaml:"max_in_flight"`
	Config      map[string]any `yaml:"config"`
}

type DependsOnList []DependsOnEntry

type DependsOnEntry struct {
	Upstream string
	Edge     *EdgeAttrs
}

type EdgeAttrs struct {
	Condition string           `yaml:"condition"`
	Route     string           `yaml:"route"`
	Buffer    *EdgeBufferConfig `yaml:"buffer"`
	Delivery  *DeliverySpec    `yaml:"delivery"`
	Required  *bool            `yaml:"required"`
}

type EdgeConfig struct {
	From      string           `yaml:"from"`
	To        string           `yaml:"to"`
	Condition string           `yaml:"condition"`
	Route     string           `yaml:"route"`
	Buffer    *EdgeBufferConfig `yaml:"buffer"`
	Delivery  *DeliverySpec    `yaml:"delivery"`
	Required  *bool            `yaml:"required"`
}

type EdgeBufferConfig struct {
	Type             string `yaml:"type"`
	Size             int    `yaml:"size"`
	Strategy         string `yaml:"strategy"`
	Key              []string `yaml:"key"`
	DiskPath         string `yaml:"disk_path"`
	DiskMaxSize      int64  `yaml:"disk_max_size"`
	DiskSyncInterval string `yaml:"disk_sync_interval"`
}

type DeliverySpec struct {
	Retry   *RetryConfig `yaml:"retry"`
	Timeout string       `yaml:"timeout"`
	DLQ     string       `yaml:"dlq"`
}

type RetryConfig struct {
	Max     int    `yaml:"max"`
	Backoff string `yaml:"backoff"`
}

func (b *BatchConfig) TimeoutDuration() (time.Duration, error) {
	if b == nil || b.Timeout == "" {
		return 0, nil
	}
	return time.ParseDuration(b.Timeout)
}

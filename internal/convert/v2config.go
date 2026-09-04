// Package convert implements the v2 → v3 migration tool (redesign-v3.md
// §7.3): it parses any archived v2 pipeline shape (read-only; legacy/ itself
// is never imported or modified — its internal packages are not importable
// across the module boundary, see redesign-v3-review-m4.md R1), translates
// the eql1 DSL to CEL predicates + Starlark scripts, and emits the v3
// three-section form plus a per-item migration report.
package convert

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// The structs below mirror legacy/internal/config/config.go (the archived v2
// loader) verbatim so that every historical writing style decodes; the
// normalization semantics in normalize.go follow legacy/internal/config/
// normalize.go. Read-only copies: legacy/ is frozen.

type v2PipelineConfig struct {
	APIVersion    string             `yaml:"apiVersion"`
	Kind          string             `yaml:"kind"`
	Metadata      map[string]string  `yaml:"metadata"`
	Engine        v2EngineConfig     `yaml:"engine"`
	Steps         map[string]v2Step  `yaml:"steps"`
	Pipeline      []v2Stage          `yaml:"pipeline"`
	Codecs        []v2CodecConfig    `yaml:"codecs"`
	EdgeDefaults  v2EdgeAttrs        `yaml:"edgeDefaults"`
	DLQ           *v2DLQConfig       `yaml:"dlq"`
	Edges         []v2EdgeConfig     `yaml:"edges"`
	HasDLQKey     bool               `yaml:"-"`
	HasObsKey     bool               `yaml:"-"`
	HasEngineKey  bool               `yaml:"-"`
	Observability map[string]any     `yaml:"observability"`
}

type v2EngineConfig struct {
	MaxWorkers   int    `yaml:"max_workers"`
	MaxInflight  int    `yaml:"max_inflight"`
	ErrorMode    string `yaml:"error_mode"`
	DrainTimeout string `yaml:"drain_timeout"`
}

type v2DLQConfig struct {
	Sink                  string `yaml:"sink"`
	IncludeCurrentPayload bool   `yaml:"include_current_payload"`
}

type v2CodecConfig struct {
	Name   string         `yaml:"name"`
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

// v2CodecRef keeps the legacy scalar/object dual shape via UnmarshalYAML.
type v2CodecRef struct {
	Type   string         `yaml:"type"`
	Ref    string         `yaml:"ref"`
	Config map[string]any `yaml:"config"`
}

func (c *v2CodecRef) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	if value.Kind == yaml.ScalarNode {
		c.Type = value.Value
		return nil
	}
	type plain v2CodecRef
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	*c = v2CodecRef(p)
	return nil
}

type v2BatchConfig struct {
	Size     int    `yaml:"size"`
	Timeout  string `yaml:"timeout"`
	MaxBytes int    `yaml:"max_bytes"`
}

type v2Step struct {
	DependsOn v2DependsOnList `yaml:"depends_on"`
	Source    *v2SourceBlock  `yaml:"source"`
	Transform *v2TransformBlk `yaml:"transform"`
	Sink      *v2SinkBlock    `yaml:"sink"`
}

type v2Stage struct {
	ID          string          `yaml:"id"`
	Kind        string          `yaml:"kind"`
	Type        string          `yaml:"type"`
	DependsOn   v2DependsOnList `yaml:"depends_on"`
	Decoder     *v2CodecRef     `yaml:"decoder"`
	Encoder     *v2CodecRef     `yaml:"encoder"`
	Workers     int             `yaml:"workers"`
	Predicate   string          `yaml:"predicate"`
	ErrorMode   string          `yaml:"error_mode"`
	Batch       *v2BatchConfig  `yaml:"batch"`
	Ordering    string          `yaml:"ordering"`
	MaxInFlight int             `yaml:"max_in_flight"`
	Config      map[string]any  `yaml:"config"`
}

type v2SourceBlock struct {
	Type    string         `yaml:"type"`
	Decoder *v2CodecRef    `yaml:"decoder"`
	Config  map[string]any `yaml:"config"`
}

type v2TransformBlk struct {
	Type      string         `yaml:"type"`
	Predicate string         `yaml:"predicate"`
	Workers   int            `yaml:"workers"`
	ErrorMode string         `yaml:"error_mode"`
	Config    map[string]any `yaml:"config"`
}

type v2SinkBlock struct {
	Type       string         `yaml:"type"`
	Encoder    *v2CodecRef    `yaml:"encoder"`
	Batch      *v2BatchConfig `yaml:"batch"`
	Ordering   string         `yaml:"ordering"`
	MaxInFlight int           `yaml:"max_in_flight"`
	Config     map[string]any `yaml:"config"`
}

type v2DependsOnList []v2DependsOnEntry

type v2DependsOnEntry struct {
	Upstream string
	Edge     *v2EdgeAttrs
}

// UnmarshalYAML accepts the three historical shapes (legacy normalize.go):
// sequence of scalars, sequence of single-key objects, or a mapping.
func (d *v2DependsOnList) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	switch value.Kind {
	case yaml.SequenceNode:
		entries := make([]v2DependsOnEntry, 0, len(value.Content))
		for _, item := range value.Content {
			switch item.Kind {
			case yaml.ScalarNode:
				entries = append(entries, v2DependsOnEntry{Upstream: item.Value})
			case yaml.MappingNode:
				if len(item.Content) != 2 {
					return fmt.Errorf("depends_on sequence item must be a single-key object")
				}
				upstream := item.Content[0].Value
				var attrs v2EdgeAttrs
				if err := item.Content[1].Decode(&attrs); err != nil {
					return err
				}
				entries = append(entries, v2DependsOnEntry{Upstream: upstream, Edge: &attrs})
			default:
				return fmt.Errorf("unsupported depends_on sequence element")
			}
		}
		*d = entries
		return nil
	case yaml.MappingNode:
		entries := make([]v2DependsOnEntry, 0, len(value.Content)/2)
		for i := 0; i < len(value.Content); i += 2 {
			upstream := value.Content[i].Value
			var attrs v2EdgeAttrs
			if err := value.Content[i+1].Decode(&attrs); err != nil {
				return err
			}
			entries = append(entries, v2DependsOnEntry{Upstream: upstream, Edge: &attrs})
		}
		*d = entries
		return nil
	default:
		return fmt.Errorf("depends_on must be a sequence or mapping")
	}
}

type v2EdgeAttrs struct {
	Condition string             `yaml:"condition"`
	Route     string             `yaml:"route"`
	Buffer    *v2EdgeBuffer      `yaml:"buffer"`
	Delivery  *v2DeliverySpec    `yaml:"delivery"`
	Required  *bool              `yaml:"required"`
}

type v2EdgeConfig struct {
	From      string          `yaml:"from"`
	To        string          `yaml:"to"`
	Condition string          `yaml:"condition"`
	Route     string          `yaml:"route"`
	Buffer    *v2EdgeBuffer   `yaml:"buffer"`
	Delivery  *v2DeliverySpec `yaml:"delivery"`
	Required  *bool           `yaml:"required"`
}

type v2EdgeBuffer struct {
	Type            string   `yaml:"type"`
	Size            int      `yaml:"size"`
	Strategy        string   `yaml:"strategy"`
	Key             []string `yaml:"key"`
	DiskPath        string   `yaml:"disk_path"`
	DiskMaxSize     int64    `yaml:"disk_max_size"`
	DiskSyncInterval string  `yaml:"disk_sync_interval"`
}

type v2DeliverySpec struct {
	Retry   *v2RetryConfig `yaml:"retry"`
	Timeout string         `yaml:"timeout"`
	DLQ     string         `yaml:"dlq"`
}

type v2RetryConfig struct {
	Max     int    `yaml:"max"`
	Backoff string `yaml:"backoff"`
}

package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func (d *DependsOnList) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	switch value.Kind {
	case yaml.SequenceNode:
		entries := make([]DependsOnEntry, 0, len(value.Content))
		for _, item := range value.Content {
			switch item.Kind {
			case yaml.ScalarNode:
				entries = append(entries, DependsOnEntry{Upstream: item.Value})
			case yaml.MappingNode:
				if len(item.Content) != 2 {
					return fmt.Errorf("depends_on sequence item must be a single-key object")
				}
				upstream := item.Content[0].Value
				var attrs EdgeAttrs
				if err := item.Content[1].Decode(&attrs); err != nil {
					return err
				}
				entries = append(entries, DependsOnEntry{Upstream: upstream, Edge: &attrs})
			default:
				return fmt.Errorf("unsupported depends_on sequence element")
			}
		}
		*d = entries
		return nil
	case yaml.MappingNode:
		entries := make([]DependsOnEntry, 0, len(value.Content)/2)
		for i := 0; i < len(value.Content); i += 2 {
			upstream := value.Content[i].Value
			var attrs EdgeAttrs
			if err := value.Content[i+1].Decode(&attrs); err != nil {
				return err
			}
			entries = append(entries, DependsOnEntry{Upstream: upstream, Edge: &attrs})
		}
		*d = entries
		return nil
	default:
		return fmt.Errorf("depends_on must be a sequence or mapping")
	}
}

func (c *CodecRef) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	if value.Kind == yaml.ScalarNode {
		c.Type = value.Value
		return nil
	}
	type plain CodecRef
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	*c = CodecRef(p)
	return nil
}

func (c CodecRef) ResolveType() string {
	if c.Ref != "" {
		return c.Ref
	}
	return c.Type
}

func (c *CodecRef) IsEmpty() bool {
	return c == nil || (c.Type == "" && c.Ref == "" && len(c.Config) == 0)
}

func MergeEdgeAttrs(base, override *EdgeAttrs) EdgeAttrs {
	out := EdgeAttrs{}
	if base != nil {
		out = *base
	}
	if override == nil {
		return out
	}
	if override.Condition != "" {
		out.Condition = override.Condition
	}
	if override.Route != "" {
		out.Route = override.Route
	}
	if override.Buffer != nil {
		out.Buffer = override.Buffer
	}
	if override.Delivery != nil {
		out.Delivery = override.Delivery
	}
	if override.Required != nil {
		out.Required = override.Required
	}
	return out
}

func RouteCondition(route string) string {
	return fmt.Sprintf(`metadata["er-route"] == %q`, route)
}

func ExpandDependsOn(stepID string, deps DependsOnList, defaults EdgeAttrs) ([]EdgeConfig, error) {
	var edges []EdgeConfig
	for _, dep := range deps {
		if dep.Upstream == "" {
			return nil, fmt.Errorf("step %q: empty depends_on upstream", stepID)
		}
		attrs := MergeEdgeAttrs(&defaults, dep.Edge)
		if attrs.Route != "" && attrs.Condition != "" {
			return nil, fmt.Errorf("step %q edge from %q: route and condition are mutually exclusive", stepID, dep.Upstream)
		}
		cond := attrs.Condition
		if attrs.Route != "" {
			cond = RouteCondition(attrs.Route)
		}
		required := true
		if attrs.Required != nil {
			required = *attrs.Required
		}
		edges = append(edges, EdgeConfig{
			From:      dep.Upstream,
			To:        stepID,
			Condition: cond,
			Route:     "",
			Buffer:    attrs.Buffer,
			Delivery:  attrs.Delivery,
			Required:  &required,
		})
	}
	return edges, nil
}

func PipelineName(cfg *PipelineConfig) string {
	if cfg.Metadata != nil {
		if name := cfg.Metadata["name"]; name != "" {
			return name
		}
	}
	return "unnamed"
}

func NormalizeSteps(cfg *PipelineConfig) ([]StageConfig, []EdgeConfig, error) {
	if len(cfg.Steps) == 0 {
		return nil, nil, fmt.Errorf("steps is empty")
	}
	if len(cfg.Pipeline) > 0 {
		return nil, nil, fmt.Errorf("steps and pipeline are mutually exclusive")
	}

	var stages []StageConfig
	var edges []EdgeConfig

	for stepName, step := range cfg.Steps {
		if step.Source != nil {
			stages = append(stages, StageConfig{
				ID:      stepName,
				Kind:    "source",
				Type:    step.Source.Type,
				Decoder: step.Source.Decoder,
				Config:  step.Source.Config,
			})
		}

		transformID := stepName
		if step.Transform != nil {
			stages = append(stages, StageConfig{
				ID:        transformID,
				Kind:      "transform",
				Type:      step.Transform.Type,
				Workers:   step.Transform.Workers,
				Predicate: step.Transform.Predicate,
				ErrorMode: step.Transform.ErrorMode,
				Config:    step.Transform.Config,
			})
		}

		sinkID := stepName
		if step.Sink != nil {
			if step.Transform != nil {
				sinkID = stepName + "-sink"
			}
			stages = append(stages, StageConfig{
				ID:          sinkID,
				Kind:        "sink",
				Type:        step.Sink.Type,
				Encoder:     step.Sink.Encoder,
				Batch:       step.Sink.Batch,
				Ordering:    step.Sink.Ordering,
				MaxInFlight: step.Sink.MaxInFlight,
				Config:      step.Sink.Config,
			})
			if step.Transform != nil {
				edges = append(edges, EdgeConfig{From: transformID, To: sinkID})
			}
		}

		depEdges, err := ExpandDependsOn(firstStageID(stepName, step), step.DependsOn, cfg.EdgeDefaults)
		if err != nil {
			return nil, nil, err
		}
		edges = append(edges, depEdges...)
	}

	return stages, edges, nil
}

func firstStageID(stepName string, step StepConfig) string {
	if step.Source != nil {
		return stepName
	}
	return stepName
}

func NormalizePipeline(cfg *PipelineConfig) ([]StageConfig, []EdgeConfig, error) {
	if len(cfg.Pipeline) == 0 {
		return nil, nil, fmt.Errorf("pipeline is empty")
	}
	if len(cfg.Steps) > 0 {
		return nil, nil, fmt.Errorf("steps and pipeline are mutually exclusive")
	}
	var edges []EdgeConfig
	for _, st := range cfg.Pipeline {
		depEdges, err := ExpandDependsOn(st.ID, st.DependsOn, cfg.EdgeDefaults)
		if err != nil {
			return nil, nil, err
		}
		edges = append(edges, depEdges...)
	}
	return cfg.Pipeline, edges, nil
}

func MergeDeprecatedEdges(edges []EdgeConfig, deprecated []EdgeConfig) ([]EdgeConfig, []string) {
	if len(deprecated) == 0 {
		return edges, nil
	}
	warnings := []string{"top-level edges: deprecated; use depends_on instead"}
	index := make(map[string]int, len(edges))
	for i, e := range edges {
		index[edgeKey(e.From, e.To)] = i
	}
	out := append([]EdgeConfig(nil), edges...)
	for _, dep := range deprecated {
		key := edgeKey(dep.From, dep.To)
		if idx, ok := index[key]; ok {
			out[idx] = dep
		} else {
			out = append(out, dep)
			index[key] = len(out) - 1
		}
	}
	return out, warnings
}

func edgeKey(from, to string) string {
	return from + "->" + to
}

func BuildTopologyIR(cfg *PipelineConfig) (*topologyBuildResult, error) {
	var stages []StageConfig
	var edges []EdgeConfig
	var err error

	switch {
	case len(cfg.Steps) > 0:
		stages, edges, err = NormalizeSteps(cfg)
	case len(cfg.Pipeline) > 0:
		stages, edges, err = NormalizePipeline(cfg)
	default:
		return nil, fmt.Errorf("config must define steps or pipeline")
	}
	if err != nil {
		return nil, err
	}

	edges, warnings := MergeDeprecatedEdges(edges, cfg.Edges)

	return &topologyBuildResult{
		Name:                PipelineName(cfg),
		Engine:              cfg.Engine,
		Stages:              stages,
		Edges:               edges,
		Codecs:              cfg.Codecs,
		EdgeDefaults:        cfg.EdgeDefaults,
		DLQ:                 cfg.DLQ,
		Observability:       cfg.Observability,
		DeprecationWarnings: warnings,
	}, nil
}

type topologyBuildResult struct {
	Name                string
	Engine              EngineConfig
	Stages              []StageConfig
	Edges               []EdgeConfig
	Codecs              []CodecConfig
	EdgeDefaults        EdgeAttrs
	DLQ                 *DLQConfig
	Observability       ObservabilityConfig
	DeprecationWarnings []string
}

func ApplyObservabilityDefaults(obs *ObservabilityConfig) {
	if obs.Metrics.Port == 0 {
		obs.Metrics.Port = 9090
	}
	if obs.Metrics.Path == "" {
		obs.Metrics.Path = "/metrics"
	}
	if obs.Health.Port == 0 {
		obs.Health.Port = 8080
	}
	if obs.Health.Liveness == "" {
		obs.Health.Liveness = "/live"
	}
	if obs.Health.Readiness == "" {
		obs.Health.Readiness = "/ready"
	}
	if obs.Logging.Level == "" {
		obs.Logging.Level = "info"
	}
	if obs.Logging.Format == "" {
		obs.Logging.Format = "json"
	}
}

func ApplyEngineDefaults(engine *EngineConfig) {
	if engine.MaxWorkers == 0 {
		engine.MaxWorkers = 16
	}
	if engine.MaxInflight == 0 {
		engine.MaxInflight = 10000
	}
	if engine.ErrorMode == "" {
		engine.ErrorMode = "propagate"
	}
	if engine.DrainTimeout == "" {
		engine.DrainTimeout = "30s"
	}
}

func ValidateStageKinds(st StageConfig) error {
	switch strings.ToLower(st.Kind) {
	case "source", "transform", "sink":
		return nil
	default:
		return fmt.Errorf("stage %q: unknown kind %q", st.ID, st.Kind)
	}
}

package topology

import (
	"fmt"

	"github.com/edgesets/edgestream/internal/config"
)

func FromConfig(cfg *config.PipelineConfig) (*TopologyIR, error) {
	built, err := config.BuildTopologyIR(cfg)
	if err != nil {
		return nil, err
	}
	config.ApplyEngineDefaults(&built.Engine)
	config.ApplyObservabilityDefaults(&built.Observability)

	ir := &TopologyIR{
		Name:                built.Name,
		Engine:              built.Engine,
		EdgeDefaults:        built.EdgeDefaults,
		DLQ:                 built.DLQ,
		Observability:       built.Observability,
		DeprecationWarnings: built.DeprecationWarnings,
		Codecs:              make(map[string]CodecIR, len(built.Codecs)),
	}

	for _, c := range built.Codecs {
		if c.Name == "" {
			return nil, fmt.Errorf("codec entry missing name")
		}
		if _, exists := ir.Codecs[c.Name]; exists {
			return nil, fmt.Errorf("duplicate codec name %q", c.Name)
		}
		ir.Codecs[c.Name] = CodecIR{
			Name:   c.Name,
			Type:   c.Type,
			Config: c.Config,
		}
	}

	for _, st := range built.Stages {
		if err := config.ValidateStageKinds(st); err != nil {
			return nil, err
		}
		ir.Stages = append(ir.Stages, StageIR{
			ID:          st.ID,
			Kind:        st.Kind,
			Type:        st.Type,
			Workers:     st.Workers,
			Predicate:   st.Predicate,
			ErrorMode:   st.ErrorMode,
			Decoder:     st.Decoder,
			Encoder:     st.Encoder,
			Batch:       st.Batch,
			Ordering:    st.Ordering,
			MaxInFlight: st.MaxInFlight,
			Config:      st.Config,
		})
	}

	for _, e := range built.Edges {
		required := true
		if e.Required != nil {
			required = *e.Required
		}
		buf := config.EdgeBufferConfig{}
		if e.Buffer != nil {
			buf = *e.Buffer
		}
		ir.Edges = append(ir.Edges, EdgeIR{
			From:      e.From,
			To:        e.To,
			Condition: e.Condition,
			Route:     e.Route,
			Buffer:    buf,
			Delivery:  e.Delivery,
			Required:  required,
		})
	}

	if err := Validate(ir); err != nil {
		return nil, err
	}
	return ir, nil
}

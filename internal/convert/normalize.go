package convert

import (
	"fmt"
	"sort"
)

// Intermediate representation between the v2 shapes and the v3 emitter.
// Unlike the archived loader we do NOT bake route attributes into
// `metadata["er-route"]` conditions here (legacy normalize.go
// RouteCondition): the route transform is folded away later with full
// knowledge of its route table (convert.go, redesign-v3-review-m4.md R3).

type stage struct {
	ID       string
	Kind     string // "source" | "transform" | "sink"
	Type     string
	Decoder  *v2CodecRef
	Encoder  *v2CodecRef
	Workers  int
	Batch    *v2BatchConfig
	Ordering string
	MaxInFlight int
	Config   map[string]any
	// Transform-only extras.
	Predicate string // v2 transform.predicate (pre-edge condition, rare)
	ErrorMode string
}

type edge struct {
	From     string
	To       string
	Condition string // explicit condition (never route-baked)
	Route     string // named route attr, resolved during route folding
	Buffer    *v2EdgeBuffer
	Delivery  *v2DeliverySpec
	Required  *bool
}

// normalize expands steps/pipeline[] into stages + edges. Deterministic:
// stage output order is fixed later by topological sorting (convert.go), so
// map iteration order here cannot leak into snapshots.
func normalize(cfg *v2PipelineConfig) ([]*stage, []edge, []string, error) {
	var notes []string
	var stages []*stage
	var edges []edge

	switch {
	case len(cfg.Steps) > 0:
		if len(cfg.Pipeline) > 0 {
			return nil, nil, nil, fmt.Errorf("steps and pipeline are mutually exclusive")
		}
		names := make([]string, 0, len(cfg.Steps))
		for name := range cfg.Steps {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, stepName := range names {
			step := cfg.Steps[stepName]
			if step.Source != nil {
				stages = append(stages, &stage{
					ID: stepName, Kind: "source", Type: step.Source.Type,
					Decoder: step.Source.Decoder, Config: step.Source.Config,
				})
			}
			if step.Transform != nil {
				stages = append(stages, &stage{
					ID: stepName, Kind: "transform", Type: step.Transform.Type,
					Workers: step.Transform.Workers, Predicate: step.Transform.Predicate,
					ErrorMode: step.Transform.ErrorMode, Config: step.Transform.Config,
				})
			}
			if step.Sink != nil {
				// 合体 step: transform+sink expands to a hidden `<name>-sink`
				// id joined by an implicit internal edge (legacy NormalizeSteps).
				sinkID := stepName
				if step.Transform != nil {
					sinkID = stepName + "-sink"
					edges = append(edges, edge{From: stepName, To: sinkID})
				}
				stages = append(stages, &stage{
					ID: sinkID, Kind: "sink", Type: step.Sink.Type,
					Encoder: step.Sink.Encoder, Batch: step.Sink.Batch,
					Ordering: step.Sink.Ordering, MaxInFlight: step.Sink.MaxInFlight,
					Config: step.Sink.Config,
				})
			}
			depEdges, err := expandDependsOn(stepName, step.DependsOn, cfg.EdgeDefaults)
			if err != nil {
				return nil, nil, nil, err
			}
			edges = append(edges, depEdges...)
		}
	case len(cfg.Pipeline) > 0:
		for _, st := range cfg.Pipeline {
			stages = append(stages, &stage{
				ID: st.ID, Kind: st.Kind, Type: st.Type, Decoder: st.Decoder,
				Encoder: st.Encoder, Workers: st.Workers, Predicate: st.Predicate,
				Batch: st.Batch, Ordering: st.Ordering, MaxInFlight: st.MaxInFlight,
				ErrorMode: st.ErrorMode, Config: st.Config,
			})
			depEdges, err := expandDependsOn(st.ID, st.DependsOn, cfg.EdgeDefaults)
			if err != nil {
				return nil, nil, nil, err
			}
			edges = append(edges, depEdges...)
		}
	default:
		return nil, nil, nil, fmt.Errorf("config must define steps or pipeline")
	}

	// Top-level `edges:` override depends_on-derived edges with the same
	// from->to key (legacy MergeDeprecatedEdges) — a deprecated style, noted.
	if len(cfg.Edges) > 0 {
		notes = append(notes, "top-level `edges:` section merged (deprecated v2 style; edges overrode same-key depends_on entries)")
		index := map[string]int{}
		for i := range edges {
			index[edgeKey(edges[i].From, edges[i].To)] = i
		}
		for _, dep := range cfg.Edges {
			e := edge{
				From: dep.From, To: dep.To, Condition: dep.Condition, Route: dep.Route,
				Buffer: dep.Buffer, Delivery: dep.Delivery, Required: dep.Required,
			}
			key := edgeKey(e.From, e.To)
			if idx, ok := index[key]; ok {
				edges[idx] = e
			} else {
				edges = append(edges, e)
				index[key] = len(edges) - 1
			}
		}
	}
	return stages, edges, notes, nil
}

func expandDependsOn(stepID string, deps v2DependsOnList, defaults v2EdgeAttrs) ([]edge, error) {
	var out []edge
	for _, dep := range deps {
		if dep.Upstream == "" {
			return nil, fmt.Errorf("step %q: empty depends_on upstream", stepID)
		}
		attrs := mergeEdgeAttrs(&defaults, dep.Edge)
		if attrs.Route != "" && attrs.Condition != "" {
			return nil, fmt.Errorf("step %q edge from %q: route and condition are mutually exclusive", stepID, dep.Upstream)
		}
		out = append(out, edge{
			From: dep.Upstream, To: stepID, Condition: attrs.Condition, Route: attrs.Route,
			Buffer: attrs.Buffer, Delivery: attrs.Delivery, Required: attrs.Required,
		})
	}
	return out, nil
}

// mergeEdgeAttrs follows the v2 per-field override rule (legacy
// MergeEdgeAttrs): an explicitly set field on the edge wins over the same
// field in edgeDefaults; absent fields fall through.
func mergeEdgeAttrs(base, override *v2EdgeAttrs) v2EdgeAttrs {
	out := v2EdgeAttrs{}
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

func edgeKey(from, to string) string { return from + "->" + to }

// routeTable is the ordered route list of a v2 route transform: explicit
// route_order first (validated), then the remaining names alphabetically,
// with `_default` always last (legacy plugins/transform/route evaluation
// order).
func routeTable(cfg map[string]any) (order []string, preds map[string]string, err error) {
	routesRaw, ok := cfg["routes"]
	if !ok {
		return nil, nil, fmt.Errorf("route transform requires routes")
	}
	routes, ok := routesRaw.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("route transform routes must be a mapping")
	}
	preds = map[string]string{}
	for name, p := range routes {
		s, ok := p.(string)
		if !ok {
			return nil, nil, fmt.Errorf("route %q predicate must be a string", name)
		}
		preds[name] = s
	}
	var explicit []string
	if ro, ok := cfg["route_order"]; ok {
		list, ok := ro.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("route_order must be a list")
		}
		for _, v := range list {
			name, _ := v.(string)
			if _, ok := preds[name]; !ok {
				return nil, nil, fmt.Errorf("route_order references unknown route %q", name)
			}
			explicit = append(explicit, name)
		}
	}
	seen := map[string]bool{}
	for _, n := range explicit {
		seen[n] = true
	}
	var rest []string
	for name := range routes {
		if !seen[name] && name != "_default" {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	order = append(order, explicit...)
	order = append(order, rest...)
	if _, ok := routes["_default"]; ok {
		order = append(order, "_default")
	}
	return order, preds, nil
}

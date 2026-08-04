package route

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/eql"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
)

func init() {
	registry.RegisterTransform("route", func(id string, cfg map[string]any) (stage.Transform, error) {
		routes, ok := cfg["routes"].(map[string]any)
		if !ok || len(routes) == 0 {
			return nil, fmt.Errorf("route transform: routes is required")
		}
		compiled := make(map[string]*eql.Program, len(routes))
		for name, expr := range routes {
			s, ok := expr.(string)
			if !ok {
				return nil, fmt.Errorf("route %q: expression must be string", name)
			}
			prg, err := eql.CompileFilter(s)
			if err != nil {
				return nil, fmt.Errorf("route %q: %w", name, err)
			}
			compiled[name] = prg
		}
		order, err := routeOrder(cfg, compiled)
		if err != nil {
			return nil, err
		}
		return &Transform{
			Base:   basestage.Base{IDVal: id, KindVal: stage.KindTransform, TypeVal: "route"},
			routes: compiled,
			order:  order,
		}, nil
	})
}

func routeOrder(cfg map[string]any, routes map[string]*eql.Program) ([]string, error) {
	if raw, ok := cfg["route_order"].([]any); ok {
		out := make([]string, 0, len(raw))
		seen := make(map[string]struct{}, len(raw))
		for _, item := range raw {
			name, ok := item.(string)
			if !ok || name == "" {
				continue
			}
			if _, exists := routes[name]; !exists {
				return nil, fmt.Errorf("route_order: unknown route %q", name)
			}
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
		if _, hasDefault := routes["_default"]; hasDefault {
			if _, listed := seen["_default"]; !listed {
				out = append(out, "_default")
			}
		}
		return out, nil
	}
	names := make([]string, 0, len(routes))
	for name := range routes {
		if name == "_default" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if _, ok := routes["_default"]; ok {
		names = append(names, "_default")
	}
	return names, nil
}

type Transform struct {
	basestage.Base
	routes map[string]*eql.Program
	order  []string
}

func (t *Transform) Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(batch))
	for _, msg := range batch {
		cp := msg.ShallowCopy()
		payload := map[string]any{}
		if cp.ParsedData() != nil {
			if m, ok := cp.ParsedData().(map[string]any); ok {
				payload = m
			}
		} else if len(cp.Payload) > 0 {
			_ = json.Unmarshal(cp.Payload, &payload)
		}
		evalCtx := &eql.EvalContext{
			Msg:     eql.NewMsgAdapter(cp),
			Input:   payload,
			Payload: payload,
			Meta:    cp.Metadata,
		}
		matched := ""
		for _, name := range t.order {
			prg := t.routes[name]
			ok, err := prg.EvalFilter(evalCtx)
			if err != nil {
				return nil, err
			}
			if ok {
				matched = name
				break
			}
		}
		if matched == "" {
			continue
		}
		if cp.Metadata == nil {
			cp.Metadata = make(map[string]any)
		}
		cp.Metadata["er-route"] = matched
		out = append(out, cp)
	}
	return out, nil
}


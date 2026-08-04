package maptransform

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/edgesets/edgestream/internal/basestage"
	"github.com/edgesets/edgestream/internal/eql"
	"github.com/edgesets/edgestream/internal/message"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/stage"
)

func init() {
	registry.RegisterTransform("map", func(id string, cfg map[string]any) (stage.Transform, error) {
		dsl, _ := cfg["dsl"].(string)
		if dsl == "" {
			return nil, fmt.Errorf("map transform: dsl is required")
		}
		prg, err := eql.CompileMapping(dsl)
		if err != nil {
			return nil, err
		}
		return &Transform{
			Base: basestage.Base{IDVal: id, KindVal: stage.KindTransform, TypeVal: "map"},
			prg:  prg,
		}, nil
	})
}

type Transform struct {
	basestage.Base
	prg *eql.Program
}

func (t *Transform) Process(ctx context.Context, batch []*message.Message) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(batch))
	for _, msg := range batch {
		cp := msg.ShallowCopy()
		payload, input := payloadMap(cp)
		evalCtx := &eql.EvalContext{
			Msg:     eql.NewMsgAdapter(cp),
			Input:   input,
			Payload: payload,
			Meta:    cp.Metadata,
		}
		if err := t.prg.EvalMapping(evalCtx); err != nil {
			return nil, err
		}
		if cp.ParsedDirty() {
			b, err := json.Marshal(evalCtx.Payload)
			if err != nil {
				return nil, err
			}
			cp.BackupOriginalPayload()
			cp.Payload = b
		}
		out = append(out, cp)
	}
	return out, nil
}

func payloadMap(msg *message.Message) (map[string]any, map[string]any) {
	var payload map[string]any
	if msg.ParsedData() != nil {
		if m, ok := msg.ParsedData().(map[string]any); ok {
			payload = m
		}
	}
	if payload == nil && len(msg.Payload) > 0 {
		_ = json.Unmarshal(msg.Payload, &payload)
	}
	if payload == nil {
		payload = make(map[string]any)
	}
	input := make(map[string]any, len(payload))
	for k, v := range payload {
		input[k] = v
	}
	return payload, input
}


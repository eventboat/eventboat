package rpcplugin

import (
	"encoding/json"
	"fmt"

	"github.com/eventboat/eventboat/internal/registry"
	pluginv1 "github.com/eventboat/eventboat/pkg/pluginv1"
)

// eventToMessage converts a wire Event into an engine Message. The engine
// stamps its own identity metadata (message_id, ingest_time, source) around
// what sources provide.
func eventToMessage(ev *pluginv1.Event) registry.Message {
	meta := make(map[string]any, len(ev.Meta))
	for k, mv := range ev.Meta {
		if v, ok := metaValueToGo(mv); ok {
			meta[k] = v
		}
	}
	raw := ev.Payload
	if raw == nil {
		raw = []byte{}
	}
	return registry.Message{
		Raw:     raw,
		Meta:    meta,
		Codec:   ev.Codec,
		Cursor:  ev.Cursor,
		SrcSeq:  ev.SrcSeq,
		SrcName: ev.SrcName,
	}
}

// messageToEvent converts an engine Message into a wire Event for sinks.
// Payload is the sink-encoded bytes (Out) falling back to the spooled raw
// bytes, matching what built-in sinks write.
func messageToEvent(m registry.Message) *pluginv1.Event {
	payload := m.Out
	if len(payload) == 0 {
		payload = m.Raw
	}
	return &pluginv1.Event{
		Payload: payload,
		Meta:    goMetaToProto(m.Meta),
		Codec:   m.Codec,
		Cursor:  m.Cursor,
		SrcSeq:  m.SrcSeq,
		SrcName: m.SrcName,
	}
}

func metaValueToGo(mv *pluginv1.MetaValue) (any, bool) {
	switch k := mv.GetKind().(type) {
	case *pluginv1.MetaValue_StringValue:
		return k.StringValue, true
	case *pluginv1.MetaValue_IntValue:
		return k.IntValue, true
	case *pluginv1.MetaValue_BoolValue:
		return k.BoolValue, true
	case *pluginv1.MetaValue_DoubleValue:
		return k.DoubleValue, true
	default:
		return nil, false
	}
}

// goMetaToProto maps engine metadata values onto the scalar wire types.
// Rich values (arrays, objects) travel as JSON strings — documented in
// docs/plugins.md so predicates know what to expect.
func goMetaToProto(meta map[string]any) map[string]*pluginv1.MetaValue {
	if meta == nil {
		return nil
	}
	out := make(map[string]*pluginv1.MetaValue, len(meta))
	for k, v := range meta {
		out[k] = goToMetaValue(v)
	}
	return out
}

func goToMetaValue(v any) *pluginv1.MetaValue {
	switch t := v.(type) {
	case string:
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_StringValue{StringValue: t}}
	case bool:
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_BoolValue{BoolValue: t}}
	case int:
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_IntValue{IntValue: int64(t)}}
	case int32:
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_IntValue{IntValue: int64(t)}}
	case int64:
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_IntValue{IntValue: t}}
	case float32:
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_DoubleValue{DoubleValue: float64(t)}}
	case float64:
		// JSON integers decode as float64; integers round-trip exactly.
		if t == float64(int64(t)) {
			return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_IntValue{IntValue: int64(t)}}
		}
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_DoubleValue{DoubleValue: t}}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
		}
		return &pluginv1.MetaValue{Kind: &pluginv1.MetaValue_StringValue{StringValue: string(b)}}
	}
}

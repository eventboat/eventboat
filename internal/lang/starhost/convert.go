package starhost

import (
	"encoding/json"
	"fmt"

	"go.starlark.net/starlark"
)

// GoToStarlark converts a decoded JSON-ish Go value into Starlark values.
// Maps become attrDict: dicts that also allow attribute reads and writes
// (payload.nested.key = v works like eql did). Conversions are fresh (no
// aliasing between Go and Starlark structures).
func GoToStarlark(v any) starlark.Value {
	switch t := v.(type) {
	case nil:
		return starlark.None
	case bool:
		return starlark.Bool(t)
	case string:
		return starlark.String(t)
	case int:
		return starlark.MakeInt64(int64(t))
	case int64:
		return starlark.MakeInt64(t)
	case uint64:
		return starlark.MakeUint64(t)
	case float64:
		return starlark.Float(t)
	case []any:
		out := make([]starlark.Value, 0, len(t))
		for _, el := range t {
			out = append(out, GoToStarlark(el))
		}
		return starlark.NewList(out)
	case map[string]any:
		d := newAttrDict(len(t))
		for k, val := range t {
			_ = d.SetKey(starlark.String(k), GoToStarlark(val))
		}
		return d
	case []byte:
		return starlark.String(string(t))
	default:
		return starlark.String(fmt.Sprintf("%v", v))
	}
}

// attrDict is a dict with attribute access: x.k reads x["k"], x.k = v writes
// x["k"] = v. Method names (keys/values/items/get/...) take precedence over
// fields, matching Starlark dict semantics.
type attrDict struct{ *starlark.Dict }

func newAttrDict(size int) *attrDict { return &attrDict{Dict: starlark.NewDict(size)} }

func (d *attrDict) Attr(name string) (starlark.Value, error) {
	if v, err := d.Dict.Attr(name); v != nil || err != nil {
		return v, err
	}
	if v, found, _ := d.Dict.Get(starlark.String(name)); found {
		return v, nil
	}
	return nil, nil
}

func (d *attrDict) SetField(name string, v starlark.Value) error {
	return d.Dict.SetKey(starlark.String(name), v)
}

func (d *attrDict) AttrNames() []string {
	names := d.Dict.AttrNames()
	for _, k := range d.Dict.Keys() {
		if s, ok := starlark.AsString(k); ok {
			names = append(names, s)
		}
	}
	return names
}

// StarlarkToGo converts a Starlark value back into JSON-ish Go values. Types
// without a JSON counterpart fall back to their string representation.
func StarlarkToGo(v starlark.Value) any {
	switch t := v.(type) {
	case nil:
		return nil
	case starlark.NoneType:
		return nil
	case starlark.Bool:
		return bool(t)
	case starlark.String:
		return string(t)
	case starlark.Int:
		if i, ok := t.Int64(); ok {
			return i
		}
		return float64(t.Float())
	case starlark.Float:
		return float64(t)
	case *starlark.List:
		out := make([]any, 0, t.Len())
		for i := 0; i < t.Len(); i++ {
			out = append(out, StarlarkToGo(t.Index(i)))
		}
		return out
	case starlark.Tuple:
		out := make([]any, 0, t.Len())
		for i := 0; i < t.Len(); i++ {
			out = append(out, StarlarkToGo(t.Index(i)))
		}
		return out
	case *attrDict:
		out := make(map[string]any, t.Len())
		for _, kv := range t.Items() {
			key, _ := starlark.AsString(kv[0])
			out[key] = StarlarkToGo(kv[1])
		}
		return out
	case *starlark.Dict:
		out := make(map[string]any, t.Len())
		for _, kv := range t.Items() {
			key, _ := starlark.AsString(kv[0])
			out[key] = StarlarkToGo(kv[1])
		}
		return out
	case *starlark.Set:
		out := make([]any, 0, t.Len())
		for el := range t.Elements() {
			out = append(out, StarlarkToGo(el))
		}
		return out
	default:
		return v.String()
	}
}

// decodeJSONGo decodes a JSON string into a Go value (for safe_json_decode).
func decodeJSONGo(s string) (any, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return v, nil
}

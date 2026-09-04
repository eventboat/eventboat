package starhost

import (
	"encoding/json"
	"fmt"

	"go.starlark.net/starlark"
)

// GoToStarlark converts a decoded JSON-ish Go value into Starlark values.
// Maps become attrDict: dicts that also allow attribute reads and writes
// (payload.nested.key = v works like eql did). Conversions are fresh (no
// aliasing between Go and Starlark structures). The engine-side materialize
// path uses convertGo directly so mutations can mark the owning MsgState.
func GoToStarlark(v any) starlark.Value {
	star, _ := convertGo(v, nil)
	return star
}

// convertGo is GoToStarlark threading a mutation marker: every attrDict in
// the produced tree carries it, so any write through the tree can set the
// owner's dirty flag precisely (beta hardening: reads must not dirty). The
// second return reports whether the tree contains ANY list — native
// *starlark.List mutators (append, index writes) cannot be intercepted, so
// callers must fall back to conservative dirtying for list-bearing trees.
func convertGo(v any, mark func()) (starlark.Value, bool) {
	switch t := v.(type) {
	case nil:
		return starlark.None, false
	case bool:
		return starlark.Bool(t), false
	case string:
		return starlark.String(t), false
	case int:
		return starlark.MakeInt64(int64(t)), false
	case int64:
		return starlark.MakeInt64(t), false
	case uint64:
		return starlark.MakeUint64(t), false
	case float64:
		return starlark.Float(t), false
	case []any:
		out := make([]starlark.Value, 0, len(t))
		for _, el := range t {
			c, _ := convertGo(el, mark)
			out = append(out, c)
		}
		return starlark.NewList(out), true
	case map[string]any:
		d := newAttrDict(len(t))
		d.mark = mark
		hasLists := false
		for k, val := range t {
			c, hl := convertGo(val, mark)
			_ = d.Dict.SetKey(starlark.String(k), c)
			hasLists = hasLists || hl
		}
		return d, hasLists
	case []byte:
		return starlark.String(string(t)), false
	default:
		return starlark.String(fmt.Sprintf("%v", v)), false
	}
}

// attrDict is a dict with attribute access: x.k reads x["k"], x.k = v writes
// x["k"] = v. Method names (keys/values/items/get/...) take precedence over
// fields, matching Starlark dict semantics. When created through a MsgState
// materialization it carries a mutation marker: every write path (SetKey,
// SetField, Delete, the mutating dict builtins) marks the owner dirty, so
// precise dirty tracking holds for map-only trees.
type attrDict struct {
	*starlark.Dict
	mark func() // nil for plain GoToStarlark conversions
}

func newAttrDict(size int) *attrDict { return &attrDict{Dict: starlark.NewDict(size)} }

// dictMutators are the native dict builtins that mutate their receiver; they
// are wrapped in Attr so the owner is marked even when invoked as a method
// (nested.update({...}), nested.pop("k")).
var dictMutators = map[string]bool{
	"clear": true, "pop": true, "popitem": true, "setdefault": true, "update": true,
}

func (d *attrDict) Attr(name string) (starlark.Value, error) {
	if v, err := d.Dict.Attr(name); v != nil || err != nil {
		if d.mark != nil && dictMutators[name] {
			native, _ := v.(*starlark.Builtin)
			d.mark()
			return starlark.NewBuiltin(name, func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if native == nil {
					return nil, fmt.Errorf("%s: no such method", name)
				}
				return native.CallInternal(thread, args, kwargs)
			}), nil
		}
		return v, err
	}
	if v, found, _ := d.Dict.Get(starlark.String(name)); found {
		return v, nil
	}
	return nil, nil
}

func (d *attrDict) SetKey(k, v starlark.Value) error {
	if d.mark != nil {
		d.mark()
	}
	return d.Dict.SetKey(k, v)
}

func (d *attrDict) SetField(name string, v starlark.Value) error {
	if d.mark != nil {
		d.mark()
	}
	return d.Dict.SetKey(starlark.String(name), v)
}

// Delete marks then delegates (the remove() glue and msgValue.deleteKey both
// route here on materialized trees).
func (d *attrDict) Delete(k starlark.Value) (starlark.Value, bool, error) {
	if d.mark != nil {
		d.mark()
	}
	return d.Dict.Delete(k)
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

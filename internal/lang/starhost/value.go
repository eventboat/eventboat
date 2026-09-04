package starhost

import (
	"fmt"
	"sort"

	"go.starlark.net/starlark"
)

// MsgState is the per-message binding state for one of payload/meta.
//
// Reads convert lazily per field (no per-message full conversion of the
// decoded payload); the first write materializes the whole value into a
// Starlark-native copy — copy-on-write, so untouched messages pay no
// write-back (redesign-v3.md §4.3).
type MsgState struct {
	kind  string // "payload" or "meta" — surfaces in error messages
	goVal any
	cache map[string]starlark.Value // lazily converted top-level fields (maps only)
	star  starlark.Value            // materialized COW tree; nil until first write
	dirty bool
}

// NewMsgState wraps a decoded Go value (map[string]any, []any or scalar).
func NewMsgState(kind string, v any) *MsgState {
	return &MsgState{kind: kind, goVal: v}
}

// Binding returns the Starlark value bound to the script global.
func (s *MsgState) Binding() starlark.Value { return msgValue{s} }

// Dirty reports whether the script wrote to this binding. Map-tree writes
// are tracked precisely (mutation interception on the converted dicts); a
// tree that contains ANY list dirties conservatively — native Starlark list
// mutators (append, index writes) cannot be intercepted (beta hardening).
func (s *MsgState) Dirty() bool { return s.dirty }

// markWritten is handed to the materialized tree so nested writes mark this
// state precisely.
func (s *MsgState) markWritten() { s.dirty = true }

// GoValue returns the current value: the materialized tree converted back to
// Go after WRITES, else the original decoded value. A materialized-but-clean
// state (container reads, no writes — precise dirty tracking) keeps the
// original: the unmarked tree is provably unchanged for map-only trees, and
// list-bearing trees are conservatively dirty anyway.
func (s *MsgState) GoValue() any {
	if s.star != nil && s.dirty {
		return StarlarkToGo(s.star)
	}
	return s.goVal
}

// MapValue returns the current value as map[string]any when it is one.
func (s *MsgState) MapValue() (map[string]any, bool) {
	v := s.GoValue()
	m, ok := v.(map[string]any)
	return m, ok
}

// msgValue implements the Starlark value protocols over a MsgState. Attribute
// writes (payload.x = v), item writes (payload["x"] = v) and reads all work;
// the mapping method set (keys/values/items/get) is served natively.
type msgValue struct{ s *MsgState }

var (
	_ starlark.Value           = msgValue{}
	_ starlark.Mapping         = msgValue{}
	_ starlark.IterableMapping = msgValue{}
	_ starlark.HasSetKey       = msgValue{}
	_ starlark.HasSetField     = msgValue{}
	_ starlark.HasAttrs        = msgValue{}
)

func (m msgValue) Type() string { return m.s.kind }
func (m msgValue) Freeze()      {} // engine-owned per message; never shared

func (m msgValue) Hash() (uint32, error) {
	return 0, fmt.Errorf("unhashable type: %s", m.s.kind)
}

func (m msgValue) Truth() starlark.Bool {
	if m.s.star != nil {
		return m.s.star.Truth()
	}
	switch t := m.s.goVal.(type) {
	case nil:
		return false
	case map[string]any:
		return len(t) > 0
	case []any:
		return len(t) > 0
	case string:
		return t != ""
	case bool:
		return starlark.Bool(t)
	default:
		return true
	}
}

func (m msgValue) String() string {
	if m.s.star != nil {
		return m.s.star.String()
	}
	return fmt.Sprintf("%s(%v)", m.s.kind, m.s.goVal)
}

// lazyRoot returns the Go map when the unmaterialized value is a mapping.
func (m msgValue) lazyRoot() (map[string]any, bool) {
	root, ok := m.s.goVal.(map[string]any)
	return root, ok
}

// fieldGet reads one field. In the lazy regime only scalar fields are
// served from the Go root (fresh conversion is safe — scalars are
// immutable); container fields (maps/lists) materialize the whole tree
// first, because Starlark reference semantics require nested writes
// (`payload.nested.k = v`, `remove(payload.a, "b")`) to reach the state —
// serving a fresh copy would silently lose them. Materializing for a READ
// no longer dirties the state (precise dirty tracking, beta hardening):
// map-tree mutations mark through the conversion marker, list-bearing trees
// dirty conservatively at materialization.
func (m msgValue) fieldGet(name string) (starlark.Value, bool) {
	if m.s.star != nil {
		if d, ok := m.s.star.(starlark.Mapping); ok {
			v, found, err := d.Get(starlark.String(name))
			if err != nil || !found {
				return nil, false
			}
			return v, true
		}
		return nil, false
	}
	if v := m.s.cache[name]; v != nil {
		return v, true
	}
	root, ok := m.lazyRoot()
	if !ok {
		return nil, false
	}
	raw, found := root[name]
	if !found {
		return nil, false
	}
	switch raw.(type) {
	case map[string]any, []any:
		if err := m.materialize(); err != nil {
			return nil, false
		}
		return m.fieldGet(name) // serve from the materialized tree (reference)
	}
	v := GoToStarlark(raw)
	if m.s.cache == nil {
		m.s.cache = map[string]starlark.Value{}
	}
	m.s.cache[name] = v
	return v, true
}

// Get implements mapping read: payload["field"].
func (m msgValue) Get(k starlark.Value) (starlark.Value, bool, error) {
	if m.s.star != nil {
		if d, ok := m.s.star.(starlark.Mapping); ok {
			return d.Get(k)
		}
		return nil, false, fmt.Errorf("%s: value of type %s does not support item access", m.s.kind, m.s.star.Type())
	}
	key, ok := k.(starlark.String)
	if !ok {
		return nil, false, fmt.Errorf("%s: key must be a string, got %s", m.s.kind, k.Type())
	}
	if _, isMap := m.lazyRoot(); !isMap {
		return nil, false, fmt.Errorf("%s: value of type %T is not a mapping", m.s.kind, m.s.goVal)
	}
	v, found := m.fieldGet(key.GoString())
	return v, found, nil
}

// SetKey implements payload["field"] = v.
func (m msgValue) SetKey(k, v starlark.Value) error {
	if err := m.materialize(); err != nil {
		return err
	}
	m.s.dirty = true
	d, ok := m.s.star.(starlark.HasSetKey)
	if !ok {
		return fmt.Errorf("%s: value of type %s does not support item assignment", m.s.kind, m.s.star.Type())
	}
	return d.SetKey(k, v)
}

// SetField implements payload.field = v (the eql "assignment same-shape" glue).
func (m msgValue) SetField(name string, v starlark.Value) error {
	if err := m.materialize(); err != nil {
		return err
	}
	m.s.dirty = true
	d, ok := m.s.star.(starlark.HasSetKey)
	if !ok {
		return fmt.Errorf("%s: field assignment requires a mapping, got %s", m.s.kind, m.s.star.Type())
	}
	return d.SetKey(starlark.String(name), v)
}

// Attr serves field reads plus the mapping method set.
func (m msgValue) Attr(name string) (starlark.Value, error) {
	if m.s.star != nil {
		if d, ok := m.s.star.(starlark.HasAttrs); ok {
			return d.Attr(name)
		}
		return nil, nil
	}
	if name == "keys" || name == "values" || name == "items" || name == "get" {
		return starlark.NewBuiltin(name, m.method), nil
	}
	if v, found := m.fieldGet(name); found {
		return v, nil
	}
	return nil, nil // no such field
}

// AttrNames lists fields plus mapping methods.
func (m msgValue) AttrNames() []string {
	if m.s.star != nil {
		if d, ok := m.s.star.(starlark.HasAttrs); ok {
			return d.AttrNames()
		}
		return nil
	}
	names := []string{"get", "items", "keys", "values"}
	if root, ok := m.lazyRoot(); ok {
		for k := range root {
			names = append(names, k)
		}
	}
	sort.Strings(names[4:])
	return names
}

// Iterate snapshots the keys at iteration start. Writes during iteration
// materialize the tree but the iterator keeps its snapshot (documented
// semantics; a conformance case will lock it — open question #4).
func (m msgValue) Iterate() starlark.Iterator {
	if m.s.star != nil {
		if im, ok := m.s.star.(starlark.Iterable); ok {
			return im.Iterate()
		}
		return &emptyIterator{}
	}
	root, ok := m.lazyRoot()
	if !ok {
		return &emptyIterator{}
	}
	keys := make([]starlark.Value, 0, len(root))
	for k := range root {
		keys = append(keys, starlark.String(k))
	}
	return &sliceIterator{keys: keys}
}

// Items implements the IterableMapping protocol (sorted for determinism).
func (m msgValue) Items() []starlark.Tuple {
	if m.s.star != nil {
		if im, ok := m.s.star.(starlark.IterableMapping); ok {
			return im.Items()
		}
		return nil
	}
	root, ok := m.lazyRoot()
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(root))
	for k := range root {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]starlark.Tuple, 0, len(keys))
	for _, k := range keys {
		v, _ := m.fieldGet(k)
		out = append(out, starlark.Tuple{starlark.String(k), v})
	}
	return out
}

// Len implements the len() protocol.
func (m msgValue) Len() int {
	if m.s.star != nil {
		if l, ok := m.s.star.(interface{ Len() int }); ok {
			return l.Len()
		}
		return 0
	}
	switch t := m.s.goVal.(type) {
	case map[string]any:
		return len(t)
	case []any:
		return len(t)
	case string:
		return len([]rune(t))
	}
	return 0
}

func (m msgValue) method(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	switch b.Name() {
	case "keys":
		var out []starlark.Value
		for _, kv := range m.Items() {
			out = append(out, kv[0])
		}
		return starlark.NewList(out), nil
	case "values":
		var out []starlark.Value
		for _, kv := range m.Items() {
			out = append(out, kv[1])
		}
		return starlark.NewList(out), nil
	case "items":
		var out []starlark.Value
		for _, kv := range m.Items() {
			out = append(out, kv)
		}
		return starlark.NewList(out), nil
	case "get":
		var key starlark.String
		var fallback starlark.Value = starlark.None
		if err := starlark.UnpackArgs(b.Name(), args, kwargs, "key", &key, "default", &fallback); err != nil {
			return nil, err
		}
		v, found, err := m.Get(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return fallback, nil
		}
		return v, nil
	}
	return nil, fmt.Errorf("%s: no such method %s", m.s.kind, b.Name())
}

// materialize converts the whole value into a Starlark-native tree on first
// write OR first container read (copy-on-write; reference semantics need the
// shared tree). It does NOT itself dirty the state — reads stay clean:
// map-tree mutations mark precisely through the marker threaded into the
// conversion, and list-bearing trees dirty conservatively right here because
// native list mutators cannot be intercepted.
func (m msgValue) materialize() error {
	if m.s.star != nil {
		return nil
	}
	star, hasLists := convertGo(m.s.goVal, m.s.markWritten)
	m.s.star = star
	if hasLists {
		m.s.dirty = true
	}
	return nil
}

// deleteKey removes one key (remove() glue, spec §4.8 del() migration
// target). Works on both the lazy Go root and the materialized tree; marks
// the state dirty so the engine writes the deletion back. A missing key is
// a no-op.
func (m msgValue) deleteKey(k starlark.Value) error {
	key, ok := k.(starlark.String)
	if !ok {
		return fmt.Errorf("%s: remove key must be a string, got %s", m.s.kind, k.Type())
	}
	if m.s.star != nil {
		if d, ok := m.s.star.(deleter); ok {
			_, _, err := d.Delete(k)
			return err
		}
		return fmt.Errorf("%s: value of type %s does not support remove", m.s.kind, m.s.star.Type())
	}
	root, ok := m.lazyRoot()
	if !ok {
		return fmt.Errorf("%s: value of type %T is not a mapping", m.s.kind, m.s.goVal)
	}
	delete(root, key.GoString())
	delete(m.s.cache, key.GoString())
	m.s.dirty = true
	return nil
}

// deleter matches *starlark.Dict (and attrDict embedding it).
type deleter interface {
	Delete(k starlark.Value) (starlark.Value, bool, error)
}

// removeKey is the remove(dict, key) host glue: deletes key from payload/meta
// roots or nested dicts. Missing keys are no-ops; returns None.
func removeKey(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var dict, key starlark.Value
	if err := starlark.UnpackArgs("remove", args, kwargs, "dict", &dict, "key", &key); err != nil {
		return nil, err
	}
	switch d := dict.(type) {
	case msgValue:
		if err := d.deleteKey(key); err != nil {
			return nil, err
		}
		return starlark.None, nil
	case *attrDict:
		if _, _, err := d.Delete(key); err != nil { // marks the owner when materialized
			return nil, fmt.Errorf("remove: %w", err)
		}
		return starlark.None, nil
	case *starlark.Dict:
		if _, _, err := d.Delete(key); err != nil {
			return nil, fmt.Errorf("remove: %w", err)
		}
		return starlark.None, nil
	}
	return nil, fmt.Errorf("remove: first argument must be a mapping (payload/meta root or a dict), got %s", dict.Type())
}

type emptyIterator struct{}

func (*emptyIterator) Next(v *starlark.Value) bool { return false }
func (*emptyIterator) Done()                       {}

type sliceIterator struct {
	keys []starlark.Value
	i    int
}

func (it *sliceIterator) Next(v *starlark.Value) bool {
	if it.i >= len(it.keys) {
		return false
	}
	*v = it.keys[it.i]
	it.i++
	return true
}
func (it *sliceIterator) Done() {}

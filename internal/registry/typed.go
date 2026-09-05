// This file implements typed plugin registration: the config contract (JSON
// Schema, runtime defaults) is derived from one struct definition instead of
// a hand-written schema string plus hand-rolled map[string]any casts.
//
// A config struct declares field names with `json` tags and constraints with
// `schema` tags:
//
//	type kafkaSourceConfig struct {
//	    Brokers []string `json:"brokers" schema:"minItems=1,desc=Kafka broker addresses"`
//	    Topics  []string `json:"topics" schema:"minItems=1"`
//	    GroupID string   `json:"group_id" schema:"default=eventboat"`
//	}
//
// The schema tag grammar (comma-separated; `desc=` must come last so the
// description may contain commas):
//
//	optional         field is optional (fields are required by default)
//	default=v        default value, applied to zero-valued fields after
//	                 decode; implies optional; scalars only
//	enum=a|b|c       allowed string values
//	min=N max=N      numeric bounds (integer/number fields)
//	minLen=N         minLength (string fields)
//	minItems=N       minItems (array fields)
//	maxItems=N       maxItems (array fields)
//	desc=text        description (documentation/LSP hover)
//
// The generated schema is validated and compiled through the same
// santhosh-tekuri pipeline as hand-written ones, so error output is
// identical (redesign-v3.md §5.6). Defaults are injected from the same tag
// the schema annotation came from, which is what keeps the two from
// drifting apart.
package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// RegisterSourceT registers a source plugin whose config contract is the
// struct C. The JSON Schema is generated from C; the factory receives a
// decoded, defaults-applied C instead of a raw map. Cross-field validation
// that JSON Schema cannot express stays inside build.
func RegisterSourceT[S Source, C any](r *Registry, name string, version int, capabilities []string, build func(C) (S, error)) error {
	if build == nil {
		return fmt.Errorf("plugin %q: nil factory", name)
	}
	plan, err := newTypePlan[C](name)
	if err != nil {
		return err
	}
	return r.RegisterSource(name, version, plan.schema, capabilities, func(cfg map[string]any) (Source, error) {
		c, err := decodeTyped[C](cfg, plan.defaults)
		if err != nil {
			return nil, err
		}
		return build(c)
	})
}

// RegisterSinkT registers a sink plugin whose config contract is the struct C.
func RegisterSinkT[S Sink, C any](r *Registry, name string, version int, build func(C) (S, error)) error {
	if build == nil {
		return fmt.Errorf("plugin %q: nil factory", name)
	}
	plan, err := newTypePlan[C](name)
	if err != nil {
		return err
	}
	return r.RegisterSink(name, version, plan.schema, func(cfg map[string]any) (Sink, error) {
		c, err := decodeTyped[C](cfg, plan.defaults)
		if err != nil {
			return nil, err
		}
		return build(c)
	})
}

// RegisterCodecT registers a codec plugin whose config contract is the
// struct C. dir (the pipeline file's directory) reaches build unchanged for
// relative path resolution.
func RegisterCodecT[C any](r *Registry, name string, version int, build func(cfg C, dir string) (Codec, error)) error {
	if build == nil {
		return fmt.Errorf("codec %q: nil factory", name)
	}
	plan, err := newTypePlan[C](name)
	if err != nil {
		return err
	}
	return r.RegisterCodec(name, version, plan.schema, func(cfg map[string]any, dir string) (Codec, error) {
		c, err := decodeTyped[C](cfg, plan.defaults)
		if err != nil {
			return nil, err
		}
		return build(c, dir)
	})
}

// RegisterTransformT registers a transform plugin whose config contract is
// the type C. C is usually a struct, but unlike the other kinds it may also
// be a scalar: transform plugin blocks are not forced to mappings, and the
// built-in script plugin's config is the Starlark source text itself
// (C = string). dir (the pipeline file's directory) reaches build unchanged
// for relative path resolution.
func RegisterTransformT[T Transform, C any](r *Registry, name string, version int, capabilities []string, build func(cfg C, dir string) (T, error)) error {
	if build == nil {
		return fmt.Errorf("transform %q: nil factory", name)
	}
	plan, err := newTypePlan[C](name)
	if err != nil {
		return err
	}
	return r.RegisterTransform(name, version, plan.schema, capabilities, func(cfg any, dir string) (Transform, error) {
		c, err := decodeTyped[C](cfg, plan.defaults)
		if err != nil {
			return nil, err
		}
		return build(c, dir)
	})
}

// --- schema generation ---

// typePlan holds everything derived from a config struct: the generated
// schema text and the pre-parsed default values.
type typePlan struct {
	schema   string
	defaults *defaultTable
}

func newTypePlan[C any](plugin string) (*typePlan, error) {
	t := reflect.TypeOf((*C)(nil)).Elem()
	pb := &planBuilder{defs: &defaultTable{byType: map[reflect.Type][]fieldDefault{}}, seen: map[reflect.Type]bool{}}
	root := newNode()
	root.set("$schema", "https://json-schema.org/draft/2020-12/schema")
	var node *schemaNode
	var err error
	if t.Kind() == reflect.Struct {
		node, err = pb.objectNode(t)
	} else {
		// Scalar root (transform configs only): the plugin block's value is
		// the config itself, e.g. the script plugin's Starlark source text.
		node, err = pb.valueNode(t, fieldSpec{})
	}
	if err != nil {
		return nil, fmt.Errorf("plugin %q: %w", plugin, err)
	}
	root.set("type", node.vals["type"])
	for _, k := range node.keys[1:] { // required, properties, additionalProperties
		root.set(k, node.vals[k])
	}
	var b strings.Builder
	root.render(&b, "")
	schema := b.String()
	var check any
	if err := json.Unmarshal([]byte(schema), &check); err != nil {
		return nil, fmt.Errorf("plugin %q: generated schema is not valid JSON: %w", plugin, err)
	}
	return &typePlan{schema: schema, defaults: pb.defs}, nil
}

// fieldSpec is the parsed schema tag of one struct field.
type fieldSpec struct {
	optional bool
	hasDef   bool
	def      string
	enums    []string
	min, max *float64
	minLen   *int
	minItems *int
	maxItems *int
	desc     string
}

func parseSchemaTag(field, tag string) (fieldSpec, error) {
	var spec fieldSpec
	if i := strings.Index(tag, "desc="); i >= 0 {
		spec.desc = strings.TrimSpace(tag[i+len("desc="):])
		tag = strings.TrimSuffix(tag[:i], ",")
	}
	if tag == "" {
		return spec, nil
	}
	num := func(k, v string) (float64, error) {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("field %s: %s: %q is not a number", field, k, v)
		}
		return f, nil
	}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		key, val := part, ""
		if i := strings.Index(part, "="); i >= 0 {
			key, val = part[:i], part[i+1:]
		}
		var err error
		switch key {
		case "optional":
			spec.optional = true
		case "default":
			if val == "" {
				return spec, fmt.Errorf("field %s: default= needs a value", field)
			}
			spec.hasDef, spec.def = true, val
		case "enum":
			if val == "" {
				return spec, fmt.Errorf("field %s: enum= needs at least one value", field)
			}
			spec.enums = strings.Split(val, "|")
		case "min":
			var f float64
			if f, err = num("min", val); err == nil {
				spec.min = &f
			}
		case "max":
			var f float64
			if f, err = num("max", val); err == nil {
				spec.max = &f
			}
		case "minLen":
			n, e := strconv.Atoi(val)
			if e != nil || n < 0 {
				err = fmt.Errorf("field %s: minLen: %q is not a non-negative integer", field, val)
			} else {
				spec.minLen = &n
			}
		case "minItems":
			n, e := strconv.Atoi(val)
			if e != nil || n < 0 {
				err = fmt.Errorf("field %s: minItems: %q is not a non-negative integer", field, val)
			} else {
				spec.minItems = &n
			}
		case "maxItems":
			n, e := strconv.Atoi(val)
			if e != nil || n < 0 {
				err = fmt.Errorf("field %s: maxItems: %q is not a non-negative integer", field, val)
			} else {
				spec.maxItems = &n
			}
		default:
			err = fmt.Errorf("field %s: unknown schema tag key %q", field, key)
		}
		if err != nil {
			return spec, err
		}
	}
	return spec, nil
}

// planBuilder walks a config struct once, producing the schema tree and
// collecting default values (including those inside nested structs and
// slices of structs).
type planBuilder struct {
	defs *defaultTable
	seen map[reflect.Type]bool
}

// fieldName returns the config key of f (the json tag name, falling back to
// the Go field name, mirroring encoding/json).
func fieldName(f reflect.StructField) string {
	if tag := f.Tag.Get("json"); tag != "" {
		if name := strings.Split(tag, ",")[0]; name != "" {
			return name
		}
	}
	return f.Name
}

func (pb *planBuilder) objectNode(t reflect.Type) (*schemaNode, error) {
	if pb.seen[t] {
		return nil, fmt.Errorf("recursive config type %s", t)
	}
	pb.seen[t] = true
	defer delete(pb.seen, t) // only ancestors count: sibling fields may share a nested type
	node := newNode().set("type", "object")
	var required []string
	props := newNode()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() || fieldName(f) == "-" {
			continue
		}
		name := fieldName(f)
		spec, err := parseSchemaTag(f.Name, f.Tag.Get("schema"))
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t.Name(), name, err)
		}
		field, err := pb.valueNode(f.Type, spec)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t.Name(), name, err)
		}
		props.set(name, field)
		if spec.hasDef {
			v, err := parseDefault(f.Type, spec.def)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.Name(), name, err)
			}
			pb.defs.byType[t] = append(pb.defs.byType[t], fieldDefault{index: i, value: v})
		}
		if !spec.optional && !spec.hasDef && f.Type.Kind() != reflect.Pointer {
			required = append(required, name)
		}
	}
	if len(required) > 0 {
		node.set("required", required)
	}
	if len(props.keys) > 0 {
		node.set("properties", props)
	}
	node.set("additionalProperties", false)
	return node, nil
}

func (pb *planBuilder) valueNode(t reflect.Type, spec fieldSpec) (*schemaNode, error) {
	n := newNode()
	base := t
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	switch base.Kind() {
	case reflect.String:
		n.set("type", "string")
	case reflect.Bool:
		n.set("type", "boolean")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n.set("type", "integer")
	case reflect.Float32, reflect.Float64:
		n.set("type", "number")
	case reflect.Slice:
		n.set("type", "array")
		item, err := pb.valueNode(base.Elem(), fieldSpec{})
		if err != nil {
			return nil, err
		}
		n.set("items", item)
	case reflect.Map:
		if base.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported config field type %s (map keys must be strings)", t)
		}
		n.set("type", "object")
		switch base.Elem() {
		case reflect.TypeFor[string]():
			n.set("additionalProperties", newNode().set("type", "string"))
		case reflect.TypeFor[any]():
			// free-form object (e.g. sql args): no value constraint
		default:
			return nil, fmt.Errorf("unsupported config field type %s (map values must be string or any)", t)
		}
	case reflect.Struct:
		obj, err := pb.objectNode(base)
		if err != nil {
			return nil, err
		}
		n = obj
	default:
		return nil, fmt.Errorf("unsupported config field type %s", t)
	}

	// Constraints, with kind checks so a mistyped tag fails at registration.
	if len(spec.enums) > 0 {
		if base.Kind() != reflect.String {
			return nil, fmt.Errorf("enum= applies to string fields, not %s", t)
		}
		n.set("enum", spec.enums)
	}
	if spec.minLen != nil {
		if base.Kind() != reflect.String {
			return nil, fmt.Errorf("minLen= applies to string fields, not %s", t)
		}
		n.set("minLength", *spec.minLen)
	}
	if spec.min != nil {
		if !isNumberKind(base.Kind()) {
			return nil, fmt.Errorf("min= applies to numeric fields, not %s", t)
		}
		n.set("minimum", *spec.min)
	}
	if spec.max != nil {
		if !isNumberKind(base.Kind()) {
			return nil, fmt.Errorf("max= applies to numeric fields, not %s", t)
		}
		n.set("maximum", *spec.max)
	}
	if spec.minItems != nil {
		if base.Kind() != reflect.Slice {
			return nil, fmt.Errorf("minItems= applies to array fields, not %s", t)
		}
		n.set("minItems", *spec.minItems)
	}
	if spec.maxItems != nil {
		if base.Kind() != reflect.Slice {
			return nil, fmt.Errorf("maxItems= applies to array fields, not %s", t)
		}
		n.set("maxItems", *spec.maxItems)
	}
	if spec.hasDef {
		switch base.Kind() {
		case reflect.String:
			n.set("default", spec.def)
		case reflect.Bool:
			v, err := strconv.ParseBool(spec.def)
			if err != nil {
				return nil, fmt.Errorf("default=%q is not a bool", spec.def)
			}
			n.set("default", v)
		default:
			if !isNumberKind(base.Kind()) {
				return nil, fmt.Errorf("default= applies to scalar fields, not %s", t)
			}
			f, err := strconv.ParseFloat(spec.def, 64)
			if err != nil {
				return nil, fmt.Errorf("default=%q is not a valid number", spec.def)
			}
			n.set("default", f)
		}
	}
	if spec.desc != "" {
		n.set("description", spec.desc)
	}
	return n, nil
}

func isNumberKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// schemaNode is a JSON object that renders its keys in insertion order, so
// generated schemas list properties in struct declaration order.
type schemaNode struct {
	keys []string
	vals map[string]any
}

func newNode() *schemaNode {
	return &schemaNode{vals: map[string]any{}}
}

func (n *schemaNode) set(k string, v any) *schemaNode {
	if _, ok := n.vals[k]; !ok {
		n.keys = append(n.keys, k)
	}
	n.vals[k] = v
	return n
}

func (n *schemaNode) render(b *strings.Builder, indent string) {
	if len(n.keys) == 0 {
		b.WriteString("{}")
		return
	}
	b.WriteString("{\n")
	inner := indent + "  "
	for i, k := range n.keys {
		b.WriteString(inner)
		b.WriteString(strconv.Quote(k))
		b.WriteString(": ")
		renderValue(b, n.vals[k], inner)
		if i < len(n.keys)-1 {
			b.WriteByte(',')
		}
		b.WriteByte('\n')
	}
	b.WriteString(indent)
	b.WriteByte('}')
}

func renderValue(b *strings.Builder, v any, indent string) {
	switch t := v.(type) {
	case string:
		b.WriteString(strconv.Quote(t))
	case bool:
		b.WriteString(strconv.FormatBool(t))
	case int:
		b.WriteString(strconv.Itoa(t))
	case float64:
		b.WriteString(strconv.FormatFloat(t, 'g', -1, 64))
	case []string:
		b.WriteByte('[')
		for i, s := range t {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote(s))
		}
		b.WriteByte(']')
	case *schemaNode:
		t.render(b, indent)
	default:
		panic(fmt.Sprintf("schema generator: unsupported value %T", v))
	}
}

// --- decode + defaults ---

// defaultTable maps each config struct type to the pre-parsed default values
// of its fields.
type defaultTable struct {
	byType map[reflect.Type][]fieldDefault
}

type fieldDefault struct {
	index int
	value reflect.Value
}

func parseDefault(t reflect.Type, s string) (reflect.Value, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return reflect.ValueOf(s), nil
	case reflect.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("default=%q is not a bool", s)
		}
		return reflect.ValueOf(v).Convert(t), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(s, 10, t.Bits())
		if err != nil {
			return reflect.Value{}, fmt.Errorf("default=%q is not an int", s)
		}
		return reflect.ValueOf(v).Convert(t), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := strconv.ParseUint(s, 10, t.Bits())
		if err != nil {
			return reflect.Value{}, fmt.Errorf("default=%q is not a uint", s)
		}
		return reflect.ValueOf(v).Convert(t), nil
	case reflect.Float32, reflect.Float64:
		v, err := strconv.ParseFloat(s, t.Bits())
		if err != nil {
			return reflect.Value{}, fmt.Errorf("default=%q is not a float", s)
		}
		return reflect.ValueOf(v).Convert(t), nil
	default:
		return reflect.Value{}, fmt.Errorf("default= applies to scalar fields, not %s", t)
	}
}

// applyDefaults fills zero-valued fields with their tagged defaults,
// recursing into nested structs, pointers and slice elements (a default may
// sit inside a []struct, e.g. csv column types).
func applyDefaults(v reflect.Value, dt *defaultTable) {
	switch v.Kind() {
	case reflect.Struct:
		for _, d := range dt.byType[v.Type()] {
			if f := v.Field(d.index); f.IsZero() && f.CanSet() {
				f.Set(d.value)
			}
		}
		for i := 0; i < v.NumField(); i++ {
			applyDefaults(v.Field(i), dt)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			applyDefaults(v.Index(i), dt)
		}
	case reflect.Pointer:
		if !v.IsNil() {
			applyDefaults(v.Elem(), dt)
		}
	}
}

// decodeTyped turns a raw config value into C. Mapping configs (sources,
// sinks, codecs) arrive as map[string]any; transform configs may be scalars
// (the script plugin's source text). YAML-decoded scalars are normalized
// through a JSON round trip; the strict decoder rejects unknown struct
// fields, which normally never happens because schema validation runs first;
// it is defense in depth against schema/struct drift. Defaults are applied
// afterwards.
func decodeTyped[C any](cfg any, dt *defaultTable) (C, error) {
	var zero C
	if cfg == nil {
		cfg = map[string]any{}
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return zero, fmt.Errorf("plugin config: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var c C
	if err := dec.Decode(&c); err != nil {
		return zero, fmt.Errorf("plugin config: %w", err)
	}
	applyDefaults(reflect.ValueOf(&c).Elem(), dt)
	return c, nil
}

// Package registry defines the plugin registration model of Eventboat v3.
//
// Every plugin (source, sink, codec) must register a JSON Schema alongside its
// factory; configuration blocks are validated strictly against that schema
// (unknown fields are errors). This is the mechanism that keeps agents from
// inventing plugins or fields that do not exist (redesign-v3.md §5.6).
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Kind enumerates the three plugin sections.
type Kind string

const (
	KindSource Kind = "sources"
	KindSink   Kind = "sinks"
	KindCodec  Kind = "codecs"
)

// Message is the unit of data flowing through a pipeline. The engine owns the
// lifecycle: Raw is the spooled truth (original bytes plus codec marker),
// Decoded is the lazily decoded payload, Out is the final encoded form the
// engine computes for sinks from the sink's encoder, and Meta carries
// engine-stamped metadata (message_id, ingest_time, source) plus
// source-provided metadata.
type Message struct {
	ID      string
	Codec   string
	Raw     []byte
	Meta    map[string]any
	Decoded any
	Out     []byte // encoded bytes for sinks (engine-owned; falls back to Raw)
	Key     []byte // order_key evaluated at sinks; used e.g. as Kafka partition key

	SrcName string // emitting source node
	SrcSeq  int64  // per-source monotonic sequence, advances commit/watermark
	Cursor  string // pull sources: cursor column value of this row ("" if none)
}

// Source is implemented by source plugins. The engine calls Init with the
// persisted state before Run, and Settled whenever the contiguous frontier of
// settled (spooled, fully processed) messages advances; sources commit their
// own offsets there (Kafka offsets, file offsets, SQL watermarks).
type Source interface {
	Init(state []byte) error
	Run(ctx context.Context, emit func(Message))
	Settled(ctx context.Context, throughSrcSeq int64) (state []byte, err error)
	Close() error
}

// Sink is implemented by sink plugins. Batching is owned by the engine; Write
// receives one batch and reports success or failure per delivery policy.
type Sink interface {
	Write(ctx context.Context, msgs []Message) error
	Close() error
}

// Codec turns raw bytes into a decoded value and back.
type Codec interface {
	Decode(raw []byte) (any, error)
	Encode(v any) ([]byte, error)
}

// reservedNames may not be used as plugin names: they collide with node-level
// framework fields or edge attributes (redesign-v3-review.md R5).
var reservedNames = map[string]bool{
	"from": true, "decoder": true, "encoder": true, "workers": true,
	"order_key": true, "batch": true, "script": true, "split": true, "wasm": true,
	"when": true, "route": true, "buffer": true, "delivery": true, "required": true,
}

type sourceEntry struct {
	name         string
	schema       string
	compiled     *jsonschema.Schema
	capabilities []string
	factory      func(cfg map[string]any) (Source, error)
}

type sinkEntry struct {
	name     string
	schema   string
	compiled *jsonschema.Schema
	factory  func(cfg map[string]any) (Sink, error)
}

type codecEntry struct {
	name    string
	factory func(cfg map[string]any) (Codec, error)
}

// Registry holds all registered plugins. Use Default() for the process-wide
// registry that compiled-in builtins register into.
type Registry struct {
	mu      sync.RWMutex
	sources map[string]*sourceEntry
	sinks   map[string]*sinkEntry
	codecs  map[string]*codecEntry
}

func New() *Registry {
	return &Registry{
		sources: map[string]*sourceEntry{},
		sinks:   map[string]*sinkEntry{},
		codecs:  map[string]*codecEntry{},
	}
}

var defaultRegistry = New()

// Default returns the process-wide registry.
func Default() *Registry { return defaultRegistry }

func compileSchema(name, schema string) (*jsonschema.Schema, error) {
	var doc any
	if err := json.Unmarshal([]byte(schema), &doc); err != nil {
		return nil, fmt.Errorf("plugin %q: schema is not valid JSON: %w", name, err)
	}
	comp := jsonschema.NewCompiler()
	url := "https://eventboat.dev/schemas/" + name + ".json"
	if err := comp.AddResource(url, doc); err != nil {
		return nil, fmt.Errorf("plugin %q: invalid schema resource: %w", name, err)
	}
	sch, err := comp.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("plugin %q: schema does not compile: %w", name, err)
	}
	return sch, nil
}

// RegisterSource registers a source plugin with its JSON Schema (draft 2020-12
// recommended, additionalProperties:false expected) and optional capabilities
// such as "pull".
func (r *Registry) RegisterSource(name, schema string, capabilities []string, factory func(cfg map[string]any) (Source, error)) error {
	if reservedNames[name] {
		return fmt.Errorf("plugin name %q is reserved by the framework field whitelist", name)
	}
	if factory == nil {
		return fmt.Errorf("plugin %q: nil factory", name)
	}
	compiled, err := compileSchema("source/"+name, schema)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.sources[name]; dup {
		return fmt.Errorf("source plugin %q already registered", name)
	}
	r.sources[name] = &sourceEntry{name: name, schema: schema, compiled: compiled, capabilities: capabilities, factory: factory}
	return nil
}

// RegisterSink registers a sink plugin with its JSON Schema.
func (r *Registry) RegisterSink(name, schema string, factory func(cfg map[string]any) (Sink, error)) error {
	if reservedNames[name] {
		return fmt.Errorf("plugin name %q is reserved by the framework field whitelist", name)
	}
	if factory == nil {
		return fmt.Errorf("plugin %q: nil factory", name)
	}
	compiled, err := compileSchema("sink/"+name, schema)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.sinks[name]; dup {
		return fmt.Errorf("sink plugin %q already registered", name)
	}
	r.sinks[name] = &sinkEntry{name: name, schema: schema, compiled: compiled, factory: factory}
	return nil
}

// RegisterCodec registers a codec plugin.
func (r *Registry) RegisterCodec(name string, factory func(cfg map[string]any) (Codec, error)) error {
	if reservedNames[name] {
		return fmt.Errorf("plugin name %q is reserved by the framework field whitelist", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.codecs[name]; dup {
		return fmt.Errorf("codec %q already registered", name)
	}
	r.codecs[name] = &codecEntry{name: name, factory: factory}
	return nil
}

// LookupSource returns the source entry registered under name.
func (r *Registry) LookupSource(name string) (*SourceMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.sources[name]
	if !ok {
		return nil, false
	}
	return &SourceMeta{Name: e.name, Schema: e.schema, Capabilities: e.capabilities}, true
}

// NewSource instantiates a source plugin after validating cfg against its schema.
func (r *Registry) NewSource(name string, cfg map[string]any) (Source, error) {
	r.mu.RLock()
	e, ok := r.sources[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown source plugin %q", name)
	}
	if err := validate(e.compiled, name, cfg); err != nil {
		return nil, err
	}
	return e.factory(cfg)
}

// LookupSink returns the sink entry registered under name.
func (r *Registry) LookupSink(name string) (*SinkMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.sinks[name]
	if !ok {
		return nil, false
	}
	return &SinkMeta{Name: e.name, Schema: e.schema}, true
}

// NewSink instantiates a sink plugin after validating cfg against its schema.
func (r *Registry) NewSink(name string, cfg map[string]any) (Sink, error) {
	r.mu.RLock()
	e, ok := r.sinks[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown sink plugin %q", name)
	}
	if err := validate(e.compiled, name, cfg); err != nil {
		return nil, err
	}
	return e.factory(cfg)
}

// NewCodec instantiates a codec plugin.
func (r *Registry) NewCodec(name string, cfg map[string]any) (Codec, error) {
	r.mu.RLock()
	e, ok := r.codecs[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown codec %q", name)
	}
	return e.factory(cfg)
}

// SourceMeta describes a registered source (for catalog output).
type SourceMeta struct {
	Name         string
	Schema       string
	Capabilities []string
}

// SinkMeta describes a registered sink (for catalog output).
type SinkMeta struct {
	Name   string
	Schema string
}

// Catalog lists registered plugins grouped by section, sorted by name.
type Catalog struct {
	Sources []SourceMeta
	Sinks   []SinkMeta
	Codecs  []string
}

func (r *Registry) Catalog() Catalog {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c := Catalog{}
	for _, e := range r.sources {
		c.Sources = append(c.Sources, SourceMeta{Name: e.name, Schema: e.schema, Capabilities: e.capabilities})
	}
	for _, e := range r.sinks {
		c.Sinks = append(c.Sinks, SinkMeta{Name: e.name, Schema: e.schema})
	}
	for name := range r.codecs {
		c.Codecs = append(c.Codecs, name)
	}
	sort.Slice(c.Sources, func(i, j int) bool { return c.Sources[i].Name < c.Sources[j].Name })
	sort.Slice(c.Sinks, func(i, j int) bool { return c.Sinks[i].Name < c.Sinks[j].Name })
	sort.Strings(c.Codecs)
	return c
}

// SchemaError reports a plugin configuration that fails its JSON Schema. Path
// is the instance path relative to the plugin block.
type SchemaError struct {
	Plugin string
	Errors []SchemaIssue
}

type SchemaIssue struct {
	Path    string
	Message string
}

func (e *SchemaError) Error() string {
	s := fmt.Sprintf("plugin %q: configuration failed schema validation", e.Plugin)
	for _, iss := range e.Errors {
		s += fmt.Sprintf("\n  %s: %s", iss.Path, iss.Message)
	}
	return s
}

func validate(sch *jsonschema.Schema, plugin string, cfg map[string]any) error {
	if cfg == nil {
		cfg = map[string]any{}
	}
	if err := sch.Validate(cfg); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			se := &SchemaError{Plugin: plugin}
			flattenOutput(ve.BasicOutput(), &se.Errors)
			return se
		}
		return fmt.Errorf("plugin %q: %w", plugin, err)
	}
	return nil
}

func flattenOutput(u *jsonschema.OutputUnit, out *[]SchemaIssue) {
	if u.Error != nil {
		*out = append(*out, SchemaIssue{Path: u.InstanceLocation, Message: u.Error.String()})
	}
	for i := range u.Errors {
		flattenOutput(&u.Errors[i], out)
	}
}

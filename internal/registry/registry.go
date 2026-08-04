package registry

import (
	"fmt"
	"sync"

	"github.com/edgesets/edgestream/internal/codec"
	"github.com/edgesets/edgestream/internal/stage"
)

type SourceFactory func(id string, cfg map[string]any) (stage.Source, error)
type TransformFactory func(id string, cfg map[string]any) (stage.Transform, error)
type SinkFactory func(id string, cfg map[string]any) (stage.Sink, error)
type CodecFactory func(config map[string]any) (codec.Codec, error)

type Registry struct {
	mu         sync.RWMutex
	sources    map[string]SourceFactory
	transforms map[string]TransformFactory
	sinks      map[string]SinkFactory
	codecs     map[string]CodecFactory
}

func New() *Registry {
	return &Registry{
		sources:    make(map[string]SourceFactory),
		transforms: make(map[string]TransformFactory),
		sinks:      make(map[string]SinkFactory),
		codecs:     make(map[string]CodecFactory),
	}
}

func (r *Registry) RegisterSource(name string, factory SourceFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources[name] = factory
}

func (r *Registry) RegisterTransform(name string, factory TransformFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transforms[name] = factory
}

func (r *Registry) RegisterSink(name string, factory SinkFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[name] = factory
}

func (r *Registry) RegisterCodec(name string, factory CodecFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codecs[name] = factory
}

func (r *Registry) CreateSource(typ, id string, cfg map[string]any) (stage.Source, error) {
	r.mu.RLock()
	factory, ok := r.sources[typ]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown source type %q", typ)
	}
	return factory(id, cfg)
}

func (r *Registry) CreateTransform(typ, id string, cfg map[string]any) (stage.Transform, error) {
	r.mu.RLock()
	factory, ok := r.transforms[typ]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown transform type %q", typ)
	}
	return factory(id, cfg)
}

func (r *Registry) CreateSink(typ, id string, cfg map[string]any) (stage.Sink, error) {
	r.mu.RLock()
	factory, ok := r.sinks[typ]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown sink type %q", typ)
	}
	return factory(id, cfg)
}

func (r *Registry) CreateCodec(typ string, cfg map[string]any) (codec.Codec, error) {
	r.mu.RLock()
	factory, ok := r.codecs[typ]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown codec type %q", typ)
	}
	return factory(cfg)
}

func (r *Registry) HasSource(typ string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.sources[typ]
	return ok
}

func (r *Registry) HasTransform(typ string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.transforms[typ]
	return ok
}

func (r *Registry) HasSink(typ string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.sinks[typ]
	return ok
}

func (r *Registry) HasCodec(typ string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.codecs[typ]
	return ok
}

var Default = New()

func RegisterSource(name string, factory SourceFactory) {
	Default.RegisterSource(name, factory)
}

func RegisterTransform(name string, factory TransformFactory) {
	Default.RegisterTransform(name, factory)
}

func RegisterSink(name string, factory SinkFactory) {
	Default.RegisterSink(name, factory)
}

func RegisterCodec(name string, factory CodecFactory) {
	Default.RegisterCodec(name, factory)
}

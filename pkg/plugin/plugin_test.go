package plugin_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/pkg/plugin"
)

// The public registration surface must behave exactly like the internal
// typed one: schema generation, strict validation, defaults, capabilities
// and the error paths. The calls below omit explicit type arguments on
// purpose — S/C inference from the build function is the ergonomics plugin
// authors get. The out-of-module acceptance gate is the runnable
// examples/custom-build, which uses only this package from outside.

type echoConfig struct {
	Prefix string `json:"prefix" schema:"default=echo,desc=line prefix"`
	Upper  bool   `json:"upper" schema:"optional"`
}

type echoSink struct{ prefix string }

func (s *echoSink) Write(ctx context.Context, msgs []plugin.Message) error { return nil }
func (s *echoSink) Close() error                                           { return nil }

type tickConfig struct {
	Events int `json:"events" schema:"min=1,max=1000,default=10"`
}

type tickSource struct{ events int }

func (s *tickSource) Init(state []byte) error                            { return nil }
func (s *tickSource) Run(ctx context.Context, emit func(plugin.Message)) {}
func (s *tickSource) Pull(ctx context.Context, emit func(plugin.Message)) error {
	return nil
}
func (s *tickSource) Commit(ctx context.Context, throughSrcSeq int64) ([]byte, error) {
	return nil, nil
}
func (s *tickSource) Close() error { return nil }

type reverseTransform struct{}

func (t *reverseTransform) Init(env *plugin.TransformEnv) error { return nil }
func (t *reverseTransform) Apply(msg *plugin.Message) ([]*plugin.Message, error) {
	return []*plugin.Message{msg}, nil
}
func (t *reverseTransform) Close() error { return nil }

type rotConfig struct {
	Rotate int `json:"rotate" schema:"min=1,max=25,default=13"`
}

type rotCodec struct{ rotate int }

func (c *rotCodec) Decode(raw []byte) (any, error) { return string(raw), nil }
func (c *rotCodec) Encode(v any) ([]byte, error)   { return []byte(v.(string)), nil }

func init() {
	if err := plugin.RegisterSink("testecho", 1, func(c echoConfig) (*echoSink, error) {
		return &echoSink{prefix: c.Prefix}, nil
	}); err != nil {
		panic(err)
	}
	if err := plugin.RegisterSource("testtick", 1, []string{"pull"}, func(c tickConfig) (*tickSource, error) {
		return &tickSource{events: c.Events}, nil
	}); err != nil {
		panic(err)
	}
	if err := plugin.RegisterTransform("testrev", 1, nil, func(cfg string, dir string) (*reverseTransform, error) {
		if cfg == "" {
			return nil, &plugin.TransformError{DiagCode: "testrev_empty", Err: errors.New("empty script")}
		}
		return &reverseTransform{}, nil
	}); err != nil {
		panic(err)
	}
	if err := plugin.RegisterCodec("testrot", 1, func(c rotConfig, dir string) (*rotCodec, error) {
		return &rotCodec{rotate: c.Rotate}, nil
	}); err != nil {
		panic(err)
	}
}

func TestRegisterAllKinds(t *testing.T) {
	reg := registry.Default()

	sinkAny, err := reg.NewSink("testecho", map[string]any{})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if got := sinkAny.(*echoSink).prefix; got != "echo" {
		t.Errorf("sink default not applied: prefix = %q", got)
	}

	srcAny, err := reg.NewSource("testtick", map[string]any{"events": 5})
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	if got := srcAny.(*tickSource).events; got != 5 {
		t.Errorf("source events = %d, want 5", got)
	}
	if _, ok := srcAny.(plugin.PullSource); !ok {
		t.Error("source does not satisfy PullSource")
	}

	tr, err := reg.NewTransform("testrev", "1 + 1", "")
	if err != nil {
		t.Fatalf("new transform: %v", err)
	}
	if tr == nil {
		t.Fatal("transform is nil")
	}
	if _, err := reg.NewTransform("testrev", "", ""); err == nil || !strings.Contains(err.Error(), "empty script") {
		t.Errorf("empty scalar config: want factory error, got %v", err)
	}

	codecAny, err := reg.NewCodec("testrot", map[string]any{}, "")
	if err != nil {
		t.Fatalf("new codec: %v", err)
	}
	if got := codecAny.(*rotCodec).rotate; got != 13 {
		t.Errorf("codec default not applied: rotate = %d", got)
	}

	if _, err := reg.NewSink("testecho", map[string]any{"nope": 1}); err == nil {
		t.Error("unknown field accepted (schema not enforced)")
	}

	meta, ok := reg.LookupSource("testtick")
	if !ok {
		t.Fatal("source not in catalog")
	}
	if meta.Version != 1 || meta.Capabilities[0] != "pull" || !strings.Contains(meta.Schema, `"events"`) {
		t.Errorf("source meta = %+v", meta)
	}
}

func TestRegisterErrors(t *testing.T) {
	build := func(echoConfig) (*echoSink, error) { return nil, nil }

	err := plugin.RegisterSink("testecho", 1, build)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Errorf("duplicate registration: want error, got %v", err)
	}
	err = plugin.RegisterSink("from", 1, build)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("reserved name: want error, got %v", err)
	}
	err = plugin.RegisterSink("testold", 0, build)
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Errorf("version 0: want error, got %v", err)
	}
}

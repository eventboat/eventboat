package testkit

import (
	"github.com/eventboat/eventboat/internal/registry"
)

// TransformFunc adapts a function to registry.Transform: identity Init and
// Close, stateless Apply shared by all workers. Returning a nil slice
// filters the message (settle-as-filtered); an error dead-letters after the
// edge's delivery retries — both engine contracts, exercised by tests.
type TransformFunc func(msg *registry.Message) ([]*registry.Message, error)

func (f TransformFunc) Init(env *registry.TransformEnv) error                    { return nil }
func (f TransformFunc) Apply(msg *registry.Message) ([]*registry.Message, error) { return f(msg) }
func (f TransformFunc) Close() error                                             { return nil }

// RegisterFakeTransform registers fn as a transform plugin named name. It
// goes through the string-schema registration API — the documented escape
// hatch — with an accept-anything schema, so the fake works with any plugin
// block the test writes.
func RegisterFakeTransform(reg *registry.Registry, name string, fn TransformFunc) error {
	schema := `{"type": ["object", "array", "string", "integer", "number", "boolean", "null"]}`
	return reg.RegisterTransform(name, 1, schema, nil, func(cfg any, dir string) (registry.Transform, error) {
		return fn, nil
	})
}

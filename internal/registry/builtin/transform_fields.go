package builtin

import (
	"fmt"

	"github.com/eventboat/eventboat/internal/registry"
)

// fieldsConfig is the declarative field writer for the common collection
// case: static values (the loader substitutes ${VAR} and ${constants.x}
// before the plugin sees them) plus source metadata copied by name.
type fieldsConfig struct {
	Fields   map[string]any `json:"fields" schema:"optional,desc=static fields written into the payload (${VAR}/${constants.x} substituted at load)"`
	FromMeta []string       `json:"from_meta" schema:"optional,desc=meta keys copied into the payload under the same name; missing keys are skipped"`
}

// fieldsTransform copies configured fields onto the payload map (decision D5
// of the log-collection design): metadata travels as meta.*, and a pipeline
// that wants some of it in the payload declares which keys instead of writing
// Starlark boilerplate. Application order is payload copy, then from_meta
// (missing meta keys are skipped), then fields — so an explicitly configured
// static field always wins. The payload map is replaced, never mutated:
// fan-out branches share the decoded map (invariant 8).
type fieldsTransform struct {
	fields   map[string]any
	fromMeta []string
}

func registerFieldsTransform(reg *registry.Registry) error {
	return registry.RegisterTransformT[*fieldsTransform](reg, "fields", 1, []string{"explain-safe"},
		func(c fieldsConfig, _ string) (*fieldsTransform, error) {
			return &fieldsTransform{fields: c.Fields, fromMeta: c.FromMeta}, nil
		})
}

func (t *fieldsTransform) Init(*registry.TransformEnv) error { return nil }

func (t *fieldsTransform) Apply(msg *registry.Message) ([]*registry.Message, error) {
	src, ok := msg.Decoded.(map[string]any)
	if !ok {
		return nil, &registry.TransformError{
			Err:  fmt.Errorf("payload is %T, want an object (fields writes into a map)", msg.Decoded),
			Kind: registry.FailureOther,
		}
	}
	out := make(map[string]any, len(src)+len(t.fromMeta)+len(t.fields))
	for k, v := range src {
		out[k] = v
	}
	for _, k := range t.fromMeta {
		if v, ok := msg.Meta[k]; ok {
			out[k] = v
		}
	}
	for k, v := range t.fields {
		out[k] = v
	}
	msg.Decoded = out
	return []*registry.Message{msg}, nil
}

func (t *fieldsTransform) Close() error { return nil }

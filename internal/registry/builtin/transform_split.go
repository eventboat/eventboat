package builtin

import (
	"fmt"

	"github.com/eventboat/eventboat/internal/registry"
)

// splitConfig carries no fields: `split: {}` is the whole declaration. The
// schema (an object with no properties) still rejects typos like
// `split: {paralllel: true}`.
type splitConfig struct{}

// splitTransform turns an array payload into one message per element
// (redesign-v3-review R8). Children are shallow copies sharing the parent's
// spool identity, message_id and metadata; only Decoded is replaced. An
// empty array yields zero outputs, which the engine settles as filtered.
type splitTransform struct{}

func registerSplitTransform(reg *registry.Registry) error {
	return registry.RegisterTransformT[*splitTransform](reg, "split", 1, []string{"explain-safe"},
		func(_ splitConfig, _ string) (*splitTransform, error) {
			return &splitTransform{}, nil
		})
}

func (t *splitTransform) Init(env *registry.TransformEnv) error { return nil }

func (t *splitTransform) Apply(msg *registry.Message) ([]*registry.Message, error) {
	items, ok := msg.Decoded.([]any)
	if !ok {
		return nil, &registry.TransformError{Err: fmt.Errorf("payload is %T, want array", msg.Decoded), Flavor: "split"}
	}
	out := make([]*registry.Message, len(items))
	for i, item := range items {
		child := *msg // shallow copy; per-child Decoded only
		child.Decoded = item
		out[i] = &child
	}
	return out, nil
}

func (t *splitTransform) Close() error { return nil }

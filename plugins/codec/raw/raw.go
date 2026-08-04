package rawcodec

import (
	"fmt"

	"github.com/edgesets/edgestream/internal/codec"
	"github.com/edgesets/edgestream/internal/registry"
	cel "github.com/google/cel-go/cel"
)

func init() {
	registry.RegisterCodec("raw", func(cfg map[string]any) (codec.Codec, error) {
		r := &Raw{}
		if err := r.ValidateConfig(cfg); err != nil {
			return nil, err
		}
		return r, nil
	})
}

type Raw struct{}

func (r *Raw) Name() string { return "raw" }

func (r *Raw) Decode(payload []byte) (any, error) {
	return payload, nil
}

func (r *Raw) Encode(data any) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("raw codec: cannot encode %T", data)
	}
}

func (r *Raw) OutputType() *cel.Type {
	return cel.BytesType
}

// ValidateConfig rejects unknown fields: the raw codec takes no config, so a
// non-empty config almost certainly means a user typo that would be ignored.
func (r *Raw) ValidateConfig(cfg map[string]any) error {
	for k := range cfg {
		return fmt.Errorf("raw codec: unknown config field %q", k)
	}
	return nil
}

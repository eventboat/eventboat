package rawcodec

import (
	"github.com/edgesets/edgestream/internal/codec"
	"github.com/edgesets/edgestream/internal/registry"
	cel "github.com/google/cel-go/cel"
)

func init() {
	registry.RegisterCodec("raw", func(cfg map[string]any) (codec.Codec, error) {
		return &Raw{}, nil
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
		return nil, nil
	}
}

func (r *Raw) OutputType() *cel.Type {
	return cel.BytesType
}

func (r *Raw) ValidateConfig(map[string]any) error { return nil }

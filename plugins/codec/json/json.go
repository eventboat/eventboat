package jsoncodec

import (
	"encoding/json"

	"github.com/edgesets/edgestream/internal/codec"
	"github.com/edgesets/edgestream/internal/registry"
	cel "github.com/google/cel-go/cel"
)

func init() {
	registry.RegisterCodec("json", func(cfg map[string]any) (codec.Codec, error) {
		return &JSON{}, nil
	})
}

type JSON struct{}

func (j *JSON) Name() string { return "json" }

func (j *JSON) Decode(payload []byte) (any, error) {
	var out any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (j *JSON) Encode(data any) ([]byte, error) {
	return json.Marshal(data)
}

func (j *JSON) OutputType() *cel.Type {
	return cel.MapType(cel.StringType, cel.DynType)
}

func (j *JSON) ValidateConfig(map[string]any) error { return nil }

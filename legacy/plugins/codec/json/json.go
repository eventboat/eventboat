package jsoncodec

import (
	"encoding/json"
	"fmt"

	"github.com/riverpod/riverpod/internal/codec"
	"github.com/riverpod/riverpod/internal/registry"
	cel "github.com/google/cel-go/cel"
)

func init() {
	registry.RegisterCodec("json", func(cfg map[string]any) (codec.Codec, error) {
		j := &JSON{}
		if err := j.ValidateConfig(cfg); err != nil {
			return nil, err
		}
		return j, nil
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

// ValidateConfig rejects unknown fields: the json codec takes no config, so a
// non-empty config almost certainly means a user typo that would be ignored.
func (j *JSON) ValidateConfig(cfg map[string]any) error {
	for k := range cfg {
		return fmt.Errorf("json codec: unknown config field %q", k)
	}
	return nil
}

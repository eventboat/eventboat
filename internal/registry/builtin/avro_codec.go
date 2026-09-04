package builtin

import (
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/eventboat/eventboat/internal/registry"
)

// The avro codec decodes/encodes against an inline Avro schema (hamba/avro —
// the library LinkedIn itself migrated to; goavro is in maintenance mode,
// redesign-v3-review-m4.md §一). Generic payloads decode to map[string]any /
// []any / scalars, so CEL predicates and Starlark scripts see the same
// shapes the json codec produces. Type mapping: int/long → CEL int,
// float/double → CEL double, string/boolean direct, bytes → base64 string at
// the JSON boundary, unions → dyn (docs/codecs.md).

type avroCodecConfig struct {
	Schema string `json:"schema" schema:"minLen=1,desc=inline Avro schema (JSON), e.g. a record definition"`
}

func registerAvroCodec(reg *registry.Registry) error {
	return registry.RegisterCodecT(reg, "avro", 1, func(c avroCodecConfig, _ string) (registry.Codec, error) {
		parsed, err := avro.Parse(c.Schema)
		if err != nil {
			return nil, fmt.Errorf("avro codec: invalid schema: %w", err)
		}
		return &avroCodec{schema: parsed}, nil
	})
}

type avroCodec struct {
	schema avro.Schema
}

func (c *avroCodec) Decode(raw []byte) (any, error) {
	var v any
	if err := avro.Unmarshal(c.schema, raw, &v); err != nil {
		return nil, fmt.Errorf("avro decode: %w", err)
	}
	return v, nil
}

func (c *avroCodec) Encode(v any) ([]byte, error) {
	b, err := avro.Marshal(c.schema, v)
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	return b, nil
}

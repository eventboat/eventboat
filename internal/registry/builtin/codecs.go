package builtin

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/eventboat/eventboat/internal/registry"
)

type jsonCodecConfig struct {
	Pretty bool `json:"pretty" schema:"optional,desc=encode with indentation (sink side)"`
}

func registerJSONCodec(reg *registry.Registry) error {
	return registry.RegisterCodecT(reg, "json", 1, func(c jsonCodecConfig, _ string) (registry.Codec, error) {
		return &jsonCodec{pretty: c.Pretty}, nil
	})
}

// jsonCodec decodes raw bytes into a generic Go value (map/slice/scalars) and
// encodes any Go value back to compact (or indented) JSON.
type jsonCodec struct {
	pretty bool
}

func (c *jsonCodec) Decode(raw []byte) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("json decode: empty payload")
	}
	var v any
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}
	return v, nil
}

func (c *jsonCodec) Encode(v any) ([]byte, error) {
	// HTML escaping is deliberately OFF. json.Marshal escapes <, > and & as
	// \u003c/\u003e/\u0026 (six bytes each), so one log line full of shell
	// redirections or JSP markup can inflate by 6x — and the encoded bytes
	// are what VictoriaLogs measures against -insert.maxLineSizeBytes. JSON
	// does not require the escaping, and no consumer of the encoded form is
	// an HTML document. Control characters still expand (\u0000 etc.), which
	// is why the victorialogs sink keeps its encoded-line bound.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if c.pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}
	// json.Encoder terminates every value with a newline; the codec contract
	// is a bare value (sinks own their framing), so it is trimmed.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

type rawCodecConfig struct{}

func registerRawCodec(reg *registry.Registry) error {
	return registry.RegisterCodecT(reg, "raw", 1, func(c rawCodecConfig, _ string) (registry.Codec, error) {
		return &rawCodec{}, nil
	})
}

// rawCodec treats the payload as an opaque string: no decoding, no encoding.
type rawCodec struct{}

func (c *rawCodec) Decode(raw []byte) (any, error) { return string(raw), nil }

func (c *rawCodec) Encode(v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, fmt.Errorf("raw encode: nil value")
	case string:
		return []byte(t), nil
	case []byte:
		return t, nil
	default:
		return nil, fmt.Errorf("raw encode: unsupported type %T (raw codec passes strings through)", v)
	}
}

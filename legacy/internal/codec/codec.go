package codec

import cel "github.com/google/cel-go/cel"

type Codec interface {
	Name() string
	Decode(payload []byte) (any, error)
	Encode(data any) ([]byte, error)
	OutputType() *cel.Type
	ValidateConfig(config map[string]any) error
}

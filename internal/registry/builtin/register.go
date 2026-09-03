// Package builtin registers Eventboat's compiled-in plugins (redesign-v3.md
// §3.5 P0 set) into a registry. Every registration carries a strict JSON
// Schema (additionalProperties: false).
package builtin

import "github.com/eventboat/eventboat/internal/registry"

// RegisterAll registers all built-in plugins into reg.
func RegisterAll(reg *registry.Registry) error {
	for _, fn := range []func(*registry.Registry) error{
		registerJSONCodec,
		registerRawCodec,
		registerFileSource,
		registerCronSource,
		registerHTTPServerSource,
		registerKafkaSource,
		registerFileSink,
		registerHTTPSink,
		registerKafkaSink,
		registerDropSink,
	} {
		if err := fn(reg); err != nil {
			return err
		}
	}
	return nil
}

// Package plugin is the compile-time extension surface of Eventboat: a Go
// plugin registers a source, transform, sink or codec here (typically from
// an init function) and the pipeline YAML then uses it like any built-in —
// same verify gate, JSON Schema, `plugin catalog`, LSP hover and MCP
// catalog, because everything reads the same registry.
//
// The types below are aliases of the engine's plugin interfaces: the public
// ABI is exactly these definitions, frozen across releases even if the
// internal packages keep refactoring behind them. Registration targets the
// process-wide registry (the same instance the CLI builds from), so a
// custom main built on the root package's RunCLI picks registrations up
// with nothing more than a blank import of the plugin package.
//
// Dependency discipline: a plugin package imports this package only — never
// the root eventboat package, which links the whole engine. That keeps the
// packages third parties share (and their test binaries) light; only the
// final custom main pulls the engine in.
//
// Configuration follows the typed model of internal/registry: the JSON
// Schema is generated from the config type's `json` and `schema` tags
// (constraints, enums, defaults), the block is validated strictly against
// it, and the factory receives a decoded, defaults-applied value. A
// transform's config type may be a scalar (e.g. string) — the one kind
// whose plugin block is not forced to a mapping.
package plugin

import "github.com/eventboat/eventboat/internal/registry"

// The plugin ABI. These aliases are the stability contract: the engine
// hands *registry.Message to sources and sinks, and *Message to transform
// workers; changing those shapes is a breaking release, not a refactor.
type (
	// Message is the unit of data flowing through a pipeline. Fan-out
	// branches share the underlying Decoded/Meta maps — never mutate them
	// in place; assign a fresh value instead (see registry.Message).
	Message = registry.Message
	// Source is implemented by continuous source plugins.
	Source = registry.Source
	// PullSource is a source with job-pipeline pull semantics (declare the
	// "pull" capability).
	PullSource = registry.PullSource
	// Sink is implemented by sink plugins; the engine owns batching.
	Sink = registry.Sink
	// Transform is implemented by transform plugins: one message in, zero
	// or more out (zero filters).
	Transform = registry.Transform
	// TransformCloner opts a transform out of shared-worker execution when
	// its state is not goroutine-safe.
	TransformCloner = registry.TransformCloner
	// TransformFlavor feeds the engine's per-flavor transform metrics.
	TransformFlavor = registry.TransformFlavor
	// TransformEnv is the execution environment handed to Transform.Init.
	TransformEnv = registry.TransformEnv
	// TransformError is the structured failure detail a transform returns.
	TransformError = registry.TransformError
	// Codec turns raw bytes into a decoded value and back.
	Codec = registry.Codec
)

// RegisterSource registers a source plugin whose config contract is the
// struct C (schema generated from its tags). capabilities may declare
// "pull" (then S must also implement PullSource). Call from init; an error
// (reserved name, duplicate, version < 1, malformed constraint) means the
// plugin can never load — fail loudly.
func RegisterSource[S Source, C any](name string, version int, capabilities []string, build func(C) (S, error)) error {
	return registry.RegisterSourceT(registry.Default(), name, version, capabilities, build)
}

// RegisterSink registers a sink plugin whose config contract is the struct C.
func RegisterSink[S Sink, C any](name string, version int, build func(C) (S, error)) error {
	return registry.RegisterSinkT(registry.Default(), name, version, build)
}

// RegisterTransform registers a transform plugin. C is usually a struct but
// may be a scalar (the script plugin's config is the Starlark source
// text). dir is the pipeline file's directory, for configs that carry
// relative file paths. capabilities may declare "explain-safe" (the
// transform may be dry-run by `eventboat explain`).
func RegisterTransform[T Transform, C any](name string, version int, capabilities []string, build func(cfg C, dir string) (T, error)) error {
	return registry.RegisterTransformT(registry.Default(), name, version, capabilities, build)
}

// RegisterCodec registers a codec plugin; codecs are referenced by the
// node's `decoder:`/`encoder:` fields and `codecs:` declarations. T exists
// so the build can return the concrete pointer type (function values do
// not assign covariantly); the other three kinds get this from their S/T
// parameter directly.
func RegisterCodec[T Codec, C any](name string, version int, build func(cfg C, dir string) (T, error)) error {
	return registry.RegisterCodecT(registry.Default(), name, version, func(cfg C, dir string) (registry.Codec, error) {
		return build(cfg, dir)
	})
}

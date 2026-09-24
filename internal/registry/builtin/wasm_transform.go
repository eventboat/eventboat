package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/wasmhost"
)

// wasmTransform is the built-in wasm plugin (ladder tier 3). The factory
// compiles the guest once (existence and ABI export checks are verify
// findings, not first-message failures); the template instance owns the
// wazero runtime, and every worker goroutine runs on its own Clone because
// module instances are not goroutine-safe and die on traps (review-m3 R4).
// wasm is deliberately not explain-safe: explain does not execute guest
// code, so downstream sees the pre-transform payload (documented behavior).
type wasmTransform struct {
	cfg      wasmhost.Config
	compiled *wasmhost.Compiled
	invoker  *wasmhost.Invoker // nil on the template; each clone owns one
	owner    bool              // template closes the shared runtime
	logf     func(string, ...any)
	warnMs   int
}

func registerWasmTransform(reg *registry.Registry) error {
	return registry.RegisterTransformT[*wasmTransform](reg, "wasm", 1, nil,
		func(cfg wasmhost.Config, dir string) (*wasmTransform, error) {
			for _, a := range cfg.Allow {
				if a != "log" {
					return nil, fmt.Errorf("allow: unknown capability %q (known: log)", a)
				}
			}
			path := cfg.Module
			if dir != "" && !filepath.IsAbs(path) {
				path = filepath.Join(dir, path)
			}
			compiled, err := wasmhost.Compile(context.Background(), path, &cfg)
			if err != nil {
				return nil, &registry.TransformError{Err: err, Kind: wasmKindOf(err), DiagCode: "expr_wasm_compile",
					Hint: "the module must be a wasm32-wasip1 reactor exporting _initialize, eb_alloc and transform (docs/wasm.md)"}
			}
			return &wasmTransform{cfg: cfg, compiled: compiled, owner: true}, nil
		})
}

// wasmKind maps the wasm host's typed kind onto the registry failure kinds
// (the adapter seam; the engine never reads the host error text). A trap is
// the guest's runtime failure, so it maps to FailureRuntime.
func wasmKind(k wasmhost.Kind) registry.FailureKind {
	switch k {
	case wasmhost.KindCompile:
		return registry.FailureCompile
	case wasmhost.KindTimeout:
		return registry.FailureTimeout
	case wasmhost.KindGuest:
		return registry.FailureGuest
	case wasmhost.KindTrap:
		return registry.FailureRuntime
	default:
		return registry.FailureRuntime
	}
}

// wasmKindOf reads the typed kind off a host error; an untyped error is a
// runtime failure, never a timeout derived from its wording.
func wasmKindOf(err error) registry.FailureKind {
	var werr *wasmhost.Error
	if errors.As(err, &werr) {
		return wasmKind(werr.Kind)
	}
	return registry.FailureRuntime
}

func (w *wasmTransform) Init(env *registry.TransformEnv) error {
	w.logf = env.Logf
	if env.SlowCallWarn > 0 {
		w.warnMs = int(env.SlowCallWarn / time.Millisecond)
	}
	return nil
}

// Clone builds the per-worker invoker (one module instance per goroutine).
func (w *wasmTransform) Clone() (registry.Transform, error) {
	return &wasmTransform{
		cfg:      w.cfg,
		compiled: w.compiled,
		invoker:  w.compiled.NewInvoker(&w.cfg, w.logf, w.warnMs),
		logf:     w.logf,
		warnMs:   w.warnMs,
	}, nil
}

func (w *wasmTransform) Apply(msg *registry.Message) ([]*registry.Message, error) {
	if w.invoker == nil {
		// The engine shares non-cloning transforms but must clone cloners;
		// reaching the template here is an engine wiring bug, not user error.
		return nil, fmt.Errorf("no invoker: wasm requires per-worker clones")
	}
	// The host's message text is preserved as-is: classification rides Kind.
	fail := func(err error, kind registry.FailureKind) error {
		return &registry.TransformError{Err: err, Kind: kind}
	}
	in, err := json.Marshal(msg.Decoded)
	if err != nil {
		return nil, fail(fmt.Errorf("encode payload: %w", err), registry.FailureOther)
	}
	out, err := w.invoker.Invoke(context.Background(), in)
	if err != nil {
		return nil, fail(err, wasmKindOf(err))
	}
	if len(out) == 0 {
		// The guest returned a well-formed empty result: a guest contract
		// violation, not a host failure.
		return nil, fail(errors.New("transform returned empty output (payload must be JSON)"), registry.FailureGuest)
	}
	var decoded any
	if err := json.Unmarshal(out, &decoded); err != nil {
		return nil, fail(fmt.Errorf("output is not valid JSON: %v", err), registry.FailureGuest)
	}
	msg.Decoded = decoded
	return []*registry.Message{msg}, nil
}

func (w *wasmTransform) Close() error {
	if w.invoker != nil {
		_ = w.invoker.Close()
	}
	if w.owner {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return w.compiled.Close(ctx)
	}
	return nil
}

// Flavor feeds the engine's wasm metrics (duration histogram, timeout
// counter); the failure kind travels typed on TransformError.
func (w *wasmTransform) Flavor() string { return "wasm" }

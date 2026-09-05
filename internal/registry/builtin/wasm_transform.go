package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
				return nil, &registry.TransformError{Err: err, Flavor: "wasm",
					DiagCode: "expr_wasm_compile",
					Hint:     "the module must be a wasm32-wasip1 reactor exporting _initialize, eb_alloc and transform (docs/wasm.md)"}
			}
			return &wasmTransform{cfg: cfg, compiled: compiled, owner: true}, nil
		})
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
	fail := func(err error, flag string) error {
		return &registry.TransformError{Err: errors.New(strings.TrimPrefix(err.Error(), "wasm: ")), Flavor: "wasm", Flag: flag}
	}
	in, err := json.Marshal(msg.Decoded)
	if err != nil {
		return nil, fail(fmt.Errorf("encode payload: %w", err), "")
	}
	out, err := w.invoker.Invoke(context.Background(), in)
	if err != nil {
		flag := ""
		if strings.Contains(err.Error(), "exceeded") {
			flag = "timeout"
		}
		return nil, fail(err, flag)
	}
	if len(out) == 0 {
		return nil, fail(errors.New("transform returned empty output (payload must be JSON)"), "")
	}
	var decoded any
	if err := json.Unmarshal(out, &decoded); err != nil {
		return nil, fail(fmt.Errorf("output is not valid JSON: %v", err), "")
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
// counter).
func (w *wasmTransform) Flavor() string { return "wasm" }

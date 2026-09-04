// Package wasmhost runs WASM transforms (redesign-v3.md §6.5 ladder tier 3)
// under wazero with a capability sandbox: WASI preview1 instantiated with
// deterministic fake clocks, no filesystem, no env/args and discarded stdio
// by default; "log" capability routes guest stdio to the engine logger.
//
// Resource model (review-m3 R1): wazero has no fuel metering, so each invoke
// gets a wall-clock context deadline plus a hard memory-pages cap. A trap,
// timeout or memory overflow kills the instance (wazero closes it); the
// invoker re-instantiates from the shared CompiledModule on the next call.
//
// Guest ABI (review-m3 R3, full docs in docs/wasm.md): the module must be a
// wasip1 reactor exporting _initialize, eb_alloc(len i32) i32 and
// transform(ptr i32, len i32) i32; transform returns a pointer to 4-byte
// little-endian length + output bytes, or 0 on error (optional
// eb_last_error() i32 for the message).
package wasmhost

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/eventboat/eventboat/internal/config"
)

// Defaults mirror config.DefaultWasm*; the config layer owns the values.
const DefaultMaxMemoryPages = config.DefaultWasmMaxMemoryPages

// Compile compiles a guest module once; the result is safe to share across
// workers of the node it was compiled for. Memory cap and kill switch come
// from the node config: a positive timeout_ms enables wazero's
// CloseOnContextDone so per-invoke deadlines can kill runaway guests —
// measured at ~5x slower on loop-heavy guests on some platforms — while the
// default (unset / zero / negative) compiles the fast way with NO kill
// switch (M3-audit J2: the performance tier defaults to fast; verify warns
// on unset; a runaway guest then wedges its worker until the pipeline
// restarts, which the slow-call watchdog makes visible).
func Compile(ctx context.Context, path string, cfg *config.WasmConfig) (*Compiled, error) {
	wasmBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("wasmhost: read module: %w", err)
	}
	pages := 0
	timeout := 0
	if cfg != nil {
		pages = cfg.MaxMemoryPages
		if cfg.TimeoutMs > 0 {
			timeout = cfg.TimeoutMs
		}
	}
	if pages <= 0 {
		pages = DefaultMaxMemoryPages
	}
	rCfg := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(uint32(pages))
	if timeout > 0 {
		rCfg = rCfg.WithCloseOnContextDone(true)
	}
	r := wazero.NewRuntimeWithConfig(ctx, rCfg)
	// Capability floor: WASI preview1 with wazero's defaults — deterministic
	// fake clocks, no filesystem, no env/args, stdio discarded unless the
	// node opts into the "log" capability (R4).
	wasi_snapshot_preview1.MustInstantiate(ctx, r)
	compiled, err := r.CompileModule(ctx, wasmBytes)
	if err != nil {
		_ = r.Close(ctx)
		return nil, fmt.Errorf("wasmhost: compile %s: %w", path, err)
	}
	// ABI check up front: a module missing the exports fails verify, not the
	// first message (gate 1 catches it).
	for _, name := range []string{"_initialize", "eb_alloc", "transform"} {
		if compiled.ExportedFunctions()[name] == nil {
			_ = r.Close(ctx)
			return nil, fmt.Errorf("wasmhost: module %s does not export %q", path, name)
		}
	}
	return &Compiled{runtime: r, module: compiled}, nil
}

// Compiled is a shared, immutable compiled module.
type Compiled struct {
	runtime wazero.Runtime
	module  wazero.CompiledModule
}

// Close releases the runtime (after the last invoker).
func (c *Compiled) Close(ctx context.Context) error {
	return c.runtime.Close(ctx)
}

// NewInvoker builds a single-threaded invoker. slowCallWarnMs arms the
// zero-interference watchdog: an invoke still running after that long is
// logged once through logf (fast mode's only observability for a wedged
// call — killed calls never reach the duration histogram; <=0 disables).
// One Invoker belongs to one worker goroutine; concurrent Invokes on the
// same Invoker are not allowed (wazero modules are not goroutine-safe, R4).
func (c *Compiled) NewInvoker(cfg *config.WasmConfig, logf func(string, ...any), slowCallWarnMs int) *Invoker {
	timeoutMs := 0 // fast mode unless a positive budget is set
	entry := "transform"
	allowLog := false
	if cfg != nil {
		if cfg.TimeoutMs > 0 {
			timeoutMs = cfg.TimeoutMs
		}
		if cfg.Entrypoint != "" {
			entry = cfg.Entrypoint
		}
		for _, a := range cfg.Allow {
			if a == "log" {
				allowLog = true
			}
		}
	}
	return &Invoker{
		compiled:       c,
		entrypoint:     entry,
		timeout:        time.Duration(timeoutMs) * time.Millisecond,
		slowCallWarnMs: slowCallWarnMs,
		logf:           logf,
		allowLog:       allowLog,
	}
}

// Invoker owns one lazily created module instance and re-creates it after a
// trap, timeout or error (wazero closes trapped instances, R4).
type Invoker struct {
	compiled       *Compiled
	entrypoint     string
	timeout        time.Duration // 0 = fast mode: no deadline, no kill switch
	slowCallWarnMs int
	logf           func(string, ...any)
	allowLog       bool

	mod       api.Module
	transform api.Function
	alloc     api.Function
}

// Invoke runs one transform: payload in, payload out. An error is a
// transform failure the engine treats exactly like a Starlark failure
// (delivery retries, then dead letter). In fast mode there is no deadline —
// a runaway call blocks until the pipeline restarts, and the watchdog logs
// it once.
func (inv *Invoker) Invoke(parent context.Context, payload []byte) ([]byte, error) {
	ctx := parent
	if inv.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, inv.timeout)
		defer cancel()
	}
	// Zero-interference slow-call watchdog: log once if the call is still
	// running past the threshold. Never kills (that is the timeout's job in
	// protected mode, and deliberately absent in fast mode).
	var timer *time.Timer
	if inv.slowCallWarnMs > 0 && inv.logf != nil {
		entry := inv.entrypoint
		timer = time.AfterFunc(time.Duration(inv.slowCallWarnMs)*time.Millisecond, func() {
			inv.logf("wasm: invoke of %q still running after %dms (no kill switch armed; a runaway guest wedges this worker until restart)", entry, inv.slowCallWarnMs)
		})
		defer timer.Stop()
	}
	if err := inv.ensure(ctx); err != nil {
		return nil, err
	}
	res, err := inv.alloc.Call(ctx, uint64(len(payload)))
	if err != nil {
		inv.reset()
		return nil, fmt.Errorf("wasm: eb_alloc: %w", err)
	}
	ptr := uint32(res[0])
	if !inv.mod.Memory().Write(ptr, payload) {
		inv.reset()
		return nil, fmt.Errorf("wasm: eb_alloc returned a pointer outside memory")
	}
	res, err = inv.transform.Call(ctx, uint64(ptr), uint64(len(payload)))
	if err != nil {
		inv.reset()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("wasm: transform exceeded %s (per-invoke budget)", inv.timeout)
		}
		return nil, fmt.Errorf("wasm: transform trap: %w", err)
	}
	out, err := readResult(inv.mod, uint32(res[0]))
	if err != nil {
		inv.reset()
		return nil, err
	}
	return out, nil
}

func (inv *Invoker) ensure(ctx context.Context) error {
	if inv.mod != nil {
		return nil
	}
	cfg := wazero.NewModuleConfig().
		WithStartFunctions("_initialize")
	if inv.allowLog {
		w := writerFunc(func(s string) {
			if inv.logf != nil {
				inv.logf("wasm guest: %s", s)
			}
		})
		cfg = cfg.WithStdout(w).WithStderr(w)
	}
	mod, err := inv.compiled.runtime.InstantiateModule(ctx, inv.compiled.module, cfg)
	if err != nil {
		return fmt.Errorf("wasm: instantiate: %w", err)
	}
	inv.mod = mod
	inv.alloc = mod.ExportedFunction("eb_alloc")
	inv.transform = mod.ExportedFunction(inv.entrypoint)
	return nil
}

// reset closes a dead instance so the next Invoke re-instantiates.
func (inv *Invoker) reset() {
	if inv.mod != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = inv.mod.Close(ctx)
		cancel()
		inv.mod = nil
		inv.alloc, inv.transform = nil, nil
	}
}

// Close releases the instance (the shared Compiled stays usable).
func (inv *Invoker) Close() error {
	inv.reset()
	return nil
}

// readResult decodes the length-prefixed output buffer.
func readResult(mod api.Module, ptr uint32) ([]byte, error) {
	if ptr == 0 {
		// Optional error message export (length-prefixed like results).
		if fn := mod.ExportedFunction("eb_last_error"); fn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if res, err := fn.Call(ctx); err == nil && len(res) > 0 {
				if msg, err := readResult(mod, uint32(res[0])); err == nil && len(msg) > 0 {
					return nil, fmt.Errorf("wasm: transform error: %s", msg)
				}
			}
		}
		return nil, fmt.Errorf("wasm: transform returned an error (null pointer)")
	}
	header, ok := mod.Memory().Read(ptr, 4)
	if !ok {
		return nil, fmt.Errorf("wasm: result pointer outside memory")
	}
	n := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16 | uint32(header[3])<<24
	if n == 0 {
		return []byte{}, nil
	}
	out, ok := mod.Memory().Read(ptr+4, n)
	if !ok {
		return nil, fmt.Errorf("wasm: result body outside memory (len %d)", n)
	}
	return out, nil
}

type writerFunc func(string)

func (f writerFunc) Write(b []byte) (int, error) {
	if len(b) > 0 {
		f(string(b))
	}
	return len(b), nil
}

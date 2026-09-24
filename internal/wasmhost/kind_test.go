package wasmhost

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// miniGuest builds a minimal wasip1 reactor module that exercises every host
// error kind without a compiled guest, so the kind matrix is deterministic
// and independent of the Go WASM toolchain:
//
//	transform       → returns 0; eb_last_error points at errorText
//	transform_trap  → executes `unreachable`
//	transform_loop  → loops forever (the per-invoke budget kills it)
//
// The bytes are assembled here rather than committed as a binary blob. The
// module exports _initialize/eb_alloc/transform (the ABI check) plus the two
// extra entrypoints; eb_alloc hands the host a fixed 32 KiB payload window.
func miniGuest(t *testing.T, errorText string) string {
	t.Helper()
	const (
		allocPtr = 1024  // where the host writes the payload
		errPtr   = 32768 // where the length-prefixed error message lives
	)

	type wasmWriter struct{ b []byte }
	u32 := func(w *wasmWriter, v uint32) {
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if v != 0 {
				b |= 0x80
			}
			w.b = append(w.b, b)
			if v == 0 {
				return
			}
		}
	}
	raw := func(w *wasmWriter, bs ...byte) { w.b = append(w.b, bs...) }
	str := func(w *wasmWriter, s string) {
		u32(w, uint32(len(s)))
		w.b = append(w.b, s...)
	}
	section := func(w *wasmWriter, id byte, body []byte) {
		w.b = append(w.b, id)
		u32(w, uint32(len(body)))
		w.b = append(w.b, body...)
	}

	// Types: 0: ()->(); 1: (i32)->i32; 2: (i32,i32)->i32; 3: ()->i32.
	types := &wasmWriter{}
	u32(types, 4)
	raw(types, 0x60)
	u32(types, 0)
	u32(types, 0)
	raw(types, 0x60)
	u32(types, 1)
	raw(types, 0x7f)
	u32(types, 1)
	raw(types, 0x7f)
	raw(types, 0x60)
	u32(types, 2)
	raw(types, 0x7f, 0x7f)
	u32(types, 1)
	raw(types, 0x7f)
	raw(types, 0x60)
	u32(types, 0)
	u32(types, 1)
	raw(types, 0x7f)

	// Functions: 0 _initialize:t0, 1 eb_alloc:t1, 2 eb_last_error:t3,
	// 3 transform:t2, 4 transform_trap:t2, 5 transform_loop:t2.
	funcs := &wasmWriter{}
	u32(funcs, 6)
	for _, ti := range []uint32{0, 1, 3, 2, 2, 2} {
		u32(funcs, ti)
	}

	// One memory page (64 KiB): allocPtr and errPtr both fit.
	mem := &wasmWriter{}
	u32(mem, 1)
	raw(mem, 0x00)
	u32(mem, 1)

	exports := &wasmWriter{}
	u32(exports, 6)
	export := func(name string, idx uint32) {
		str(exports, name)
		raw(exports, 0x00) // kind: function
		u32(exports, idx)
	}
	export("_initialize", 0)
	export("eb_alloc", 1)
	export("eb_last_error", 2)
	export("transform", 3)
	export("transform_trap", 4)
	export("transform_loop", 5)

	code := &wasmWriter{}
	u32(code, 6)
	entry := func(instr ...byte) {
		body := append([]byte{0x00}, instr...) // zero local declarations
		u32(code, uint32(len(body)))
		raw(code, body...)
	}
	// i32Const encodes `i32.const <v>`.
	i32Const := func(v uint32) []byte {
		out := []byte{0x41}
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if v != 0 {
				b |= 0x80
			}
			out = append(out, b)
			if v == 0 {
				return out
			}
		}
	}
	ret := func(v uint32) []byte { return append(i32Const(v), 0x0b) } // ...; end
	entry(0x0b)                                                       // _initialize: end
	entry(ret(allocPtr)...)                                           // eb_alloc: return the payload window
	entry(ret(errPtr)...)                                             // eb_last_error: return the error buffer
	entry(ret(0)...)                                                  // transform: 0 = error signal
	entry(0x00, 0x0b)                                                 // transform_trap: unreachable; end
	entry(0x03, 0x40, 0x0c, 0x00, 0x0b, 0x00, 0x0b)                   // transform_loop: loop br 0 end; unreachable; end

	msg := make([]byte, 4+len(errorText))
	binary.LittleEndian.PutUint32(msg, uint32(len(errorText)))
	copy(msg[4:], errorText)
	data := &wasmWriter{}
	u32(data, 1)
	u32(data, 0) // active segment, memory 0
	raw(data, ret(errPtr)...)
	u32(data, uint32(len(msg)))
	data.b = append(data.b, msg...)

	mod := &wasmWriter{}
	raw(mod, 0x00, 0x61, 0x73, 0x6d) // \0asm
	raw(mod, 0x01, 0x00, 0x00, 0x00) // version 1
	section(mod, 1, types.b)
	section(mod, 3, funcs.b)
	section(mod, 5, mem.b)
	section(mod, 7, exports.b)
	section(mod, 10, code.b)
	section(mod, 11, data.b)

	path := filepath.Join(t.TempDir(), "mini-guest.wasm")
	if err := os.WriteFile(path, mod.b, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func hostKindOf(t *testing.T, err error) Kind {
	t.Helper()
	if err == nil {
		t.Fatal("expected a host error, got nil")
	}
	var werr *Error
	if !errors.As(err, &werr) {
		t.Fatalf("error is not a typed *wasmhost.Error: %T (%v)", err, err)
	}
	return werr.Kind
}

// Candidate 07 acceptance 2 (wasm host) and 5 (false positive): the host kind
// matrix — compile/timeout/guest/trap — is decided where each failure is
// created. A guest error whose TEXT contains "exceeded" stays a guest error;
// the old adapter classified it as a timeout by matching the message.
func TestHostErrorKinds(t *testing.T) {
	ctx := context.Background()

	// compile: unreadable and malformed modules.
	if _, err := Compile(ctx, filepath.Join(t.TempDir(), "missing.wasm"), nil); err == nil {
		t.Error("missing module accepted")
	} else if got := hostKindOf(t, err); got != KindCompile {
		t.Errorf("missing module kind = %q, want %q", got, KindCompile)
	}
	notWasm := filepath.Join(t.TempDir(), "not.wasm")
	if err := os.WriteFile(notWasm, []byte("not wasm"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(ctx, notWasm, nil); err == nil {
		t.Error("non-wasm module accepted")
	} else if got := hostKindOf(t, err); got != KindCompile {
		t.Errorf("non-wasm module kind = %q, want %q", got, KindCompile)
	}

	path := miniGuest(t, "sample count exceeded the guest's limit")
	compiled, err := Compile(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = compiled.Close(ctx) }()

	// guest: eb_last_error carries a message whose text contains "exceeded".
	inv := compiled.NewInvoker(nil, nil, 0)
	defer func() { _ = inv.Close() }()
	_, err = inv.Invoke(ctx, []byte(`{}`))
	if got := hostKindOf(t, err); got != KindGuest {
		t.Fatalf("guest error kind = %q, want %q (text-derived timeouts are the bug)", got, KindGuest)
	}
	if !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("guest error text lost: %v", err)
	}

	// trap: wazero reports the guest's fault.
	trapInv := compiled.NewInvoker(&Config{Entrypoint: "transform_trap"}, nil, 0)
	defer func() { _ = trapInv.Close() }()
	if _, err := trapInv.Invoke(ctx, []byte(`{}`)); err == nil {
		t.Error("trap accepted")
	} else if got := hostKindOf(t, err); got != KindTrap {
		t.Errorf("trap kind = %q, want %q", got, KindTrap)
	}

	// timeout: the per-invoke budget kills the loop, typed at the deadline.
	loopCompiled, err := Compile(ctx, path, &Config{TimeoutMs: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loopCompiled.Close(ctx) }()
	loopInv := loopCompiled.NewInvoker(&Config{Entrypoint: "transform_loop", TimeoutMs: 100}, nil, 0)
	defer func() { _ = loopInv.Close() }()
	start := time.Now()
	_, err = loopInv.Invoke(ctx, []byte(`{}`))
	if got := hostKindOf(t, err); got != KindTimeout {
		t.Fatalf("budget error kind = %q, want %q", got, KindTimeout)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("kill was not prompt: %v", elapsed)
	}
}

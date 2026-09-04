# WASM transforms (ladder tier 3)

WASM is the top of Eventboat's transform ladder
([redesign-v3.md](../redesign-v3.md) §4.5): CEL for predicates, Starlark for
mapping, **WASM for heavy computation, or when you need a native dependency**
(crypto, compression, parsing libraries). The bar is deliberate:

> **Trigger standard (§4.6): reach for WASM when the workload is performance-
> or dependency-bound, not because the logic is complex.** Starlark already
> covers complex logic; WASM's near-native speed is what it adds. Logic
> complexity alone is NOT a reason.

## Configuration

```yaml
transforms:
  heavy:
    from: [ingest]
    wasm:
      module: transforms/heavy.wasm   # path relative to the pipeline file
      entrypoint: transform           # exported function name (default: transform)
      timeout_ms: 1000                # per-invoke wall-clock budget (default 1000)
      max_memory_pages: 512           # linear memory cap, 64 KiB pages (default 512 = 32 MiB)
      allow: []                       # capability allowlist; known: log
```

`wasm:` is a transform main field, mutually exclusive with `script:` and
`split:`. `module` may reference `${ENV}` / `${constants.*}` /
`${parameters.*}` (resolved before the module is loaded each run).

## Semantics

- **Wire format**: the message payload goes in as **JSON bytes** (the decoded
  payload re-encoded, exactly what a Starlark script would see after an
  upstream script node); whatever comes back must be **valid JSON** and
  replaces the payload. Metadata passes through untouched — to modify meta,
  chain a small `script:` node (composition over configuration).
- **Error model**: guest errors (null-pointer return with `eb_last_error`),
  traps, timeouts and memory overflow are transform failures — the incoming
  edge's `delivery` policy retries them, then the message dead-letters with
  the error text. Identical to the Starlark error path.
- **Resource model**: wazero has no instruction-count metering, so the
  budget is **wall-clock per invoke** (`timeout_ms`, enforced by killing the
  instance exactly at the deadline) plus a **hard memory cap**
  (`max_memory_pages`). A killed instance is re-instantiated transparently
  before the next message. Timers are counted in
  `eventboat_wasm_timeouts_total`; durations in
  `eventboat_wasm_transform_duration_seconds`.
- **Capability sandbox**: WASI preview1 is instantiated with wazero's
  defaults — deterministic fake clocks, no filesystem, no env/args, stdio
  discarded. Nothing else is exposed. `allow: [log]` routes the guest's
  stdout/stderr into the engine log. There is no network, no host call API.
- **Verify compiles the module** (existence + required exports) at gate 1;
  a broken module is a verify error, not a first-message failure. `explain`
  does not dry-run the guest — downstream conditions are shown against the
  pre-transform payload.

## Guest ABI

The module must be a **wasm32-wasip1 reactor** (exports `_initialize`, no
`_start` side effects) and must export:

| export | signature | meaning |
|---|---|---|
| `eb_alloc` | `(len: i32) -> i32` | allocate `len` bytes, return pointer; host writes the payload there |
| `transform` (or your `entrypoint`) | `(ptr: i32, len: i32) -> i32` | read input at `ptr`, return pointer to output |
| `eb_last_error` | `() -> i32` | optional: pointer to the last error message |

Return buffers are **4-byte little-endian length prefix + bytes** in guest
memory, valid until the next call. Returning `0` from `transform` signals an
error; `eb_last_error` (same length-prefix format) explains it.

## Writing guests

**Go (the in-repo guest is built this way)** — any Go ≥1.24, standard
toolchain only:

```go
package main

import "unsafe"

var inbuf, outbuf []byte

//go:wasmexport eb_alloc
func ebAlloc(size uint32) uint32 {
	inbuf = make([]byte, size)
	return uint32(uintptr(unsafe.Pointer(&inbuf[0])))
}

//go:wasmexport transform
func transform(ptr, length uint32) uint32 {
	in := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length)
	outbuf = lengthPrefixed(compute(in)) // your logic, JSON in / JSON out
	return uint32(uintptr(unsafe.Pointer(&outbuf[0])))
}

func main() {} // reactor: never run as a command
```

Build with (the `-buildmode=c-shared` reactor form is what makes exported
functions callable without running `main`):

```
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o heavy.wasm .
```

The reference guest lives at
[internal/wasmhost/testdata/guest](../internal/wasmhost/testdata/guest/main.go)
(array statistics, also the benchmark workload), with its compiled
`aggregate.wasm` committed beside it so `go test ./...` needs no extra
toolchain. Rebuild with the command above (or set
`EVENTBOAT_WASM_GUEST=/path/to/guest.wasm` to point tests at a fresh build).

**Other languages** work the same way as long as they emit a wasip1 reactor:

- Rust: `crate-type = ["cdylib"]` + `#[no_mangle] pub extern "C" fn eb_alloc/transform`; build with `cargo build --target wasm32-wasip1 --release`. Ensure `_initialize` is exported (static initializers) — `wasm-bindgen` is not needed.
- TinyGo: `-target=wasi` with `//export` functions; TinyGo emits `_initialize` for library-style modules.
- AssemblyScript / Grain / anything with wasip1 support: export the three functions and don't run `_start`.

## Benchmark: when the WASM tier pays

The repo carries a same-workload benchmark (Starlark vs the Go WASM guest,
both doing multi-pass statistics over a numeric array):

```
go test ./internal/wasmhost/ -bench BenchmarkHeavyTransform -benchtime 2s
```

Reference numbers and how to reproduce them are in the
[README](../README.md#performance). Short version: heavy per-message
computation is where WASM wins by an order of magnitude; for simple field
mapping Starlark wins (instantiation and JSON crossing costs dominate at
small workloads) — which is exactly why the ladder exists.

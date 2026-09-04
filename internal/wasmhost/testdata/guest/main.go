// Command guest is the WASM transform guest used by the wasmhost tests and
// the Starlark-vs-WASM benchmark (redesign-v3.md §7.4 M3). It lives under
// testdata/ so the normal Go build ignores it; build it with:
//
//	GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o ../aggregate.wasm .
//
// Payload in: {"samples": [numbers], "passes": optional int}
// Payload out: {"count":N,"sum":S,"mean":M,"min":m,"max":x}
//
// The compute loop is deliberately heavy (multiple passes over the array):
// it is the "heavy script" workload the WASM tier exists for (§4.6).
package main

import (
	"encoding/json"
	"math"
	"unsafe"
)

var (
	inbuf  []byte
	outbuf []byte
	errbuf []byte
)

//go:wasmexport eb_alloc
func ebAlloc(size uint32) uint32 {
	if size == 0 {
		size = 1 // keep &buf[0] valid for empty payloads
	}
	inbuf = make([]byte, size)
	return uint32(uintptr(unsafe.Pointer(&inbuf[0])))
}

//go:wasmexport transform
func transform(ptr, length uint32) uint32 {
	in := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length)
	out, err := aggregate(in)
	if err != nil {
		errbuf = lengthPrefixed([]byte(err.Error()))
		return 0 // documented error signal; details via eb_last_error
	}
	outbuf = lengthPrefixed(out)
	return uint32(uintptr(unsafe.Pointer(&outbuf[0])))
}

//go:wasmexport eb_last_error
func ebLastError() uint32 {
	if errbuf == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(&errbuf[0])))
}

func lengthPrefixed(b []byte) []byte {
	out := make([]byte, 4+len(b))
	n := len(b)
	out[0] = byte(n)
	out[1] = byte(n >> 8)
	out[2] = byte(n >> 16)
	out[3] = byte(n >> 24)
	copy(out[4:], b)
	return out
}

func aggregate(in []byte) ([]byte, error) {
	var req struct {
		Values []float64 `json:"samples"`
		Passes int       `json:"passes"`
	}
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	if len(req.Values) == 0 {
		return nil, errString("samples must not be empty")
	}
	passes := req.Passes
	if passes <= 0 {
		passes = 20
	}
	sum, mn, mx := 0.0, math.Inf(1), math.Inf(-1)
	for p := 0; p < passes; p++ {
		for _, v := range req.Values {
			sum += v + float64(p)*1e-9
			if v < mn {
				mn = v
			}
			if v > mx {
				mx = v
			}
		}
	}
	n := len(req.Values)
	out, err := json.Marshal(map[string]any{
		"count": n,
		"sum":   sum,
		"mean":  sum / float64(n),
		"min":   mn,
		"max":   mx,
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type errString string

func (e errString) Error() string { return string(e) }

func main() {} // reactor: never run as a command

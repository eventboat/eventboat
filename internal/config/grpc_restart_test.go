package config

import (
	"path/filepath"
	"testing"
)

// grpc.restart policy parsing: fast-fail (default/""), restart, and the
// error for anything else.
func TestGrpcRestartParsing(t *testing.T) {
	manifest := filepath.ToSlash(filepath.Join("..", "..", "examples", "plugins", "ticker-source", "manifest.json"))
	mk := func(restart string) *Result {
		return LoadBytes("p.yaml", []byte(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: x }
sources:
  in:
    grpc:
      command: ["./plugin"]
      schema: `+manifest+`
      restart: `+restart+`
    ticker: { symbol: X }
sinks:
  out: { from: [in], file: { path: b } }
`))
	}

	res := mk("restart")
	if res.HasErrors() {
		t.Fatalf("restart rejected: %+v", res.Diagnostics)
	}
	if got := res.Pipeline.Sources["in"].Grpc.Restart; got != "restart" {
		t.Fatalf("restart = %q", got)
	}

	res = mk(`"fast-fail"`)
	if res.HasErrors() {
		t.Fatalf("fast-fail rejected: %+v", res.Diagnostics)
	}
	if got := res.Pipeline.Sources["in"].Grpc.Restart; got != "fast-fail" {
		t.Fatalf("restart = %q", got)
	}

	res = mk("always")
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == "cfg_grpc_restart" {
			found = true
		}
	}
	if !found {
		t.Fatalf("bad policy accepted: %+v", res.Diagnostics)
	}
}

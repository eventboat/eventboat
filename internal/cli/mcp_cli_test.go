package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// The mcp command validates the admin surface's security combination only
// when that surface will actually start: --http owns the admin listener, so
// a non-loopback bind without a token refuses there; a pure --stdio session
// has no admin listener at all and must start regardless of admin.listen
// (the same only-when-enabled guard run-dir applies via admin.enable). The
// stdio side needs a real stdin to serve, so it is verified by running the
// binary; here: the refusal path (returns before any listener is created).
func TestMCPHTTPRefusesNonLoopbackWithoutToken(t *testing.T) {
	t.Setenv("EVENTBOAT_ADMIN_TOKEN", "")
	rt := filepath.Join(t.TempDir(), "eventboat.yaml")
	cfg := "apiVersion: eventboat/v3\nkind: Runtime\nadmin:\n  listen: \"0.0.0.0:7788\"\n"
	if err := os.WriteFile(rt, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := cmdMCP([]string{"--http", "--runtime", rt}, false); code != 2 {
		t.Fatalf("non-loopback admin listen without token: exit = %d, want 2", code)
	}
}

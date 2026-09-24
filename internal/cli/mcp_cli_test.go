package cli

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eventboat/eventboat/internal/admin"
	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/store"
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
	cfg := "apiVersion: eventboat/v1\nkind: Runtime\nadmin:\n  listen: \"0.0.0.0:7788\"\n"
	if err := os.WriteFile(rt, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := cmdMCP([]string{"--http", "--runtime", rt}, false); code != 2 {
		t.Fatalf("non-loopback admin listen without token: exit = %d, want 2", code)
	}
}

// Candidate 06 acceptance 5b: the daemon gates /mcp on mcp.enable — disabled
// means the endpoint is not registered at all, while the admin surface still
// serves; the explicit `mcp --http` command always registers it.
func TestMCPEnableGatesDaemonEndpoint(t *testing.T) {
	reg, err := commandRegistry()
	if err != nil {
		t.Fatal(err)
	}
	owner := store.NewMemoryOwner()
	t.Cleanup(func() { _ = owner.Close() })
	svc := ops.New(ops.Options{Reg: reg, Stores: owner})
	t.Cleanup(svc.Stop)

	do := func(handler http.Handler, method, path string) int {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(`{}`)))
		return rec.Code
	}

	disabled := admin.Handler(svc, nil, mcpHandlerFor(svc, false), admin.Security{})
	if code := do(disabled, http.MethodPost, "/mcp"); code != http.StatusNotFound {
		t.Fatalf("mcp.enable=false still registers /mcp: status %d", code)
	}
	if code := do(disabled, http.MethodGet, "/admin/status.json"); code != http.StatusOK {
		t.Fatalf("admin surface broken with mcp disabled: status %d", code)
	}

	enabled := admin.Handler(svc, nil, mcpHandlerFor(svc, true), admin.Security{})
	if code := do(enabled, http.MethodPost, "/mcp"); code == http.StatusNotFound {
		t.Fatal("explicit MCP mode must register /mcp")
	}
}

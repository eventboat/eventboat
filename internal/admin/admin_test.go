package admin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/ops"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/registry/builtin"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

func newService(t *testing.T) *ops.Service {
	t.Helper()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := testkit.RegisterFakePull(reg); err != nil {
		t.Fatal(err)
	}
	return ops.New(ops.Options{
		DataDir:  t.TempDir(),
		Reg:      reg,
		StoreFor: func(pipeline string) (store.Store, error) { return store.NewMemory(pipeline), nil },
		Clock:    func() time.Time { return time.Now() },
	})
}

const tinyPipeline = `
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: tiny }
sources:
  in:
    decoder: json
    fakepull: { id: tiny-feed }
sinks:
  out:
    from: [in]
    file: { path: out.jsonl }
`

// The REST surface mirrors the MCP tools: deploy is verify-first, status is
// readable, and a failing deploy returns the diagnostics.
func TestAdminRESTSurface(t *testing.T) {
	svc := newService(t)
	t.Cleanup(svc.Stop)
	h := Handler(svc, nil, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	post := func(path, body string, wantCode int) string {
		t.Helper()
		res, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		b, _ := io.ReadAll(res.Body)
		if res.StatusCode != wantCode {
			t.Fatalf("%s: status %d, want %d: %s", path, res.StatusCode, wantCode, b)
		}
		return string(b)
	}
	get := func(path string, wantCode int) string {
		t.Helper()
		res, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		b, _ := io.ReadAll(res.Body)
		if res.StatusCode != wantCode {
			t.Fatalf("GET %s: status %d, want %d: %s", path, res.StatusCode, wantCode, b)
		}
		return string(b)
	}

	// A broken deploy is rejected with diagnostics (verify-first).
	body := post("/admin/deploy",
		`{"config":"apiVersion: eventboat/v3\nkind: Pipeline\nmetadata: { name: bad }\nrun:\n  mode: job\nsources:\n  x:\n    decoder: json\n    cron: { expression: \"0 0 * * *\" }\nsinks:\n  o: { from: [x], file: { path: o } }\n"}`,
		http.StatusBadRequest)
	if !strings.Contains(body, "job_source_not_pull") {
		t.Fatalf("deploy diagnostics missing the capability error: %s", body)
	}

	// A good deploy succeeds and appears in status.
	cfgJSON, _ := json.Marshal(map[string]string{"config": tinyPipeline})
	post("/admin/deploy", string(cfgJSON), http.StatusOK)
	status := get("/admin/status.json", 200)
	if !strings.Contains(status, `"tiny"`) {
		t.Fatalf("status missing deployed pipeline: %s", status)
	}

	// Catalog and the UI are served.
	if cat := get("/admin/catalog.json", 200); !strings.Contains(cat, `"sources"`) {
		t.Fatalf("catalog broken: %.200s", cat)
	}
	if ui := get("/admin/", 200); !strings.Contains(ui, "Eventboat") {
		t.Fatalf("UI not served: %.200s", ui)
	}

	// SSE streams status snapshots.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/admin/sse", nil)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("SSE content type = %q", ct)
	}
	buf := make([]byte, 512)
	n, _ := res.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "event:") {
		t.Fatalf("SSE stream did not send events: %s", buf[:n])
	}
}

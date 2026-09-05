package admin

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
		StoreFor: func(pipeline string) (store.Store, error) { return store.NewMemory(), nil },
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
// readable, and a failing deploy returns the diagnostics. Loopback listen,
// no token: the historical behavior, now routed through the real security
// middleware (host allowlist on, auth off).
func TestAdminRESTSurface(t *testing.T) {
	svc := newService(t)
	t.Cleanup(svc.Stop)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(svc, nil, nil, Security{Listen: ln.Addr().String()})
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = ln
	srv.Start()
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

// A configured token guards EVERY endpoint on the listener (the write
// surface can deploy pipeline YAML that executes plugin commands): missing
// or wrong Authorization → 401, the correct bearer passes. The ?token=
// query form is accepted on the SSE endpoint only — EventSource cannot set
// headers — and rejected everywhere else (review-2026-09: a token leaked in
// a URL must not unlock the write surface). Every response carries
// X-Content-Type-Options: nosniff.
func TestAdminTokenAuth(t *testing.T) {
	svc := newService(t)
	t.Cleanup(svc.Stop)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(svc, nil, nil, Security{Token: "s3cret", Listen: ln.Addr().String()})
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = ln
	srv.Start()
	defer srv.Close()

	get := func(path, auth string, want int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		res, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		if res.StatusCode != want {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("GET %s (auth %q): status %d, want %d: %s", path, auth, res.StatusCode, want, b)
		}
		if h := res.Header.Get("X-Content-Type-Options"); h != "nosniff" {
			t.Errorf("GET %s: X-Content-Type-Options = %q, want nosniff", path, h)
		}
	}
	get("/admin/status.json", "", http.StatusUnauthorized)
	get("/admin/status.json", "Bearer wrong", http.StatusUnauthorized)
	get("/admin/status.json", "Bearer s3cret", http.StatusOK)
	// The query form is narrowed to /admin/sse: anywhere else it does not
	// authenticate, even with the correct token (header-only surface).
	get("/admin/status.json?token=s3cret", "", http.StatusUnauthorized)
	get("/admin/?token=s3cret", "", http.StatusUnauthorized)

	// SSE keeps the query form (EventSource cannot set headers) and streams
	// once authorized by it.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/admin/sse?token=s3cret", nil)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/sse?token=: status %d, want 200", res.StatusCode)
	}
	if h := res.Header.Get("X-Content-Type-Options"); h != "nosniff" {
		t.Errorf("SSE: X-Content-Type-Options = %q, want nosniff", h)
	}
	buf := make([]byte, 512)
	n, _ := res.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "event: hello") {
		t.Fatalf("SSE via ?token= did not stream events: %s", buf[:n])
	}
	// A wrong query token is still rejected on SSE itself.
	get("/admin/sse?token=wrong", "", http.StatusUnauthorized)

	// Deploy is denied before ops.Deploy runs; with the token it reaches
	// verify (and fails there on the bogus config — 400, not 401).
	post := func(path, auth, body string, want int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		res, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		if res.StatusCode != want {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("POST %s (auth %q): status %d, want %d: %s", path, auth, res.StatusCode, want, b)
		}
	}
	post("/admin/deploy", "", `{"config":"bogus"}`, http.StatusUnauthorized)
	post("/admin/deploy", "Bearer s3cret", `{"config":"bogus"}`, http.StatusBadRequest)

	// Browser navigations get the sign-in prompt as the 401 body (no data);
	// machine clients get plain text.
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/admin/", nil)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	res, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized || !strings.Contains(string(b), "admin token") {
		t.Fatalf("HTML 401: status %d, body %.200s", res.StatusCode, b)
	}
}

// The Host allowlist is the DNS-rebinding defense: only the loopback
// spellings of the configured listen address pass; anything else → 403.
func TestAdminHostAllowlist(t *testing.T) {
	svc := newService(t)
	t.Cleanup(svc.Stop)
	h := Handler(svc, nil, nil, Security{Listen: "127.0.0.1:7788"})
	do := func(host string) int {
		req := httptest.NewRequest(http.MethodGet, "/admin/status.json", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	for _, ok := range []string{"127.0.0.1:7788", "localhost:7788", "[::1]:7788", "127.0.0.1", "localhost"} {
		if c := do(ok); c != http.StatusOK {
			t.Errorf("Host %q: status %d, want 200", ok, c)
		}
	}
	for _, evil := range []string{"evil.example.com:7788", "evil.example.com", "127.0.0.1:9999"} {
		if c := do(evil); c != http.StatusForbidden {
			t.Errorf("Host %q: status %d, want 403", evil, c)
		}
	}
}

// Secure default: constructing security for a non-loopback bind without a
// token fails (callers refuse to start); loopback without a token and any
// bind with a token stay allowed.
func TestNewSecurityRefusesNonLoopbackWithoutToken(t *testing.T) {
	for _, listen := range []string{"0.0.0.0:7788", ":7788", "10.0.0.5:7788", "[::]:7788"} {
		if _, err := NewSecurity("", listen); err == nil {
			t.Errorf("NewSecurity(\"\", %q) unexpectedly ok", listen)
		}
	}
	for _, listen := range []string{"127.0.0.1:7788", "localhost:7788", "[::1]:7788"} {
		if _, err := NewSecurity("", listen); err != nil {
			t.Errorf("NewSecurity(\"\", %q): %v", listen, err)
		}
	}
	if _, err := NewSecurity("tok", "0.0.0.0:7788"); err != nil {
		t.Errorf("NewSecurity with token on wildcard bind: %v", err)
	}
}

// Request bodies are capped: an oversized POST gets 413 (and the connection
// is not worth keeping), a normal one still goes through. Every JSON body on
// the surface funnels through the body() helper, so one endpoint proves the
// wrapper.
func TestAdminBodySizeLimit(t *testing.T) {
	svc := newService(t)
	t.Cleanup(svc.Stop)
	h := Handler(svc, nil, nil, Security{Listen: "127.0.0.1:7788"})

	oversized := `{"config":"` + strings.Repeat("x", maxBodyBytes) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/deploy", strings.NewReader(oversized))
	req.Host = "127.0.0.1:7788"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: status %d, want 413: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "exceeds") {
		t.Fatalf("413 body should name the limit: %s", rec.Body.String())
	}

	// A small body still reaches the handler (and fails verify with 400 —
	// proof it was decoded and dispatched, not rejected by the wrapper).
	req = httptest.NewRequest(http.MethodPost, "/admin/deploy", strings.NewReader(`{"config":"bogus"}`))
	req.Host = "127.0.0.1:7788"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("normal body: status %d, want 400 (reached verify): %s", rec.Code, rec.Body.String())
	}
}

// A traversal metadata.name (../../evil) is rejected by verify-first at the
// loader, before ops.Deploy writes anything under <data-dir>/pipelines/.
func TestAdminDeployRejectsTraversalName(t *testing.T) {
	dir := t.TempDir()
	reg := registry.New()
	if err := builtin.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	svc := ops.New(ops.Options{
		DataDir:  dir,
		Reg:      reg,
		StoreFor: func(pipeline string) (store.Store, error) { return store.NewMemory(), nil },
	})
	t.Cleanup(svc.Stop)
	h := Handler(svc, nil, nil, Security{Listen: "127.0.0.1:7788"})

	req := httptest.NewRequest(http.MethodPost, "/admin/deploy", strings.NewReader(
		`{"config":"apiVersion: eventboat/v3\nkind: Pipeline\nmetadata: { name: ../../evil }\nsources:\n  in: { decoder: json, file: { path: a } }\nsinks:\n  out: { from: [in], file: { path: o } }\n"}`))
	req.Host = "127.0.0.1:7788"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("deploy with traversal name: status %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cfg_name_invalid") {
		t.Fatalf("deploy diagnostics missing cfg_name_invalid: %s", rec.Body.String())
	}
	written := []string{}
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			written = append(written, path)
		}
		return nil
	})
	if len(written) > 0 {
		t.Fatalf("rejected deploy wrote files: %v", written)
	}
}

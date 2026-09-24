package admin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Rethink follow-up 2026-09-24 (supplement A): the k8s probes referenced
// /live and /ready while the mux served neither (the shipped manifest put
// the pod in a restart loop). The endpoints now exist and are exempt from
// the bearer token — a kubelet probe cannot carry a Secret — while
// everything else stays gated.
func TestHealthEndpointsExemptFromToken(t *testing.T) {
	svc := newService(t)
	t.Cleanup(svc.Stop)
	srv := httptest.NewServer(Handler(svc, nil, nil, Security{Token: "secret"}))
	defer srv.Close()

	get := func(path string) (int, string) {
		t.Helper()
		res, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}

	if code, body := get("/live"); code != http.StatusOK || !strings.Contains(body, "ok") {
		t.Fatalf("/live = %d %q, want 200 ok", code, body)
	}
	if code, body := get("/ready"); code != http.StatusOK || !strings.Contains(body, "ready") {
		t.Fatalf("/ready = %d %q, want 200 ready", code, body)
	}
	// Everything else still requires the token.
	if code, _ := get("/admin/status.json"); code != http.StatusUnauthorized {
		t.Fatalf("/admin/status.json without a token = %d, want 401", code)
	}
	// A stopping service drains from the probes; liveness is the process.
	svc.Stop()
	if code, body := get("/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/ready after Stop = %d %q, want 503", code, body)
	}
	if code, _ := get("/live"); code != http.StatusOK {
		t.Fatalf("/live after Stop = %d, want 200", code)
	}
}

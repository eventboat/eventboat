package builtin

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

// vlRequest is one request as the test backend saw it.
type vlRequest struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   []byte
}

type vlCapture struct {
	mu   sync.Mutex
	reqs []vlRequest
}

func (c *vlCapture) snapshot() []vlRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]vlRequest(nil), c.reqs...)
}

// newVLBackend serves every request with status and records it. The mutex is
// the synchronization point between the handler goroutine and the assertions
// (the race detector needs it; the HTTP response alone does not order them).
func newVLBackend(t *testing.T, status int) (*httptest.Server, *vlCapture) {
	t.Helper()
	cap := &vlCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.mu.Lock()
		cap.reqs = append(cap.reqs, vlRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.Query(),
			header: r.Header.Clone(),
			body:   body,
		})
		cap.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, cap
}

func TestVictoriaLogsSinkPostsOneBatchAsJSONLines(t *testing.T) {
	reg := newReg(t)
	srv, cap := newVLBackend(t, http.StatusOK)

	// The trailing slash must not turn into //insert/jsonline.
	sink, err := reg.NewSink("victorialogs", map[string]any{"url": srv.URL + "/"})
	if err != nil {
		t.Fatal(err)
	}
	msgs := []registry.Message{
		{Out: []byte(`{"msg":"a"}`)},
		{Out: []byte(`{"msg":"b"}`)},
		{Raw: []byte(`{"msg":"c"}`)}, // no Out: encodedBytes falls back to Raw
	}
	if err := sink.Write(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}

	reqs := cap.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want exactly 1 (one Write = one POST)", len(reqs))
	}
	r := reqs[0]
	if r.method != http.MethodPost {
		t.Errorf("method = %s, want POST", r.method)
	}
	if r.path != "/insert/jsonline" {
		t.Errorf("path = %q, want /insert/jsonline", r.path)
	}
	if ct := r.header.Get("Content-Type"); ct != "application/stream+json" {
		t.Errorf("Content-Type = %q, want application/stream+json", ct)
	}
	want := "{\"msg\":\"a\"}\n{\"msg\":\"b\"}\n{\"msg\":\"c\"}\n"
	if string(r.body) != want {
		t.Errorf("body = %q, want %q (one encoded message per line)", r.body, want)
	}
	if err := sink.Close(); err != nil {
		t.Error(err)
	}
}

func TestVictoriaLogsSinkQueryParamsAndTenantHeaders(t *testing.T) {
	reg := newReg(t)
	srv, cap := newVLBackend(t, http.StatusOK)

	sink, err := reg.NewSink("victorialogs", map[string]any{
		"url":           srv.URL,
		"stream_fields": "host,app",
		"time_field":    "ts",
		"msg_field":     "message",
		"extra_fields":  "env,dc",
		"account_id":    42,
		"project_id":    7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), []registry.Message{{Out: []byte(`{"a":1}`)}}); err != nil {
		t.Fatal(err)
	}

	r := cap.snapshot()[0]
	for key, want := range map[string]string{
		"_stream_fields": "host,app",
		"_time_field":    "ts",
		"_msg_field":     "message",
		"_extra_fields":  "env,dc",
	} {
		if got := r.query.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if len(r.query) != 4 {
		t.Errorf("query = %v, want exactly the four configured parameters", r.query)
	}
	if got := r.header.Get("AccountID"); got != "42" {
		t.Errorf("AccountID header = %q, want 42", got)
	}
	if got := r.header.Get("ProjectID"); got != "7" {
		t.Errorf("ProjectID header = %q, want 7", got)
	}
}

func TestVictoriaLogsSinkOmitsUnsetParamsAndUsesDefaults(t *testing.T) {
	reg := newReg(t)
	srv, cap := newVLBackend(t, http.StatusOK)

	sink, err := reg.NewSink("victorialogs", map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	// The documented defaults land on the client/transport (the two knobs are
	// not observable over the wire).
	vl := sink.(*victorialogsSink)
	if vl.client.Timeout != 10*time.Second {
		t.Errorf("client timeout = %v, want 10s", vl.client.Timeout)
	}
	transport, ok := vl.client.Transport.(*http.Transport)
	if !ok || transport.MaxIdleConnsPerHost != 2 {
		t.Errorf("MaxIdleConnsPerHost = %v, want 2", transport.MaxIdleConnsPerHost)
	}
	if err := sink.Write(context.Background(), []registry.Message{{Out: []byte(`{"a":1}`)}}); err != nil {
		t.Fatal(err)
	}

	r := cap.snapshot()[0]
	if len(r.query) != 0 {
		t.Errorf("query = %v, want none when no parameter is configured", r.query)
	}
	if got := r.header.Get("AccountID"); got != "" {
		t.Errorf("AccountID header = %q, want absent at 0", got)
	}
	if got := r.header.Get("ProjectID"); got != "" {
		t.Errorf("ProjectID header = %q, want absent at 0", got)
	}
}

func TestVictoriaLogsSinkGzipsBody(t *testing.T) {
	reg := newReg(t)
	srv, cap := newVLBackend(t, http.StatusOK)

	sink, err := reg.NewSink("victorialogs", map[string]any{"url": srv.URL, "gzip": true})
	if err != nil {
		t.Fatal(err)
	}
	msgs := []registry.Message{
		{Out: []byte(`{"msg":"a"}`)},
		{Out: []byte(`{"msg":"b"}`)},
	}
	if err := sink.Write(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}

	r := cap.snapshot()[0]
	if enc := r.header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", enc)
	}
	if ct := r.header.Get("Content-Type"); ct != "application/stream+json" {
		t.Errorf("Content-Type = %q, want application/stream+json", ct)
	}
	zr, err := gzip.NewReader(bytes.NewReader(r.body))
	if err != nil {
		t.Fatalf("body is not valid gzip: %v", err)
	}
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"msg\":\"a\"}\n{\"msg\":\"b\"}\n"
	if string(got) != want {
		t.Errorf("decompressed body = %q, want %q", got, want)
	}
}

func TestVictoriaLogsSinkErrorMapping(t *testing.T) {
	reg := newReg(t)
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"200 committed", http.StatusOK, false},
		{"204 committed", http.StatusNoContent, false},
		{"400 permanent", http.StatusBadRequest, true},
		{"503 transient", http.StatusServiceUnavailable, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newVLBackend(t, tc.status)
			sink, err := reg.NewSink("victorialogs", map[string]any{"url": srv.URL})
			if err != nil {
				t.Fatal(err)
			}
			err = sink.Write(context.Background(), []registry.Message{{Out: []byte(`{"a":1}`)}})
			if tc.wantErr && err == nil {
				t.Fatal("Write succeeded, want an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Write: %v", err)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "victorialogs sink:") {
				t.Errorf("error %q lacks the victorialogs sink: prefix", err)
			}
		})
	}
}

func TestVictoriaLogsSinkRejectsBadConfig(t *testing.T) {
	reg := newReg(t)
	if _, err := reg.NewSink("victorialogs", map[string]any{}); err == nil {
		t.Error("sink without url accepted")
	}
	if _, err := reg.NewSink("victorialogs", map[string]any{"url": "127.0.0.1:9428"}); err == nil {
		t.Error("url without :// accepted")
	}
	if _, err := reg.NewSink("victorialogs", map[string]any{"url": "http://127.0.0.1:9428", "nope": 1}); err == nil {
		t.Error("unknown config field accepted")
	}
	if _, err := reg.NewSink("victorialogs", map[string]any{"url": "http://127.0.0.1:9428", "max_line_bytes": -1}); err == nil {
		t.Error("negative max_line_bytes accepted")
	}
}

// The ENCODED line guardrail (the D1/D2/D3 silent-drop fix): a non-object or
// over-long line is refused before the request is sent, so the engine's
// delivery policy dead-letters it instead of committing a 200 that VL used to
// answer while skipping the line. The error carries the message id, the
// reason and a <=128-byte line fragment for DLQ triage.
func TestVictoriaLogsSinkEncodedLineGuardrails(t *testing.T) {
	reg := newReg(t)

	long := func(n int) []byte {
		b := make([]byte, 0, n)
		b = append(b, `{"msg":"`...)
		for len(b) < n-2 {
			b = append(b, 'a')
		}
		b = append(b, `"}`...)
		return b
	}

	cases := []struct {
		name    string
		cfg     map[string]any
		msg     registry.Message
		wantErr string // "" = must pass
	}{
		{
			name:    "over-long encoded line",
			msg:     registry.Message{ID: "m-long", Out: long(262145)},
			wantErr: "over max_line_bytes 262144",
		},
		{
			name:    "array is not an object",
			msg:     registry.Message{ID: "m-arr", Out: []byte(`[1,2,3]`)},
			wantErr: "not a JSON object",
		},
		{
			name:    "string is not an object",
			msg:     registry.Message{ID: "m-str", Out: []byte(`"plain text"`)},
			wantErr: "not a JSON object",
		},
		{
			name:    "number is not an object",
			msg:     registry.Message{ID: "m-num", Out: []byte(`42`)},
			wantErr: "not a JSON object",
		},
		{
			name:    "null is not an object",
			msg:     registry.Message{ID: "m-null", Out: []byte(`null`)},
			wantErr: "not a JSON object",
		},
		{
			name:    "empty line",
			msg:     registry.Message{ID: "m-empty", Out: []byte(``)},
			wantErr: "not a JSON object",
		},
		{
			name:    "raw fallback must be valid JSON",
			msg:     registry.Message{ID: "m-badraw", Raw: []byte(`{"a": `)},
			wantErr: "raw payload is not valid JSON",
		},
		{
			name: "raw fallback object passes",
			msg:  registry.Message{ID: "m-raw", Raw: []byte(`{"a":1}`)},
		},
		{
			name: "valid object with encoded payload passes",
			msg:  registry.Message{ID: "m-ok", Out: []byte(`{"msg":"a<b>&c"}`)},
		},
		{
			name: "leading whitespace before the object is tolerated",
			msg:  registry.Message{ID: "m-ws", Out: []byte("  \n {\"a\":1}")},
		},
		{
			name:    "small explicit bound rejects a short line",
			cfg:     map[string]any{"max_line_bytes": 16},
			msg:     registry.Message{ID: "m-small", Out: []byte(`{"msg":"0123456789"}`)},
			wantErr: "over max_line_bytes 16",
		},
		{
			name: "max_line_bytes 0 disables the bound",
			cfg:  map[string]any{"max_line_bytes": 0},
			msg:  registry.Message{ID: "m-off", Out: []byte(`{"msg":"0123456789"}`)},
		},
		{
			name: "max_line_bytes 0 accepts a huge line",
			cfg:  map[string]any{"max_line_bytes": 0},
			msg:  registry.Message{ID: "m-off-huge", Out: long(300000)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, cap := newVLBackend(t, http.StatusOK)
			cfg := map[string]any{"url": srv.URL}
			for k, v := range tc.cfg {
				cfg[k] = v
			}
			sink, err := reg.NewSink("victorialogs", cfg)
			if err != nil {
				t.Fatal(err)
			}
			err = sink.Write(context.Background(), []registry.Message{tc.msg})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
				if len(cap.snapshot()) != 1 {
					t.Fatalf("valid line did not reach the backend")
				}
				return
			}
			if err == nil {
				t.Fatal("Write succeeded, want a guardrail error")
			}
			for _, want := range []string{tc.msg.ID, tc.wantErr, "line:"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q lacks %q", err, want)
				}
			}
			if len(cap.snapshot()) != 0 {
				t.Errorf("guardrail error still sent a request (VL would 200-skip it)")
			}
		})
	}
}

// The fragment in the guardrail error is bounded (a multi-megabyte line must
// not bloat the dead-letter row) and the default bound tracks VL's
// -insert.maxLineSizeBytes.
func TestVictoriaLogsSinkGuardrailFragmentAndDefaults(t *testing.T) {
	reg := newReg(t)
	srv, _ := newVLBackend(t, http.StatusOK)
	sink, err := reg.NewSink("victorialogs", map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if got := sink.(*victorialogsSink).maxLineBytes; got != 262144 {
		t.Fatalf("default maxLineBytes = %d, want 262144", got)
	}
	off, err := reg.NewSink("victorialogs", map[string]any{"url": srv.URL, "max_line_bytes": 0})
	if err != nil {
		t.Fatal(err)
	}
	if got := off.(*victorialogsSink).maxLineBytes; got != 0 {
		t.Fatalf("explicit 0 maxLineBytes = %d, want 0 (disabled)", got)
	}

	// A raw line far over the bound: the error must stay small.
	huge := append([]byte(`{"msg":"`), bytes.Repeat([]byte("x"), 1<<20)...)
	huge = append(huge, `"}`...)
	err = sink.Write(context.Background(), []registry.Message{{ID: "m-huge", Out: huge}})
	if err == nil {
		t.Fatal("huge line accepted")
	}
	if len(err.Error()) > 300 {
		t.Fatalf("guardrail error is %d bytes; the fragment must be bounded: %.120s...", len(err.Error()), err.Error())
	}
	if !strings.Contains(err.Error(), "1048586 bytes") { // the real length is still reported
		t.Errorf("error lacks the encoded length: %s", err)
	}
}

func TestVictoriaLogsSinkInCatalog(t *testing.T) {
	reg := newReg(t)
	meta, ok := reg.LookupSink("victorialogs")
	if !ok {
		t.Fatal("victorialogs not registered")
	}
	if meta.Version != 1 {
		t.Errorf("version = %d, want 1", meta.Version)
	}
	if !strings.Contains(meta.Schema, `"stream_fields"`) {
		t.Errorf("catalog schema lacks the generated knobs: %s", meta.Schema)
	}
	found := false
	for _, s := range reg.Catalog().Sinks {
		if s.Name == "victorialogs" {
			found = true
		}
	}
	if !found {
		t.Error("plugin catalog does not list victorialogs")
	}
}

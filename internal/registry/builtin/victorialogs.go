package builtin

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eventboat/eventboat/internal/registry"
)

type victorialogsSinkConfig struct {
	URL          string `json:"url" schema:"minLen=1,desc=VictoriaLogs base URL; the sink POSTs to <url>/insert/jsonline"`
	StreamFields string `json:"stream_fields" schema:"optional,desc=_stream_fields query parameter (e.g. host,app): the fields that identify the log stream"`
	TimeField    string `json:"time_field" schema:"optional,desc=_time_field query parameter: the JSON field carrying the event timestamp"`
	MsgField     string `json:"msg_field" schema:"optional,desc=_msg_field query parameter: the JSON field carrying the log message"`
	ExtraFields  string `json:"extra_fields" schema:"optional,desc=_extra_fields query parameter: comma-separated fields stored as extra fields"`
	Gzip         bool   `json:"gzip" schema:"default=false,desc=true gzips the request body (Content-Encoding: gzip)"`
	MaxIdleConns int    `json:"max_idle_conns" schema:"min=1,default=2,desc=HTTP transport MaxIdleConnsPerHost (connection reuse; one sink goroutine by design)"`
	TimeoutMS    int    `json:"timeout_ms" schema:"min=1,default=10000,desc=request timeout in milliseconds"`
	AccountID    int    `json:"account_id" schema:"min=0,default=0,desc=VictoriaLogs tenant account id; 0 omits the AccountID header"`
	ProjectID    int    `json:"project_id" schema:"min=0,default=0,desc=VictoriaLogs tenant project id; 0 omits the ProjectID header"`
	// A pointer: unset means 262144 (VL's -insert.maxLineSizeBytes default),
	// while an explicit 0 disables the bound (a non-pointer default would
	// swallow the explicit 0).
	MaxLineBytes *int `json:"max_line_bytes" schema:"default=262144,min=0,desc=encoded single-line byte bound; align with VictoriaLogs' -insert.maxLineSizeBytes; 0 disables"`
}

func registerVictoriaLogsSink(reg *registry.Registry) error {
	return registry.RegisterSinkT(reg, "victorialogs", 1, func(c victorialogsSinkConfig) (registry.Sink, error) {
		if !strings.Contains(c.URL, "://") {
			return nil, fmt.Errorf("victorialogs sink: url must be absolute")
		}
		// Only the configured knobs become query parameters: VL applies its
		// own defaults for the rest, and an empty parameter would override
		// them with nothing.
		params := url.Values{}
		if c.StreamFields != "" {
			params.Set("_stream_fields", c.StreamFields)
		}
		if c.TimeField != "" {
			params.Set("_time_field", c.TimeField)
		}
		if c.MsgField != "" {
			params.Set("_msg_field", c.MsgField)
		}
		if c.ExtraFields != "" {
			params.Set("_extra_fields", c.ExtraFields)
		}
		endpoint := strings.TrimRight(c.URL, "/") + "/insert/jsonline"
		if q := params.Encode(); q != "" {
			endpoint += "?" + q
		}
		// Clone the default transport instead of zero-valuing one: proxy env,
		// dial timeouts and HTTP/2 stay, only the idle pool is resized. One
		// sink goroutine by design (no sinks.workers), so the pool only needs
		// to keep the next request's connection warm.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConnsPerHost = c.MaxIdleConns
		maxLineBytes := 0
		if c.MaxLineBytes != nil {
			maxLineBytes = *c.MaxLineBytes
		}
		return &victorialogsSink{
			endpoint:     endpoint,
			client:       &http.Client{Timeout: time.Duration(c.TimeoutMS) * time.Millisecond, Transport: transport},
			gzip:         c.Gzip,
			accountID:    c.AccountID,
			projectID:    c.ProjectID,
			maxLineBytes: maxLineBytes,
		}, nil
	})
}

// victorialogsSink ships one batch as one POST to VictoriaLogs' JSON-line
// ingest endpoint (/insert/jsonline): the body is the batch's engine-encoded
// JSON, one object per line, and VL commits the whole request.
//
// Error mapping follows VL's semantics (design 2026-09-24 §2.1):
//
//   - 2xx is committed. Note VL SKIPS lines it cannot ingest server-side and
//     still answers 200 — that is exactly why the body must be codec-encoded
//     JSON: shipping raw text would lose lines silently. This sink does not
//     trust that protocol either: every encoded line is validated against the
//     jsonline contract BEFORE the request is sent (validateLine), so a
//     violation becomes a delivery error and the batch dead-letters visibly
//     instead of being committed while VL drops the line.
//   - 4xx (VL only returns one when EVERY line in the batch is unparseable)
//     is permanent. The sink still returns a plain error: the edge's delivery
//     retry policy exhausts and the batch dead-letters — re-sending cannot
//     help — and no new engine error type is invented for it.
//   - 5xx / network error / timeout are transient: VL applies pushback instead
//     of dropping while overloaded, and the blocked write is what drives
//     engine backpressure (spool fills, the file source pauses reading).
type victorialogsSink struct {
	endpoint  string
	client    *http.Client
	gzip      bool
	accountID int
	projectID int

	// maxLineBytes is the bound on the ENCODED single line, aligned with VL's
	// -insert.maxLineSizeBytes; 0 disables the check.
	maxLineBytes int
}

// Write performs the batch's single POST. The engine hands over one batch per
// call, so this is one request — not one per message (that is the whole point
// of the dedicated sink over the generic http sink).
func (s *victorialogsSink) Write(ctx context.Context, msgs []registry.Message) error {
	if len(msgs) == 0 {
		// Nothing to deliver: VL rejects an empty body, and no message exists
		// to attribute a failure to.
		return nil
	}
	body := make([]byte, 0, 128*len(msgs))
	for _, m := range msgs {
		line := encodedBytes(m)
		if err := s.validateLine(m, line); err != nil {
			return err
		}
		body = append(body, line...)
		body = append(body, '\n')
	}
	if s.gzip {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			return fmt.Errorf("victorialogs sink: gzip: %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("victorialogs sink: gzip: %w", err)
		}
		body = buf.Bytes()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("victorialogs sink: %w", err)
	}
	req.Header.Set("Content-Type", "application/stream+json")
	if s.gzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if s.accountID != 0 {
		req.Header.Set("AccountID", strconv.Itoa(s.accountID))
	}
	if s.projectID != 0 {
		req.Header.Set("ProjectID", strconv.Itoa(s.projectID))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("victorialogs sink: %w", err)
	}
	// Read a bounded prefix (it explains which line VL refused) and drain the
	// rest so a keep-alive connection returns to the idle pool.
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()                     //nolint:errcheck
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return vlStatusError(resp.StatusCode, detail)
	}
	return nil
}

func (s *victorialogsSink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// validateLine enforces the jsonline contract on the ENCODED line before it
// enters the request body. VictoriaLogs is silent about violations: a
// non-object or an over-long line is skipped server-side while the request
// still answers 200, so an unchecked batch is committed by the engine while
// the rows never land (the D1/D2/D3 silent-drop failure). Returning an error
// here routes the batch through the edge's delivery policy into the DLQ,
// where the record is visible and replayable.
//
// Three checks:
//
//  1. the first non-whitespace byte must be '{': jsonline ingests JSON
//     objects only, and a decoded array/string/number/null (or a raw payload
//     the json encoder quoted into a string) would be skipped;
//  2. a message taking the Raw fallback (no engine-encoded Out — the batch
//     arrived with a nil Decoded) must additionally be valid JSON: the '{'
//     test alone accepts a truncated object;
//  3. when enabled, the encoded line must fit max_line_bytes — the bound is
//     on the encoded bytes because that is what VL measures (json encoding
//     expands control characters, and raw/csv payloads are not objects at
//     all).
func (s *victorialogsSink) validateLine(m registry.Message, line []byte) error {
	reason := ""
	trimmed := bytes.TrimLeft(line, " \t\r\n")
	switch {
	case len(trimmed) == 0 || trimmed[0] != '{':
		reason = "payload is not a JSON object (jsonline accepts objects only)"
	case len(m.Out) == 0 && !json.Valid(line):
		reason = "raw payload is not valid JSON"
	case s.maxLineBytes > 0 && len(line) > s.maxLineBytes:
		reason = fmt.Sprintf("encoded line is %d bytes, over max_line_bytes %d", len(line), s.maxLineBytes)
	default:
		return nil
	}
	return fmt.Errorf("victorialogs sink: message %s: %s (line: %q)", m.ID, reason, lineFragment(line))
}

// lineFragment renders at most 128 bytes of one offending line for the
// dead-letter reason; a longer line is cut and marked with "...".
func lineFragment(line []byte) string {
	const max = 128
	if len(line) <= max {
		return string(line)
	}
	return string(line[:max-3]) + "..."
}

// vlStatusError renders a non-2xx ingest response, echoing VL's explanation
// (bounded) so the dead-letter reason tells the operator which line was bad.
func vlStatusError(code int, detail []byte) error {
	if msg := strings.TrimSpace(string(detail)); msg != "" {
		return fmt.Errorf("victorialogs sink: unexpected status %d: %s", code, msg)
	}
	return fmt.Errorf("victorialogs sink: unexpected status %d", code)
}

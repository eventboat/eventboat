package testrun

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/engine"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
	"github.com/eventboat/eventboat/internal/store"
	"github.com/eventboat/eventboat/internal/testkit"
)

// CaseResult is the outcome of one contract case.
type CaseResult struct {
	Name     string
	Status   string // "pass" | "fail"
	Failures []string
}

// Report is the outcome of one suite.
type Report struct {
	Suite    string
	Pipeline string
	Cases    []CaseResult
}

// OK reports whether every case passed.
func (r *Report) OK() bool {
	for _, c := range r.Cases {
		if c.Status != "pass" {
			return false
		}
	}
	return true
}

type specFile struct {
	Suite    string     `yaml:"suite"`
	Pipeline string     `yaml:"pipeline"`
	Cases    []specCase `yaml:"cases"`
}

type specCase struct {
	Name   string     `yaml:"name"`
	Inject specInject `yaml:"inject"`
	Expect specExpect `yaml:"expect"`
}

type specInject struct {
	At       string   `yaml:"at"`
	Messages []string `yaml:"messages"`
	Raw      string   `yaml:"raw"`
}

type specExpect struct {
	Capture *specCapture `yaml:"capture"`
	DLQ     *specDLQ     `yaml:"dlq"`
}

type specCapture struct {
	At       string           `yaml:"at"`
	Count    *int             `yaml:"count"`
	Messages []map[string]any `yaml:"messages"`
}

type specDLQ struct {
	Count          *int   `yaml:"count"`
	ReasonContains string `yaml:"reason_contains"`
}

// IsSuite reports whether the YAML file at path declares a contract test
// suite (a top-level `suite:` key). Directory mode uses this to run suites
// and silently skip pipelines and unrelated YAML. A file that cannot be read
// or parsed yields a non-nil parseErr: callers must treat that as a hard
// error, never as "not a suite" — a broken suite must not vanish behind the
// skipped count (round-2 review #2).
func IsSuite(path string) (isSuite bool, parseErr error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var probe struct {
		Suite string `yaml:"suite"`
	}
	if err := yaml.Unmarshal(data, &probe); err != nil {
		return false, err
	}
	return probe.Suite != "", nil
}

// RunFile executes one contract test file (redesign-v3.md §3.2). The pipeline
// runs in-process against the real engine with an ephemeral store, a fixed
// clock and capture-wrapped sinks.
func RunFile(testFile string, reg *registry.Registry) (*Report, error) {
	data, err := os.ReadFile(testFile)
	if err != nil {
		return nil, err
	}
	var spec specFile
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("%s: %w", testFile, err)
	}
	if spec.Pipeline == "" {
		return nil, fmt.Errorf("%s: missing pipeline:", testFile)
	}
	if len(spec.Cases) == 0 {
		return nil, fmt.Errorf("%s: no cases:", testFile)
	}

	baseDir := filepath.Dir(testFile)
	pipelinePath := filepath.Clean(filepath.Join(baseDir, spec.Pipeline))

	lr := config.LoadFile(pipelinePath)
	if lr.HasErrors() {
		return nil, fmt.Errorf("%s: pipeline %s has verify errors:\n%s", testFile, pipelinePath, formatDiags(lr.Diagnostics))
	}
	piplineIR, diags := ir.Build(lr.Pipeline, reg, starhost.DefaultOptions())
	if hasErrDiags(diags) {
		return nil, fmt.Errorf("%s: pipeline %s has verify errors:\n%s", testFile, pipelinePath, formatDiags(diags))
	}

	report := &Report{Suite: spec.Suite, Pipeline: pipelinePath}
	for _, c := range spec.Cases {
		report.Cases = append(report.Cases, runCase(baseDir, pipelinePath, piplineIR, c, reg))
	}
	return report, nil
}

func runCase(baseDir, pipelinePath string, pip *ir.Pipeline, c specCase, reg *registry.Registry) CaseResult {
	result := CaseResult{Name: c.Name, Status: "pass"}
	fail := func(format string, args ...any) {
		result.Status = "fail"
		result.Failures = append(result.Failures, fmt.Sprintf(format, args...))
	}

	recorders := map[string]*testkit.Recorder{}
	opts := engine.DefaultOptions()
	opts.Clock = testkit.FixedClock(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC))
	opts.NewID = testkit.CounterID()
	opts.BackoffBase = time.Millisecond
	opts.DLBackoff = time.Millisecond
	opts.BatchFlush = 20 * time.Millisecond
	opts.DefaultTimeout = 2 * time.Second
	// Contract tests capture at the sink without touching the real world:
	// the wrapper replaces the plugin sink with a capture-only sink.
	opts.SinkWrapper = func(node string, s registry.Sink) registry.Sink {
		capture := testkit.NewCaptureSink(node, testkit.DiscardSink{})
		recorders[node] = capture.Rec
		return capture
	}

	st := store.NewMemory(pip.Config.Name)
	eng, err := engine.New(pip, st, reg, opts.WithLimits(pip.Config.Limits))
	if err != nil {
		fail("engine build: %v", err)
		return result
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	// Wait for the engine to be ready for injection.
	ready := false
	for i := 0; i < 200; i++ {
		if eng.Ready() {
			ready = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !ready {
		fail("engine did not start")
		cancel()
		return result
	}

	if c.Inject.At == "" {
		fail("inject.at is required")
	}
	if len(c.Inject.Messages) > 0 && c.Inject.Raw != "" {
		fail("inject: use either messages or raw, not both")
	}
	switch {
	case c.Inject.Raw != "":
		if _, err := eng.InjectAt(c.Inject.At, []byte(c.Inject.Raw), nil); err != nil {
			fail("inject raw at %s: %v", c.Inject.At, err)
		}
	case len(c.Inject.Messages) > 0:
		for _, mf := range c.Inject.Messages {
			raw, err := os.ReadFile(filepath.Clean(filepath.Join(baseDir, mf)))
			if err != nil {
				fail("read fixture %s: %v", mf, err)
				continue
			}
			if _, err := eng.InjectAt(c.Inject.At, raw, nil); err != nil {
				fail("inject %s at %s: %v", mf, c.Inject.At, err)
			}
		}
	default:
		fail("inject: messages or raw is required")
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	werr := eng.WaitSettled(waitCtx)
	waitCancel()
	if werr != nil {
		fail("engine: %v", werr)
	}

	// Evaluate expectations.
	if c.Expect.Capture != nil {
		rec, ok := recorders[c.Expect.Capture.At]
		if !ok {
			fail("capture.at %q is not a sink node", c.Expect.Capture.At)
		} else {
			captured := rec.Captured()
			if c.Expect.Capture.Count != nil && len(captured) != *c.Expect.Capture.Count {
				fail("capture %s: got %d messages, want %d", c.Expect.Capture.At, len(captured), *c.Expect.Capture.Count)
			}
			if err := matchExpectations(c.Expect.Capture.Messages, captured); err != nil {
				fail("capture %s: %v", c.Expect.Capture.At, err)
			}
		}
	}
	if c.Expect.DLQ != nil {
		dls, err := st.DeadLetters(pip.Config.Name)
		if err != nil {
			fail("dlq query: %v", err)
		} else {
			if c.Expect.DLQ.Count != nil && len(dls) != *c.Expect.DLQ.Count {
				fail("dlq: got %d dead letters, want %d", len(dls), *c.Expect.DLQ.Count)
			}
			if c.Expect.DLQ.ReasonContains != "" {
				found := false
				for _, dl := range dls {
					if strings.Contains(dl.Reason, c.Expect.DLQ.ReasonContains) {
						found = true
						break
					}
				}
				if !found {
					reasons := make([]string, 0, len(dls))
					for _, dl := range dls {
						reasons = append(reasons, dl.Reason)
					}
					sort.Strings(reasons)
					fail("dlq: no dead letter reason contains %q (reasons: %v)", c.Expect.DLQ.ReasonContains, reasons)
				}
			}
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	_ = st.Close()
	return result
}

// matchExpectations does in-order distinct matching: each expected entry must
// match a captured message at or after the previous match position.
func matchExpectations(expected []map[string]any, captured []registry.Message) error {
	pos := 0
	for i, exp := range expected {
		matched := -1
		for j := pos; j < len(captured); j++ {
			if ok, _ := subsetMatch(exp, captured[j]); ok {
				matched = j
				break
			}
		}
		if matched < 0 {
			return fmt.Errorf("expectation %d matched no captured message (from position %d)", i+1, pos)
		}
		pos = matched + 1
	}
	return nil
}

// subsetMatch checks dotted-path expectations ("payload.total", "meta.tier")
// against one captured message. Absent paths fail the expectation.
func subsetMatch(exp map[string]any, msg registry.Message) (bool, string) {
	for path, want := range exp {
		parts := strings.Split(path, ".")
		var root any
		switch parts[0] {
		case "payload":
			root = msg.Decoded
		case "meta":
			root = msg.Meta
		default:
			return false, fmt.Sprintf("expectation path %q must start with payload. or meta.", path)
		}
		got, found := walkPath(root, parts[1:])
		if !found {
			return false, fmt.Sprintf("%s: path not present", path)
		}
		if !valueEqual(got, want) {
			return false, fmt.Sprintf("%s: got %v (%T), want %v", path, got, got, want)
		}
	}
	return true, ""
}

func walkPath(v any, parts []string) (any, bool) {
	for _, p := range parts {
		switch t := v.(type) {
		case map[string]any:
			nv, ok := t[p]
			if !ok {
				return nil, false
			}
			v = nv
		default:
			return nil, false
		}
	}
	return v, true
}

func valueEqual(got, want any) bool {
	switch w := want.(type) {
	case int:
		switch g := got.(type) {
		case int:
			return g == w
		case int64:
			return g == int64(w)
		case float64:
			return g == float64(w)
		}
	case int64:
		switch g := got.(type) {
		case int64:
			return g == w
		case int:
			return int64(g) == w
		case float64:
			return g == float64(w)
		}
	case float64:
		switch g := got.(type) {
		case float64:
			return g == w
		case int64:
			return float64(g) == w
		case int:
			return float64(g) == w
		}
	case bool:
		g, ok := got.(bool)
		return ok && g == w
	case string:
		g, ok := got.(string)
		return ok && g == w
	case nil:
		return got == nil
	}
	return fmt.Sprintf("%v", got) == fmt.Sprintf("%v", want)
}

func formatDiags(diags []config.Diagnostic) string {
	var b strings.Builder
	for _, d := range diags {
		fmt.Fprintf(&b, "  %s\n", d.Error())
		if d.Hint != "" {
			fmt.Fprintf(&b, "    hint: %s\n", d.Hint)
		}
	}
	return b.String()
}

func hasErrDiags(diags []config.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == "error" {
			return true
		}
	}
	return false
}

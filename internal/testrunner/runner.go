package testrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/edgesets/edgestream/internal/config"
	"github.com/edgesets/edgestream/internal/engine"
	"github.com/edgesets/edgestream/internal/registry"
	"github.com/edgesets/edgestream/internal/testutil"
	"github.com/edgesets/edgestream/internal/topology"
	"gopkg.in/yaml.v3"
)

type Suite struct {
	Name     string     `yaml:"name"`
	Pipeline string     `yaml:"pipeline"`
	Cases    []TestCase `yaml:"cases"`
}

type TestCase struct {
	Name   string      `yaml:"name"`
	Input  []any       `yaml:"input"`
	Expect Expectation `yaml:"expect"`
}

type Expectation struct {
	Count    int              `yaml:"count"`
	Messages []MessageMatcher `yaml:"messages"`
}

type MessageMatcher struct {
	Payload map[string]any `yaml:"payload"`
}

type Result struct {
	Suite  string
	Case   string
	Passed bool
	Error  string
}

// RunFile executes a YAML test suite against a pipeline config.
func RunFile(path string, reg *registry.Registry) ([]Result, error) {
	if reg == nil {
		reg = registry.Default
	}
	testutil.Register(reg)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var suite Suite
	if err := yaml.Unmarshal(data, &suite); err != nil {
		return nil, fmt.Errorf("parse test suite: %w", err)
	}
	if suite.Pipeline == "" {
		return nil, fmt.Errorf("test suite: pipeline path is required")
	}
	pipelinePath := suite.Pipeline
	if !filepath.IsAbs(pipelinePath) {
		pipelinePath = filepath.Join(filepath.Dir(path), pipelinePath)
	}

	var results []Result
	for _, tc := range suite.Cases {
		res := Result{Suite: suite.Name, Case: tc.Name}
		if err := runCase(context.Background(), reg, pipelinePath, tc); err != nil {
			res.Error = err.Error()
		} else {
			res.Passed = true
		}
		results = append(results, res)
	}
	return results, nil
}

func runCase(ctx context.Context, reg *registry.Registry, pipelinePath string, tc TestCase) error {
	testutil.ResetCaptures()

	cfg, err := config.LoadFile(pipelinePath)
	if err != nil {
		return err
	}
	ir, err := topology.FromConfig(cfg)
	if err != nil {
		return err
	}
	sinkID, err := wireTestStages(ir, tc.Input)
	if err != nil {
		return err
	}

	eng := engine.New(reg)
	if err := eng.Load(ctx, ir); err != nil {
		return err
	}
	if err := eng.Start(ctx); err != nil {
		return err
	}

	cap, ok := testutil.CaptureSinkFor(sinkID)
	if !ok {
		_ = eng.Stop(ctx)
		return fmt.Errorf("capture sink %q not found", sinkID)
	}
	pipe, _ := eng.Pipeline(ir.Name)
	if err := waitForResult(pipe, cap, tc.Expect.Count, 5*time.Second); err != nil {
		_ = eng.Stop(context.Background())
		return err
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = eng.Stop(stopCtx)

	got := cap.Messages()
	if len(got) != tc.Expect.Count {
		return fmt.Errorf("message count = %d, want %d", len(got), tc.Expect.Count)
	}
	for i, matcher := range tc.Expect.Messages {
		if err := matchPayload(got[i].Payload, matcher.Payload); err != nil {
			return fmt.Errorf("message[%d]: %w", i, err)
		}
	}
	return nil
}

func waitForResult(pipe *engine.Pipeline, cap *testutil.CaptureSink, expectCount int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n := len(cap.Messages())
		inflight := int32(0)
		if pipe != nil {
			inflight = pipe.Inflight()
		}
		if n == expectCount && inflight == 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d output messages", expectCount)
}

func wireTestStages(ir *topology.TopologyIR, inputs []any) (sinkID string, err error) {
	var srcIdx, sinkIdx = -1, -1
	for i, st := range ir.Stages {
		switch st.Kind {
		case topology.KindSource:
			srcIdx = i
		case topology.KindSink:
			sinkIdx = i
		}
	}
	if srcIdx < 0 || sinkIdx < 0 {
		return "", fmt.Errorf("pipeline must have source and sink")
	}
	sinkID = ir.Stages[sinkIdx].ID
	ir.Stages[srcIdx].Type = testutil.SourceTypeGenerator
	ir.Stages[srcIdx].Config = map[string]any{"messages": inputs}
	ir.Stages[sinkIdx].Type = testutil.SinkTypeCapture
	ir.Stages[sinkIdx].Config = nil
	return sinkID, nil
}

func matchPayload(payload []byte, want map[string]any) error {
	if len(want) == 0 {
		return nil
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	for k, v := range want {
		gv, ok := got[k]
		if !ok {
			return fmt.Errorf("payload missing key %q", k)
		}
		gb, _ := json.Marshal(gv)
		wb, _ := json.Marshal(v)
		if string(gb) != string(wb) {
			return fmt.Errorf("payload[%q] = %v, want %v", k, gv, v)
		}
	}
	return nil
}

func FormatResults(results []Result) string {
	var b strings.Builder
	passed := 0
	for _, r := range results {
		if r.Passed {
			passed++
			fmt.Fprintf(&b, "PASS %s/%s\n", r.Suite, r.Case)
		} else {
			fmt.Fprintf(&b, "FAIL %s/%s: %s\n", r.Suite, r.Case, r.Error)
		}
	}
	fmt.Fprintf(&b, "\n%d passed, %d failed\n", passed, len(results)-passed)
	return b.String()
}

func AllPassed(results []Result) bool {
	for _, r := range results {
		if !r.Passed {
			return false
		}
	}
	return true
}

// Package verify owns the one verify-first composition: load → build → judge
// (candidate 05, design §3.4). Every surface — CLI, MCP, Admin, LSP, jobs,
// testrun, explain — enters here, so the same input yields the same
// diagnostic sequence and the same strict verdict everywhere, and no second
// composition exists.
//
// The package is a cycle-free leaf consumer of config/ir/registry: ops cannot
// host it (ops depends on testrun/explain/jobs, which need to call it).
//
// Two forms are exposed:
//
//   - the one-shot File/Bytes for verify-class callers, returning a Result
//     that carries the built *ir.Pipeline, the merged diagnostics and the
//     strict verdict OK;
//   - the two-stage LoadFile/LoadBytes → (caller step, e.g. jobs substituting
//     parameters) → Build, over the exact same primitives.
//
// Content-based entries take an explicit baseDir for relative paths (the LSP
// passes the document's directory, Deploy the deploy directory, the CLI the
// file's directory); a pure-text MCP/Admin submission passes "" and relative
// paths resolve against the process CWD.
package verify

import (
	"github.com/eventboat/eventboat/internal/config"
	"github.com/eventboat/eventboat/internal/ir"
	"github.com/eventboat/eventboat/internal/lang/starhost"
	"github.com/eventboat/eventboat/internal/registry"
)

// Options tunes the composition. Strict is the --strict policy; Star carries
// the Starlark sandbox options (zero value = the documented default budget);
// Parameters are resolved job parameter values (nil for non-job pipelines
// and for the declarative verify pass, which fills declared defaults).
type Options struct {
	Strict     bool
	Star       starhost.Options
	Parameters map[string]any
}

// Result is the verdict of one composition: the typed configuration, the
// built pipeline (nil when errors aborted the build) and the merged
// load+build diagnostics.
type Result struct {
	Config      *config.Pipeline
	Pipeline    *ir.Pipeline
	Diagnostics config.Diagnostics
	OK          bool // strict verdict: no errors; under Strict, no warnings either
}

// LoadFile is the first stage of the two-stage form for path-based callers.
func LoadFile(path string) *config.Result {
	return config.LoadFile(path)
}

// LoadBytes is the first stage of the two-stage form for content-based
// callers; baseDir is the explicit relative-path base ("" = process CWD).
func LoadBytes(name string, content []byte, baseDir string) *config.Result {
	return config.LoadBytesIn(name, baseDir, content)
}

// Build is the second stage of the two-stage form: it compiles the (possibly
// parameter-substituted) typed configuration into the IR. It is the only
// production build entry.
func Build(cfg *config.Pipeline, reg *registry.Registry, opts Options) (*ir.Pipeline, config.Diagnostics) {
	return ir.Build(cfg, reg, opts.Star, opts.Parameters)
}

// File verifies one pipeline file; relative paths resolve against the file's
// directory.
func File(path string, reg *registry.Registry, opts Options) *Result {
	return finish(config.LoadFile(path), reg, opts)
}

// Bytes verifies configuration content with an explicit base directory.
func Bytes(name string, content []byte, baseDir string, reg *registry.Registry, opts Options) *Result {
	return finish(config.LoadBytesIn(name, baseDir, content), reg, opts)
}

// finish merges the stages and applies the strict policy once. A load that
// already errored skips the build: cascade diagnostics from a half-decoded
// config are noise, and every surface must agree on that policy (the CLI's
// historical behavior — ops.Verify used to build anyway; design §3.4).
func finish(lr *config.Result, reg *registry.Registry, opts Options) *Result {
	res := &Result{Config: lr.Pipeline, Diagnostics: append(config.Diagnostics(nil), lr.Diagnostics...)}
	if lr.Pipeline != nil && !res.Diagnostics.HasErrors() {
		pip, buildDiags := Build(lr.Pipeline, reg, opts)
		res.Pipeline = pip
		res.Diagnostics = append(res.Diagnostics, buildDiags...)
	}
	res.OK = res.Diagnostics.StrictOK(opts.Strict)
	return res
}

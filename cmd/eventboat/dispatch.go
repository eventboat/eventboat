package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/lynx-go/commands"
)

// The dispatch layer: one verb per command, assembled into a
// lynx-go/commands App. The verbs declare the flags in SetFlags (so the
// framework owns parse errors, the -h screens and `help <verb>`), then Run
// rebuilds the flag tokens and delegates to the cmdX executors — those keep
// their own flag parsing and diagnostics, unchanged and still directly
// callable from tests.

const jsonFlagHelp = "machine-readable output for agents and CI (the global --json, also accepted here)"

// newApp assembles the dispatch table. --json is a global root bool (parsed
// wherever it appears, delivered via Environment.RootBools); renderSilent
// keeps the cmdX executors the single source of error lines — the framework
// adds only the exit code and the usage hint.
func newApp() *commands.App {
	app := commands.New()
	app.RootBoolFlags = []string{"json"}
	app.RenderError = renderSilent
	app.HelpHeader = "eventboat — agent-native event router (v3 POC)"
	app.VerbTitle = "commands:"
	app.HelpFooter = `Global flags:
  --json    machine-readable output for agents and CI (any position:
            eventboat --json verify, eventboat verify --json)

Run 'eventboat help <verb>' for a verb's usage and flags.`
	app.Register(
		&verifyVerb{}, &testVerb{}, &runVerb{}, &triggerVerb{}, &jobsVerb{},
		&explainVerb{}, &replayVerb{}, &replVerb{}, &lspVerb{}, &pluginVerb{}, &mcpVerb{},
	)
	return app
}

// errSilent marks an exit whose diagnostics the cmdX executor has already
// printed; renderSilent renders it as an empty stderr line.
var errSilent = errors.New("eventboat: diagnostics already printed")

func renderSilent(err error) string {
	if errors.Is(err, errSilent) {
		return ""
	}
	return err.Error()
}

// exitErr maps a cmdX exit code onto the framework's error contract: 0 is
// nil, 2 is a UsageError (exit code 2, the verb's usage line appended as the
// hint — the diagnostic itself was printed by the executor), anything else
// is a silent error (exit code 1).
func exitErr(code int, usage string) error {
	switch code {
	case commands.ExitOK:
		return nil
	case commands.ExitUsage:
		return &commands.UsageError{Usage: usage, Err: errSilent}
	default:
		return errSilent
	}
}

// jsonOut resolves the machine-output switch from its two accepted
// spellings: the global root bool (stripped wherever it appears) and the
// verb-level fallback flag (e.g. after a "--" terminator).
func jsonOut(env *commands.Environment, verbJSON bool) bool {
	return env.RootBools["json"] || verbJSON
}

// rebuildArgs reconstructs the argument list for a cmdX executor: the flag
// tokens the framework actually parsed (exact values; the verb-level --json
// is excluded, it travels as the jsonOut parameter), followed by the
// positional arguments — flags first, because the executors' flag parsing
// stops at the first positional.
func rebuildArgs(fs *flag.FlagSet, rest []string) []string {
	var args []string
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "json" {
			return
		}
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			args = append(args, "-"+f.Name+"="+f.Value.String())
			return
		}
		args = append(args, "-"+f.Name, f.Value.String())
	})
	return append(args, rest...)
}

// flaggy is the shared plumbing of the Flagged verbs: the framework's
// FlagSet is kept so Run can rebuild the invocation for the executor.
type flaggy struct {
	fs   *flag.FlagSet
	json bool
}

func (f *flaggy) setJSON(fs *flag.FlagSet) {
	f.fs = fs
	fs.BoolVar(&f.json, "json", false, jsonFlagHelp)
}

type verifyVerb struct {
	flaggy
	config string
	strict bool
}

func (v *verifyVerb) Name() string { return "verify" }
func (v *verifyVerb) Synopsis() string {
	return "statically validate a pipeline: schema, topology, CEL+Starlark compilation, lint (§3.1)"
}
func (v *verifyVerb) Usage() string {
	return "eventboat [--json] verify --config <pipeline.yaml> [--strict]"
}

func (v *verifyVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.StringVar(&v.config, "config", "", "pipeline configuration file")
	fs.BoolVar(&v.strict, "strict", false, "upgrade warnings to errors")
}

func (v *verifyVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdVerify(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

type testVerb struct{ flaggy }

func (v *testVerb) Name() string { return "test" }
func (v *testVerb) Synopsis() string {
	return "run contract test suites against the real in-process engine (§3.2)"
}
func (v *testVerb) Usage() string { return "eventboat [--json] test <testfile-or-dir> [...]" }

func (v *testVerb) SetFlags(fs *flag.FlagSet) { v.setJSON(fs) }

func (v *testVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdTest(rest, jsonOut(env, v.json), env.Stdout), v.Usage())
}

type runVerb struct {
	flaggy
	config     string
	configDir  string
	runtime    string
	dataDir    string
	ephemeral  bool
	adminToken string
}

func (v *runVerb) Name() string { return "run" }
func (v *runVerb) Synopsis() string {
	return "execute a pipeline (spool+commit+checkpoint, SQLite store); --config-dir runs every pipeline in a directory"
}
func (v *runVerb) Usage() string {
	return "eventboat run --config <pipeline.yaml> [--data-dir DIR] [--ephemeral] | eventboat run --config-dir <dir> [--runtime runtime.yaml]"
}

func (v *runVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.StringVar(&v.config, "config", "", "pipeline configuration file")
	fs.StringVar(&v.configDir, "config-dir", "", "directory of pipeline YAML files (multi-pipeline daemon with admin surface)")
	fs.StringVar(&v.runtime, "runtime", "", "Runtime configuration file (telemetry endpoints; default: ./eventboat.yaml)")
	fs.StringVar(&v.dataDir, "data-dir", "data", "SQLite storage directory (deployment-level concern, POC flag)")
	fs.BoolVar(&v.ephemeral, "ephemeral", false, "in-memory store: nothing persists across restarts")
	fs.StringVar(&v.adminToken, "admin-token", "", "bearer token for the admin/MCP HTTP surface in --config-dir mode (required for non-loopback binds; also EVENTBOAT_ADMIN_TOKEN / admin.token)")
}

func (v *runVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdRun(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

type triggerVerb struct {
	flaggy
	config     string
	parameters string
	dataDir    string
	ephemeral  bool
}

func (v *triggerVerb) Name() string { return "trigger" }
func (v *triggerVerb) Synopsis() string {
	return "manually fire a job pipeline once, optionally with parameters (backfill)"
}
func (v *triggerVerb) Usage() string {
	return `eventboat [--json] trigger --config <job.yaml> [--parameters '{"from":"..."}']`
}

func (v *triggerVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.StringVar(&v.config, "config", "", "job pipeline configuration file")
	fs.StringVar(&v.parameters, "parameters", "", "JSON object of job parameters (e.g. backfill ranges)")
	fs.StringVar(&v.dataDir, "data-dir", "data", "SQLite storage directory")
	fs.BoolVar(&v.ephemeral, "ephemeral", false, "in-memory store (nothing persists)")
}

func (v *triggerVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdTrigger(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

// jobsVerb is a bare verb (no Flagged): its list/show subcommands keep the
// hand parsing that allows flags before or after the positional run-id.
type jobsVerb struct{}

func (v *jobsVerb) Name() string { return "jobs" }
func (v *jobsVerb) Synopsis() string {
	return "job run history: list or show (counts, parameters, dead letters)"
}
func (v *jobsVerb) Usage() string {
	return "eventboat [--json] jobs list --config <job.yaml> [--data-dir DIR] [--limit N] | eventboat [--json] jobs show <run-id> --config <job.yaml>"
}

func (v *jobsVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	if len(rest) == 0 {
		return &commands.UsageError{Usage: v.Usage(), Err: errors.New("jobs: missing subcommand (list | show)")}
	}
	if rest[0] != "list" && rest[0] != "show" {
		return &commands.UsageError{Usage: v.Usage(), Err: fmt.Errorf("jobs: unknown subcommand %q (list | show)", rest[0])}
	}
	return exitErr(cmdJobs(rest, env.RootBools["json"]), v.Usage())
}

type explainVerb struct {
	flaggy
	config   string
	message  string
	entry    string
	topology bool
}

func (v *explainVerb) Name() string { return "explain" }
func (v *explainVerb) Synopsis() string {
	return "deterministic walkthrough: symbolic, message-level with real CEL/Starlark, or --topology DAG render"
}
func (v *explainVerb) Usage() string {
	return "eventboat [--json] explain --config <pipeline.yaml> [--message f.json] [--topology]"
}

func (v *explainVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.StringVar(&v.config, "config", "", "pipeline configuration file")
	fs.StringVar(&v.message, "message", "", "sample message JSON file (message-level evaluation)")
	fs.StringVar(&v.entry, "at", "", "entry node for the sample message (default: first source)")
	fs.BoolVar(&v.topology, "topology", false, "render the DAG (mermaid + ASCII) instead of a trace")
}

func (v *explainVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdExplain(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

type replayVerb struct {
	flaggy
	config    string
	dlq       bool
	spoolMode bool
	jobRun    string
	since     string
	where     string
	from      int
	to        int
	at        string
	dryRun    bool
	ids       string
	limit     int
	dataDir   string
	ephemeral bool
	del       bool
}

func (v *replayVerb) Name() string { return "replay" }
func (v *replayVerb) Synopsis() string {
	return "re-inject dead letters (--dlq), a spool window (--spool) or one job run (--job) into a live pipeline"
}
func (v *replayVerb) Usage() string {
	return "eventboat [--json] replay --config <pipeline.yaml> (--dlq | --spool --from N | --job <run-id>) [--dry-run]"
}

func (v *replayVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.StringVar(&v.config, "config", "", "pipeline configuration file")
	fs.BoolVar(&v.dlq, "dlq", false, "replay dead letters (filtered by --since/--where)")
	fs.BoolVar(&v.spoolMode, "spool", false, "replay a spool window from --from <seq>")
	fs.StringVar(&v.jobRun, "job", "", "replay one job run's dead letters (run-id)")
	fs.StringVar(&v.since, "since", "", "duration filter for --dlq (e.g. 2h)")
	fs.StringVar(&v.where, "where", "", `CEL filter over {payload, meta} for --dlq (e.g. 'meta.region == "eu"')`)
	fs.IntVar(&v.from, "from", -1, "spool sequence to replay from (--spool)")
	fs.IntVar(&v.to, "to", -1, "spool sequence to replay through, inclusive (--spool)")
	fs.StringVar(&v.at, "at", "", "target node for re-injection (default: each message's origin node)")
	fs.BoolVar(&v.dryRun, "dry-run", false, "explain the paths instead of delivering")
	fs.StringVar(&v.ids, "ids", "", "comma-separated dead letter ids to replay (--dlq)")
	fs.IntVar(&v.limit, "limit", 100, "maximum messages to replay")
	fs.StringVar(&v.dataDir, "data-dir", "data", "SQLite storage directory")
	fs.BoolVar(&v.ephemeral, "ephemeral", false, "run the replay engine against an in-memory store (sinks still real)")
	fs.BoolVar(&v.del, "delete", false, "delete replayed dead letters after successful reinjection")
}

func (v *replayVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdReplay(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

type replVerb struct {
	flaggy
	message string
	cel     string
	script  string
}

func (v *replVerb) Name() string { return "repl" }
func (v *replVerb) Synopsis() string {
	return "evaluate CEL predicates and Starlark scripts against one sample message (§3.6/§4.4)"
}
func (v *replVerb) Usage() string {
	return "eventboat repl [--message sample.json] [--cel 'expr' | --script f.star]"
}

func (v *replVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.StringVar(&v.message, "message", "", "sample message JSON file (default: {})")
	fs.StringVar(&v.cel, "cel", "", "evaluate one CEL predicate against the message and exit")
	fs.StringVar(&v.script, "script", "", "run one Starlark script file against the message and exit")
}

func (v *replVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdRepl(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

// lspVerb is a bare verb: lsp takes no flags (stdio is the protocol channel;
// stray arguments stay a usage error inside cmdLSP).
type lspVerb struct{}

func (v *lspVerb) Name() string { return "lsp" }
func (v *lspVerb) Synopsis() string {
	return "language server over stdio: verify diagnostics, completion and hover for pipeline YAML"
}
func (v *lspVerb) Usage() string { return "eventboat lsp (stdio; no flags)" }

func (v *lspVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdLSP(rest, env.RootBools["json"]), v.Usage())
}

// pluginVerb's subcommands (catalog, schema) carry their own flags; the
// framework's parse stops at the subcommand word, so the arguments pass
// through untouched to cmdPlugin.
type pluginVerb struct{ flaggy }

func (v *pluginVerb) Name() string { return "plugin" }
func (v *pluginVerb) Synopsis() string {
	return "plugin ABI surface: catalog lists registered plugins; schema exports JSON Schemas (§6.5)"
}
func (v *pluginVerb) Usage() string {
	return "eventboat [--json] plugin catalog | eventboat [--json] plugin schema <name> | eventboat plugin schema --all --dir schemas/"
}

func (v *pluginVerb) SetFlags(fs *flag.FlagSet) { v.setJSON(fs) }

func (v *pluginVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdPlugin(rest, jsonOut(env, v.json)), v.Usage())
}

type mcpVerb struct {
	flaggy
	stdio      bool
	httpMode   bool
	configDir  string
	runtime    string
	dataDir    string
	ephemeral  bool
	adminToken string
}

func (v *mcpVerb) Name() string { return "mcp" }
func (v *mcpVerb) Synopsis() string {
	return "the agent operations surface: MCP tools over stdio (agent hosts) or HTTP (with Admin REST + SSE + UI)"
}
func (v *mcpVerb) Usage() string {
	return "eventboat mcp (--stdio | --http) [--config-dir <dir>] [--data-dir DIR]"
}

func (v *mcpVerb) SetFlags(fs *flag.FlagSet) {
	v.setJSON(fs)
	fs.BoolVar(&v.stdio, "stdio", false, "speak MCP over stdin/stdout (for agent hosts to spawn)")
	fs.BoolVar(&v.httpMode, "http", false, "serve MCP over HTTP (Streamable HTTP) together with the admin surface")
	fs.StringVar(&v.configDir, "config-dir", "", "directory of pipeline YAML files to deploy at startup")
	fs.StringVar(&v.runtime, "runtime", "", "Runtime configuration file (default: ./eventboat.yaml)")
	fs.StringVar(&v.dataDir, "data-dir", "", "override storage.data_dir")
	fs.BoolVar(&v.ephemeral, "ephemeral", false, "override storage.ephemeral (in-memory stores)")
	fs.StringVar(&v.adminToken, "admin-token", "", "bearer token for the admin/MCP HTTP surface (required for non-loopback binds; also EVENTBOAT_ADMIN_TOKEN / admin.token)")
}

func (v *mcpVerb) Run(_ context.Context, env *commands.Environment, rest []string) error {
	return exitErr(cmdMCP(rebuildArgs(v.fs, rest), jsonOut(env, v.json)), v.Usage())
}

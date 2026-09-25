---
title: "Configuration & diagnostics"
order: 4
---

# Configuration & diagnostics

A pipeline is one YAML file loaded by `internal/config` and compiled by
`internal/ir`. Loading is deliberately strict: unknown keys, unknown plugins
and un-compilable expressions are errors, not warnings — the config is a
contract with agents as much as with humans. Every finding surfaces as a
`config.Diagnostic` with a stable `code`, severity, file/line, message and
hint (`internal/config/config.go`); `--json` output, the LSP, the MCP verify
tool and the admin UI all render the same structs.

## The load pipeline

`LoadBytes(file, data)` (`internal/config/loader.go`) runs four passes:

1. **Parse + env substitution.** The document parses into a `yaml.Node`;
   every string scalar goes through `${VAR}` / `${?VAR}` substitution
   (unset `${VAR}` is a `cfg_env_unset` error; unset `${?VAR}` omits the
   key; substituted values are *never* re-scanned — single-pass, ruling D1).
2. **Structural decode** into generic maps.
3. **Constants substitution.** `${constants.x}` expands (unknown constant =
   error); `${parameters.x}` passes through untouched only in job pipelines
   (`run.mode: job`) — anywhere else it is a `cfg_scope_unknown` error. The
   optional marker `?` is only legal for plain environment variables.
4. **Structural validation with whitelists**: top-level keys, `metadata.name`
   validation (metadata is a strictly checked mapping: unknown keys and a
   non-mapping value are errors), the `limits`/`run`/`parameters`/`hooks`/`telemetry`/`codecs`/`dlq`
   sections, the three node sections with their framework-field whitelists,
   and finally manifest reads for every external (`grpc:`) node. The
   whitelists, the top-level keys, the edge attributes, the reserved plugin
   names and the node-level default constants all come from the single leaf
   package `internal/framework` (candidate 06) — config, registry and the LSP
   read one vocabulary, so a name like `grpc` or `version` cannot register as
   a plugin that config would parse as a framework field.

`metadata.name` is a conservative identifier (`cfg_name_invalid`): 1–64
characters of `[a-zA-Z0-9._-]`, starting alphanumeric, no `..` (path
traversal), and no Windows reserved device name (CON, PRN, AUX, NUL,
COM1-9, LPT1-9) — the name becomes the deployed file name and the store key.

`Pipeline.Order` is the YAML **document order** of node declarations
(candidate 06), preserved from the node stream: jobs binds `cursor` to the
first source in that order, explain picks its default entry node from it and
the diagnostics iterate it, so a shuffled document still behaves like the one
a human reads.

## The three-section topology

`sources`, `transforms`, `sinks` — each node is `name: {plugin block, ...framework
fields}`. `depends_on` edges join them. Sources and sinks are required; the
transforms section is optional. Exact node-level whitelists
(`internal/framework`, read by `internal/config/sections.go`) —

| Section | Allowed framework fields |
|---|---|
| `sources` | `decoder` (default materialized to `json`), `grpc`, `version` (never `depends_on`) |
| `transforms` | `depends_on`, `workers`, `version` |
| `sinks` | `depends_on`, `encoder` (default `json`), `order_key`, `batch`, `grpc`, `version` |

— everything else at node level must be exactly one plugin key. `workers` is
transform-only: `sinks.workers` was accepted and ignored before and is now
rejected with `cfg_sink_workers` (sink concurrency is engine-owned; implement
or refuse). Edge attributes on a `depends_on` element: `when`, `route`,
`buffer`, `delivery`, `required`. `when` accepts a string (CEL) or
`{lang: cel|cesql, expr: "..."}`; `route` is sugar compiled to
`meta.route == "<name>"` and is mutually exclusive with `when`.
`edge_defaults` accepts only `delivery`, `required` and `buffer` — a global
default predicate is a footgun and a default route is meaningless, so
`when`/`route` there are `cfg_edge_defaults_field` errors (candidate 06).
Pipeline-level defaults (decoder/encoder `json`, delivery `retries: 3` /
`backoff: exponential`, `required: true`, buffer `max_events: 128`, workers 1)
are **materialized into the typed config at load**: downstream layers read the
typed field instead of re-applying a default (the `json` fallback used to be
copied in ten places).

## Runtime configuration

Deployment-level settings live in a separate `kind: Runtime` document
(`internal/runtimecfg/runtimecfg.go`), resolved from `--runtime`, then
`./eventboat.yaml`, then defaults; CLI flags override. The decode is **typed
and strict** (candidate 06): unknown keys AND type errors are errors
(`data_dir: 123`, `enable: "yes"`, `sample_ratio: -1` used to fall through to
the defaults), and `sample_ratio` must be in `[0, 1]` (`0` = tracing off).

| Key | Default | Meaning |
|---|---|---|
| `storage.data_dir` | `data` | SQLite storage directory; each pipeline's store is `<dir>/stores/<sanitized name>.db` (candidate 04) |
| `storage.ephemeral` | `false` | in-memory store, nothing persists |
| `storage.spool_retention` | `10000` | spool rows kept behind the checkpoint |
| `admin.listen` | `127.0.0.1:7788` | admin listener bind address |
| `admin.enable` | `true` | serve the admin surface (config-dir daemon mode) |
| `admin.token` | "" | bearer token (mandatory for non-loopback binds) |
| `mcp.enable` | `true` | register `/mcp` next to the admin surface; `false` means the daemon does not serve MCP at all (the explicit `eventboat mcp --http` command always does) |
| `telemetry.otlp_endpoint` | "" | OTLP/HTTP push endpoint (empty = off) |
| `telemetry.sample_ratio` | `0.1` | trace sampler ratio |
| `telemetry.prometheus` | `true` | serve Prometheus exposition at `/metrics` |

## Verify and IR resolution

Production code builds pipelines only through `internal/verify` (candidate
05): `verify.File`/`verify.Bytes` load, build and judge in one call and
return a `Result` with the built `*ir.Pipeline`, the merged diagnostics and
the strict verdict; `verify.LoadBytes` + `verify.Build` is the two-stage form
for consumers with a middle step (jobs substitutes parameters between the
stages). `ir.Build(cfg, reg, starOpts, parameters)` is the build stage it
wraps. Per node, in order:

1. **Plugin lookup** — the plugin key must resolve in the registry
   (`plugin_unknown`) or, for external nodes, match its manifest
   (`grpc_manifest_name`, `grpc_builtin_conflict` for compiled-in names).
2. **Version pin check** — a node `version:` must equal the registered or
   manifest version (`plugin_version_mismatch`); this is the §6.5 rule that
   makes "documented but absent" impossible.
3. **JSON Schema validation** — the plugin block validates against the
   generated/manifest schema (`plugin_schema`).
4. **Factory instantiation** — the registry factory builds a real instance
   at verify time: the script plugin compiles the Starlark program
   (`expr_starlark_compile`), the wasm plugin compiles the module and checks
   ABI exports (`expr_wasm_compile`), sources/sinks run their typed build
   functions (cross-field rules JSON Schema cannot express live here).

Transform instances declaring `explain-safe` are retained on the IR for
explain dry-runs — but only in the retaining build (`verify.Options.ForExplain`,
used by the CLI/`ops` explain entries and `replay --dry-run`); the default
verify-only build validates every instance and closes it immediately, failure
paths included, so nothing outlives a verify. An explain caller owns the
pipeline and closes the retained instances with `ir.Pipeline.Close` when the
walkthrough is done. Others (wasm — explain never executes guest code) are
always closed immediately. Then: codec resolution
(`codec_unknown`, `codec_config`), order-key compilation, topological sort,
job semantics, telemetry pattern checks and lint.

`explain` renders the **resolved** semantics (candidate 09), never a
per-plugin re-derivation: the sample message is decoded with the entry
source's declared decoder (the same codec instance the engine resolves — a
`raw`/csv source is not fed JSON), each edge predicate is evaluated once per
walk, every inbound edge of a sink is rendered with its own delivery policy
plus the engine's mixed-batch rule (max retries, max explicit timeout; the
default timeout applies only when no edge in the batch sets one), and a
`required: false` edge shows its terminal drop — the engine drops and commits
the message instead of dead-lettering it. A wasm symbolic line shows the
resolved per-invoke mode (`fast mode (no per-invoke kill switch)` when
`timeout_ms` is unset).

## The diagnostics table

Every diagnostic code that exists in the code, by emitting layer. Severity
`error` fails verify; `warning` is lint (upgraded to error by
`verify --strict`). Example messages are abbreviated.

### Loader: file, syntax, document shape

| Code | Sev | Fires when | Example |
|---|---|---|---|
| `io_read` | error | the config file cannot be read | `open p.yaml: The system cannot find the file specified.` |
| `yaml_parse` | error | YAML syntax error, top level not a mapping, or decode failure | `top level must be a mapping` |
| `empty_config` | error | the document is empty | `configuration is empty` |
| `cfg_unknown_top_section` | error | unknown top-level key | `unknown top-level key "codecs2"` |
| `cfg_api_version` | error | `apiVersion` is not `eventboat/v1` | `apiVersion must be "eventboat/v1"` |
| `cfg_kind` | error | `kind` is not `Pipeline` | `kind must be "Pipeline"` |
| `cfg_metadata_name` | error | `metadata.name` missing/blank | `metadata.name is required` |
| `cfg_metadata_type` | error | `metadata` is not a mapping | `metadata must be a mapping` |
| `cfg_name_invalid` | error | name violates charset/`..`/reserved-name rules | `metadata.name "con" must be 1-64 characters of [a-zA-Z0-9._-]...` |
| `cfg_env_unset` | error | `${VAR}` references an unset variable | `environment variable TOKEN is not set` |
| `cfg_constant_unknown` | error | `${constants.x}` names an undeclared constant | `unknown constant "vip"` |
| `cfg_scope_unknown` | error | unknown scope, `${parameters.*}` outside a job pipeline, or `?` on a scoped reference | `unknown scoped reference ${parameters.x}: parameters are only available in job pipelines` |

### Loader: sections

| Code | Sev | Fires when |
|---|---|---|
| `cfg_limits_type` | error | `limits` not a mapping; `drain_timeout` not a duration string |
| `cfg_limits_range` | error | `max_in_flight < 1`; `drain_timeout` not positive |
| `cfg_run_type` | error | `run` not a mapping |
| `cfg_run_mode` | error | `run.mode` not `continuous`/`job`/`batch` |
| `cfg_run_schedule` | error | empty schedule, or schedule without `mode: job` |
| `cfg_run_overlap` | error | `run.overlap` not `skip|all|latest` |
| `cfg_run_catchup` | error | `catchup_window` not a valid duration |
| `cfg_run_skip` | error | `skip_if_successful` not a boolean |
| `cfg_run_retention` | error | `retention` shape/type/range errors |
| `cfg_dlq_type` | error | `dlq` not a mapping |
| `cfg_dlq_retention` | error | `dlq.retention` missing/not a duration/not positive |
| `cfg_telemetry_type` | error | `telemetry` not a mapping |
| `cfg_telemetry_redact` | error | `redact` not a list of non-empty strings |
| `cfg_telemetry_span_rate` | error | `span_sample_rate` not a number in [0,1] |
| `cfg_codecs_type` | error | `codecs` or a declaration not a mapping |
| `cfg_codec_type` | error | codec declaration missing `type` |
| `cfg_parameters_type` | error | `parameters`/a declaration not a mapping |
| `cfg_parameters_not_job` | error | `parameters:` present without `run.mode: job` |
| `cfg_parameters_decl` | error | any parameter-declaration rule: type mismatch, bad enum/pattern/min/max, default violating constraints, `required` + `default` |
| `cfg_hooks_type` | error | `hooks` not a mapping |
| `cfg_hooks_sink` | error | unknown hook name, or hook not exactly one inline sink block |
| `cfg_unknown_field` | error | unknown field inside `metadata`, `limits`, `run`, `retention`, `dlq`, `parameters`, `grpc`, `batch`, edge attribute blocks, `when` objects, `buffer`, `delivery` |

### Loader/sections: nodes and edges

| Code | Sev | Fires when |
|---|---|---|
| `cfg_missing_section` | error | `sources:` or `sinks:` absent |
| `cfg_section_type` | error | a section is not a mapping |
| `cfg_empty_section` | error | a section present but empty |
| `cfg_node_type` | error | a node is not a mapping |
| `cfg_source_with_depends_on` | error | a source declares `depends_on` (sources have no in-edges) |
| `cfg_sink_workers` | error | a sink declares `workers` (not implemented; sink concurrency is engine-owned) |
| `cfg_from_renamed` | error | node still uses the old `from` key, renamed to `depends_on` (migration diagnostic) |
| `cfg_missing_plugin` | error | node has no plugin block |
| `cfg_multiple_plugins` | error | node has more than one plugin block |
| `cfg_plugin_block_type` | error | source/sink plugin block is not a mapping |
| `cfg_missing_depends_on` | error | transform/sink without `depends_on` |
| `cfg_bad_depends_on` | error | `depends_on` not a name/list/single-key mapping; empty name; multi-key element; non-mapping attrs |
| `cfg_when_type` | error | `when` empty, not a string/object, or `expr` empty |
| `cfg_when_lang` | error | `when.lang` not `cel`/`cesql` |
| `cfg_when_route_exclusive` | error | `when` and `route` on one edge |
| `cfg_edge_defaults_field` | error | `edge_defaults` is not a mapping, or declares `when`/`route` (only `delivery`/`required`/`buffer` are allowed) |
| `cfg_route_type` | error | `route` not a non-empty name |
| `cfg_required_type` | error | `required` not a boolean |
| `cfg_buffer_type` | error | `buffer` not a mapping; `type` not `memory` |
| `cfg_buffer_range` | error | `buffer.max_events < 1` |
| `cfg_delivery_type` | error | `delivery` not a mapping |
| `cfg_delivery_range` | error | `retries < 0`; `timeout_ms < 1` |
| `cfg_delivery_backoff` | error | `backoff` not `exponential`/`constant` |
| `cfg_decoder_type` | error | source `decoder` not a codec name string |
| `cfg_encoder_type` | error | sink `encoder` not a codec name string |
| `cfg_order_key_type` | error | sink `order_key` not a CEL string |
| `cfg_batch_type` | error | `batch` not a mapping; unknown batch fields |
| `cfg_batch_range` | error | `batch.size < 1`; `timeout_ms < 1` |
| `cfg_workers_range` | error | `workers < 1` |
| `cfg_version_range` | error | node `version: < 1` |
| `cfg_grpc_type` | error | `grpc` not a mapping |
| `cfg_grpc_command` | error | `grpc.command` not a non-empty string argv |
| `cfg_grpc_env` | error | `grpc.env` value not a string |
| `cfg_grpc_schema` | error | `grpc.schema` not a manifest path |
| `cfg_grpc_restart` | error | `grpc.restart` not `fast-fail`/`restart` |
| `topo_dup_name` | error | duplicate node name within a section (loader) or across all three sections (ir) |
| `grpc_manifest_read` | error | manifest file unreadable, or path references job parameters |
| `grpc_manifest_parse` | error | manifest is not valid JSON |
| `grpc_manifest_kind` | error | manifest kind mismatches the node's section |
| `grpc_manifest_version` | error | manifest version < 1 |
| `grpc_manifest_schema` | error | manifest has no `config_schema` (loader) or schema marshal fails (ir) |

### IR: topology, plugins, expressions, jobs, lint

| Code | Sev | Fires when |
|---|---|---|
| `topo_missing_ref` | error | `depends_on` references an unknown node |
| `topo_sink_as_upstream` | error | `depends_on` references a sink (sinks have no out-edges) |
| `topo_cycle` | error | the DAG has a cycle |
| `topo_no_path` | error | no source-to-sink path exists |
| `topo_orphan` | error | source with no downstream; non-source with no in-edges |
| `plugin_unknown` | error | plugin key not in the registry (all kinds; transform hint notes gRPC transforms are future work) |
| `plugin_version_mismatch` | error | node `version:` ≠ registered/manifest version |
| `plugin_schema` | error | plugin block fails its JSON Schema (also hook sink validation) |
| `grpc_builtin_conflict` | error | `grpc:` block on a compiled-in plugin name |
| `grpc_manifest_name` | error | manifest name ≠ plugin block key |
| `codec_unknown` | error | decoder/encoder names an unknown codec |
| `codec_config` | error | a codec needing configuration is referenced bare instead of via `codecs:` |
| `cfg_codec_shadow` | error | `codecs:` declaration name shadows a registered codec |
| `expr_cel_env` | error | the CEL environment cannot be built |
| `expr_cel_compile` | error | a CEL predicate or order key fails to compile |
| `expr_route_compile` | error | a `route:`-sugared edge's compiled CEL fails |
| `expr_route_dangling` | error | `route:` edge whose upstream transform never assigns `meta.route` |
| `expr_cesql_compile` | error | a CESQL `when` fails to parse |
| `expr_starlark_compile` | error | script plugin: Starlark resolve failed (via `TransformError.DiagCode`, with backtrace context) |
| `expr_wasm_compile` | error | wasm plugin: module unreadable, not a reactor, or missing ABI exports (via `TransformError.DiagCode`) |
| `wasm_no_kill_switch` | warning | wasm transform without `timeout_ms`: no kill switch (escalated by `--strict`) |
| `run_retention_unset` | warning | job pipeline without `run.retention.history`: run history keeps forever (escalated by `--strict`) |
| `batch_no_finite_source` | warning | `run.mode: batch` but no source declares finite exhaustion: the run hangs until cancelled (escalated by `--strict`) |
| `job_file_source_no_eof` | warning | job pipeline file source without `on_eof: stop`: the run never completes (escalated by `--strict`) |
| `job_bad_schedule` | error | `run.schedule` is not a valid 5-field cron |
| `job_source_not_pull` | error | job pipeline source lacks the `pull` capability |
| `job_multiple_pull_sources` | warning | job pipeline with >1 pull source (`cursor` binds the first) |
| `job_parameters_in_continuous` | error | `parameters.*` referenced in a continuous pipeline |
| `job_parameter_unknown` | error | `${parameters.x}` names an undeclared parameter (verify; also the runtime backstop in `SubstituteParameters`) |
| `telemetry_redact_pattern` | error | a `telemetry.redact` pattern is not a valid field-path glob |
| `lint_when_literal` | warning | edge condition is literally `true`/`false` |
| `lint_constant_unused` | warning | a declared constant is never referenced |
| `lint_sql_continuous` | warning | a pull source (sql / pull-capable plugin) used in a continuous pipeline |
| `lint_line_bytes_over_vl` | warning | file source with `max_line_bytes > 262144` in a pipeline with a `victorialogs` sink: VL's `-insert.maxLineSizeBytes` default would skip longer lines server-side |
| `lint_multiline_no_timeout` | warning | file source with multiline `timeout_ms` explicitly `0`: no time-based flush, so an isolated trailing group waits for the next group-starting line |
| `lint_collector_batch_one` | warning | file source plus a non-`drop`/`debug` sink whose effective `batch.size` is 1 (unset, or explicit `1`): the collection shape should batch |

### Retired codes

Spec v1.19 retired the hand-parsed transform diagnostics —
`cfg_transform_main_field`, `cfg_script_type`, `cfg_split_type`,
`cfg_wasm_type`, `cfg_wasm_module`, `cfg_wasm_range`, `cfg_wasm_allow` —
their checks now surface as `cfg_missing_plugin`/`cfg_multiple_plugins` or
`plugin_schema` issues like every other kind. `expr_starlark_compile`,
`expr_wasm_compile` and `wasm_no_kill_switch` survive via error
classification (see [Plugins](03-plugins.md)).

## Where diagnostics are consumed

- `verify` / `run` / `test` CLI verbs print `severity[code] file:line:
  message` plus hints; `--json` emits the structs.
- The LSP converts the same diagnostics into editor squiggles
  (`internal/lsp/diagnostics.go`).
- The MCP `verify` tool returns them with an `ok` verdict so agents iterate;
  `deploy` fails the tool call when verification fails — verify-first has no
  bypass.

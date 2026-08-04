# Agent Skill & MCP Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement task-by-task.

**Goal:** Make edgestream operable by external AI agents — Cursor Skill (done), machine-readable CLI, optional MCP server — **before** in-pipeline LLM transforms.

**Architecture:** Skill documents CLI + Admin API workflow; new CLI flags emit JSON; MCP wraps the same operations as thin tools.

**Tech Stack:** Go 1.22+, existing `cmd/edgestream`, `mcp-go` or stdlib JSON-RPC over stdio (evaluate at Task 4).

---

## Phase 0: Cursor Skill — **Completed**

| Deliverable | Path |
|-------------|------|
| Project Skill | `skills/edgestream/SKILL.md` |
| Extended reference | `skills/edgestream/reference.md` |
| Skills catalog | `skills/README.md` |
| Design doc | `docs/ai-agent.md` § Phase 0 |

### Manual acceptance

- [ ] Open Cursor in edgestream repo; ask Agent to create a cron→map→drop pipeline
- [ ] Agent reads Skill, runs `edgestream validate`, fixes errors without reading Go source
- [ ] Agent runs `edgestream test` or uses `_examples/01-linear-etl.yaml` as reference

---

## Phase 1a: Machine-readable CLI

**Goal:** Agents parse validate/plugins output without regex on human text.

### Task 1: `edgestream plugins list`

**Files:** `cmd/edgestream/main.go`, `internal/registry/registry.go` (read-only list API)

- [ ] **1.1** Add `ListSources()`, `ListTransforms()`, `ListSinks()`, `ListCodecs()` on Registry
- [ ] **1.2** Add subcommand `edgestream plugins list [--format text|json]`
- [ ] **1.3** JSON shape: `{ "sources": ["kafka",...], "transforms": [...], "sinks": [...], "codecs": [...] }`
- [ ] **1.4** Test + update Skill reference

### Task 2: `edgestream validate --format json`

**Files:** `cmd/edgestream/main.go`, `internal/topology/` (wrap errors)

- [ ] **2.1** On success: `{ "ok": true, "pipelines": [{ "name", "stages", "edges", "warnings": [] }] }`
- [ ] **2.2** On failure: `{ "ok": false, "errors": [{ "file", "message" }] }`, exit 1
- [ ] **2.3** Default `--format text` unchanged
- [ ] **2.4** Test + update Skill

### Task 3: `edgestream doc` (optional, stretch)

**Files:** `cmd/edgestream/doc.go`, `internal/topology/`

- [ ] **3.1** `edgestream doc --config FILE --format dot` → Graphviz DOT of stages/edges
- [ ] **3.2** Document in Skill

---

## Phase 1b: MCP Server

**Goal:** Cursor / Claude Desktop connect via MCP instead of shell-only.

### Task 4: Spike MCP transport

- [ ] **4.1** Choose library (e.g. `github.com/mark3labs/mcp-go`) or minimal stdio JSON-RPC
- [ ] **4.2** `edgestream mcp` subcommand starts server on stdio
- [ ] **4.3** Document `.cursor/mcp.json` snippet in `docs/ai-agent.md`

### Task 5: MCP tools (wrap CLI logic, no duplicate business logic)

| Tool | Input | Output |
|------|-------|--------|
| `validate` | `path`, optional `format` | same as CLI JSON |
| `test` | `config` or `dir` | pass/fail + case results |
| `plugins_list` | — | plugin catalog JSON |
| `pipeline_status` | `base_url`, `name` | admin API response |
| `reload_pipeline` | `base_url`, `name` | task_id |

- [ ] **5.1** Extract shared functions from `validateCmd` / `testCmd` into `internal/agentapi/` or `internal/cliout/`
- [ ] **5.2** Register MCP tools calling shared functions
- [ ] **5.3** Integration test with mock HTTP for admin tools

---

## Global Constraints

- Phase 1 must not require a running engine except admin tools
- MCP and CLI share validation code paths — no drift
- Skill stays under ~500 lines; push detail to `reference.md`
- Do not implement `llm` transform in this plan (see `2026-07-01-ai-agent-foundation.md`)

---

## Verification

```bash
go test ./...
bin/edgestream plugins list --format json
bin/edgestream validate --config _examples/01-linear-etl.yaml --format json
# MCP manual: configure mcp.json, invoke validate tool from client
```

---

## Out of Scope

- In-pipeline LLM/agent transforms (Phase 2)
- Auth on Admin API (document as localhost-only for now)
- Remote edgestream cloud service

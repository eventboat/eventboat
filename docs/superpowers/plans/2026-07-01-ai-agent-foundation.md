# AI/Agent Phase 2: In-Pipeline LLM Foundation Implementation Plan

> **Prerequisite:** Phase 0 Agent Skill complete — see [2026-07-01-agent-skill.md](2026-07-01-agent-skill.md).

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver `llm` and `embed` transforms with OpenAI-compatible and Ollama providers — **after** external agents can author and validate pipelines via Skill/CLI/MCP.

**Architecture:** Provider interface in `internal/llm/`; transforms register via existing `registry.RegisterTransform`; eql templates compile at init; metrics hook into `internal/observability/metrics.go`.

**Tech Stack:** Go 1.22+, cel-go (existing), `net/http` for providers (no heavy SDK v1), testify for tests.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `internal/llm/provider.go` | `Provider`, `CompletionRequest`, `CompletionResponse` interfaces |
| `internal/llm/openai/openai.go` | OpenAI-compatible HTTP client |
| `internal/llm/ollama/ollama.go` | Ollama chat + embed API |
| `internal/llm/registry.go` | Provider factory registry |
| `internal/llm/mock/mock.go` | Test mock provider |
| `internal/eql/template.go` | `${path}` template expansion for strings |
| `plugins/transform/llm/llm.go` | `llm` transform |
| `plugins/transform/embed/embed.go` | `embed` transform |
| `plugins/all/all.go` | Blank import new transforms |
| `internal/observability/metrics.go` | Add `edgestream_llm_*` counters/histograms |
| `_examples/09-llm-classify.yaml` | End-to-end example |
| `testdata/tests/llm_classify.yaml` | Fixture test for `edgestream test` |
| `docs/configurations.md` | User config reference section |

---

## Task 1: Provider interface + mock

**Files:** Create `internal/llm/provider.go`, `internal/llm/mock/mock.go`, `internal/llm/registry.go`

- [ ] **1.1** Write test `TestMockProviderComplete` in `internal/llm/mock/mock_test.go`
- [ ] **1.2** Run test — expect FAIL (package doesn't exist)
- [ ] **1.3** Implement `Provider` interface, `TokenUsage`, `Message`, `ToolDef` types
- [ ] **1.4** Implement mock that returns fixed JSON content + usage
- [ ] **1.5** Implement `RegisterProvider(name, factory)` + `NewProvider(name, cfg)`
- [ ] **1.6** Run `go test ./internal/llm/...` — PASS

---

## Task 2: OpenAI-compatible adapter

**Files:** Create `internal/llm/openai/openai.go`, `internal/llm/openai/openai_test.go`

- [ ] **2.1** Write test with `httptest.Server` returning chat completion JSON
- [ ] **2.2** Run test — FAIL
- [ ] **2.3** Implement POST `/v1/chat/completions` client (api_key, base_url, timeout)
- [ ] **2.4** Map `response_format: json` → `response_format: { type: json_object }`
- [ ] **2.5** Classify HTTP errors: 429→retryable, 401/400→deterministic
- [ ] **2.6** Run tests — PASS

---

## Task 3: eql template expansion

**Files:** Create `internal/eql/template.go`, `internal/eql/template_test.go`

- [ ] **3.1** Write tests: `${payload.text}`, missing key → empty string, `${metadata.id}`
- [ ] **3.2** Run — FAIL
- [ ] **3.3** Implement `ExpandTemplate(s string, evalCtx *EvalContext) (string, error)` using existing `MsgAdapter`
- [ ] **3.4** Run `go test ./internal/eql/...` — PASS

---

## Task 4: `llm` transform

**Files:** Create `plugins/transform/llm/llm.go`, `plugins/transform/llm/llm_test.go`

- [ ] **4.1** Write test: mock provider, input message `{ "text": "hello" }`, expect `payload.category` set
- [ ] **4.2** Run — FAIL
- [ ] **4.3** Register `llm` transform; parse config (provider, model, messages, result, timeout)
- [ ] **4.4** At Init: compile message templates; validate provider exists
- [ ] **4.5** Process: expand templates → provider.Complete → write result field → set `er-llm-*` metadata
- [ ] **4.6** Map provider errors to transform errors (respect upstream error_mode)
- [ ] **4.7** Add blank import in `plugins/all/all.go`
- [ ] **4.8** Run tests — PASS

---

## Task 5: LLM metrics

**Files:** Modify `internal/observability/metrics.go`; wire from `plugins/transform/llm/llm.go`

- [ ] **5.1** Add counters/histograms: `edgestream_llm_requests_total`, `edgestream_llm_latency_seconds`, `edgestream_llm_tokens_total`, `edgestream_llm_errors_total`
- [ ] **5.2** Call from llm transform on each Complete
- [ ] **5.3** Run `go test ./...` — PASS

---

## Task 6: Ollama adapter + `embed` transform

**Files:** `internal/llm/ollama/ollama.go`, `plugins/transform/embed/embed.go`

- [ ] **6.1** Ollama chat test with httptest (`/api/chat`)
- [ ] **6.2** Ollama embed test with httptest (`/api/embeddings`)
- [ ] **6.3** Implement adapters
- [ ] **6.4** `embed` transform: input template → Embed → write `result.field`
- [ ] **6.5** Register in `plugins/all/all.go`
- [ ] **6.6** Run tests — PASS

---

## Task 7: Examples + fixture test + docs

**Files:** `_examples/09-llm-classify.yaml`, `testdata/tests/llm_classify.yaml`, `docs/configurations.md`

- [ ] **7.1** Add classify example (comments explain mock/local ollama switch)
- [ ] **7.2** Add fixture test using in-process test plugins or mock provider env
- [ ] **7.3** Add `### Transform: llm` and `### Transform: embed` sections to configurations.md
- [ ] **7.4** Run `edgestream validate --config _examples/09-llm-classify.yaml`
- [ ] **7.5** Run `go test ./...` — PASS

---

## Global Constraints

- Do not add LLM awareness to `internal/engine/` — all logic stays in transform plugins
- Provider HTTP clients must respect `context.Context` cancellation (engine drain)
- No secrets in logs; redact `api_key` in error messages
- v1 scope: no streaming, no tool calling (Phase B)

---

## Verification Checklist

```bash
go test ./...
go build -o bin/edgestream ./cmd/edgestream
bin/edgestream validate --config _examples/09-llm-classify.yaml
bin/edgestream test --dir testdata/tests
```

Optional integration (requires local Ollama):

```bash
OLLAMA_HOST=http://localhost:11434 go test ./internal/llm/ollama/... -tags=integration
```

---

## Out of Scope (Phase B)

- `agent` transform + tool loop
- Bedrock / Vertex adapters
- Session store
- Vector sinks

See [docs/ai-agent.md](../../ai-agent.md) Phase B.

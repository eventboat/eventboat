# AI / Agent 支持

> 版本：v1.1-draft  
> 日期：2026-07-01  
> 状态：Phase 0 进行中（Agent 调用 riverpod）

English summary in [README](../README.md#agent-first-ai-integration).

---

## 1. 定位与目标

AI/Agent 支持分 **两个方向**，优先级明确：

| 方向 | 含义 | 优先级 |
|------|------|--------|
| **Agent → riverpod** | 外部 AI Agent（Cursor、Claude、自定义编排）**调用** riverpod：写配置、校验、测试、运行、热加载 | **Phase 0（第一步）** |
| **riverpod → LLM** | Pipeline 内部的 `llm` / `embed` / `agent` Transform，在 DAG 里调用大模型 | Phase 2（v2.1+） |

**第一步的核心：** 让 Agent 能可靠地操作 riverpod，而不是先在引擎里嵌 LLM。

典型 Agent 工作流：

```
用户意图 → Agent 读 Skill → 选 _examples 模板 → 写 YAML
         → riverpod validate → riverpod test → riverpod run / admin reload
```

已交付 / 进行中的 Agent 入口：

```bash
npx skills add riverpod/riverpod@riverpod
```

Browse: https://skills.sh/riverpod/riverpod/riverpod

| 入口 | 路径 | 状态 |
|------|------|------|
| **Agent Skill (skills.sh)** | [`skills/riverpod/SKILL.md`](../skills/riverpod/SKILL.md) | ✅ Phase 0 |
| **CLI** | `riverpod validate` / `test` / `run` | ✅ 已有 |
| **Admin HTTP API** | `/admin/reload`、`/admin/pipelines` | ✅ 已有 |
| **MCP Server** | `riverpod mcp` 或独立 `riverpod-mcp` | Phase 1b |
| **JSON 输出** | `validate --format json` 等 | Phase 1b |

### 1.1 为什么 Agent 调用是第一步

| 原因 | 说明 |
|------|------|
| **立刻可用** | 不改动引擎核心；Skill + 文档即可让 Cursor Agent 写 pipeline |
| **复利最大** | Agent 帮用户配 pipeline → 降低 riverpod 使用门槛 → 生态飞轮 |
| **与 LLM Transform 正交** | 外部 Agent 写配置；内部 `llm` transform 是运行时能力，可后做 |
| **对标真实需求** | 开发者用 AI 助手搭数据管道，比「管道里调 GPT」更常见 |

### 1.2 设计原则（Agent 侧）

1. **CLI 优先** — Agent 通过可脚本化命令操作；HTTP Admin 仅在引擎已运行时
2. **Skill 即契约** — `skills/riverpod/` 发布到 skills.sh，固化术语、工作流、插件清单
3. **校验先于运行** — Skill 强制 `validate` → `test` → `run` 顺序
4. **机器可读输出** — Phase 1b 起 CLI/MCP 返回 JSON，便于 Agent 解析
5. **文档分层** — Skill（操作手册）→ configurations.md（字段参考）→ riverpod-design.md（语义）

### 1.3 设计原则（Pipeline 内 LLM，Phase 2+）

1. **引擎无 LLM 感知** — LLM 调用封装在 Transform 插件内
2. **Provider 可插拔** — OpenAI 兼容 API 为默认抽象
3. **声明式配置** — prompt 用 eql 模板；错误走现有 retry/DLQ 链

---

## 2. 架构概览

### 2.1 Phase 0：Agent 调用 riverpod（当前）

```
┌─────────────────────────────────────────────────────────┐
│  Cursor / Claude / Custom Agent                          │
│  reads skills/riverpod/SKILL.md                            │
└──────────────────────────┬──────────────────────────────┘
                           │ shell / MCP (Phase 1b)
                           ▼
┌─────────────────────────────────────────────────────────┐
│  riverpod CLI                    Admin HTTP (运行中)        │
│  validate · test · run         reload · pipelines · status│
└──────────────────────────┬──────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│  Config → TopologyIR → Engine → Plugins                  │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Phase 2+：riverpod 内嵌 LLM（后续）

```
Source → map/route → llm transform → embed → Sink
                         │
                   Provider Layer (OpenAI/Ollama/…)
```

Pipeline 内 LLM 的组件与配置详见 [§3](#3-pipeline-内-llm-组件phase-2)（原 Phase A/B/C，顺序后移）。

---

## 3. Pipeline 内 LLM 组件（Phase 2+）

> 以下内容为 v2.1+ 规划；**不影响 Phase 0 交付**。Agent 通过 Skill 写 pipeline 时，暂不要使用未实现的 `llm`/`agent` transform type。

### 3.1 Transform：`llm`（P0，v2.1）

单轮 LLM 调用，对标 Redpanda Connect `openai_chat_completion`。

```yaml
steps:
  classify:
    depends_on: [kafka-in]
    transform:
      type: llm
      workers: 4                    # 并发受 engine.max_inflight 约束
      config:
        provider: openai            # openai | ollama | bedrock | vertex | azure
        model: gpt-4o-mini
        api_key: ${OPENAI_API_KEY}
        base_url: ${OPENAI_BASE_URL}   # 可选，兼容 OpenAI API 的网关
        timeout: 30s
        max_retries: 2               # Transform 内瞬时重试；确定性错误不重试
        response_format: json        # text | json | json_schema
        json_schema:                 # response_format=json_schema 时
          name: classification
          schema: { type: object, properties: { category: { type: string } } }
        messages:
          - role: system
            content: |
              Classify the user message into: billing, support, sales.
          - role: user
            content: "${payload.text}"   # eql 模板，编译期校验变量引用
        result:
          field: payload.classification  # 写入 payload 路径
          # 或 metadata_key: er-llm-response
        usage:
          record_tokens: true            # 写入 metadata er-llm-* 
```

**行为语义：**

- `messages[].content` 支持 eql 模板（`${payload.x}`、`${metadata.y}`）
- 成功：按 `result` 配置写入 payload/metadata；可选保留 raw response 到 `metadata['er-llm-raw']`
- 失败：429/5xx → 瞬时错误，走边 retry；400/401 → 确定性错误，直接 DLQ
- 超时：context deadline，计 `riverpod_llm_timeout_total`

### 3.2 Transform：`embed`（P1，v2.1）

```yaml
  vectorize:
    depends_on: [chunk]
    transform:
      type: embed
      config:
        provider: ollama
        model: nomic-embed-text
        input: "${payload.chunk_text}"
        result:
          field: payload.embedding
          dimensions: 768
```

### 3.3 Transform：`agent`（P0，v2.2）

多轮 Agent，支持 tool calling 与可选会话持久化。

```yaml
  support-agent:
    depends_on: [classify]
    transform:
      type: agent
      workers: 2
      config:
        provider: openai
        model: gpt-4o
        max_turns: 10
        session:
          key: "${metadata.session_id}"     # 同 key 共享会话
          store: memory                     # memory | redis（v2.2+）
          ttl: 1h
        system: |
          You are a support agent. Use tools when needed.
        tools:
          - name: lookup_order
            description: Look up order by ID
            parameters:
              type: object
              properties:
                order_id: { type: string }
            invoke:
              type: http                     # http | pipeline | mcp
              method: GET
              url: "https://api.example.com/orders/${arguments.order_id}"
              timeout: 5s
          - name: escalate
            invoke:
              type: pipeline                 #  fan-out 到子 pipeline（v2.3）
              pipeline: escalation-handler
        result:
          field: payload.agent_reply
```

**Agent 循环（单 Message 生命周期内）：**

```
1. 组装 messages（system + session history + 当前 user content）
2. 调用 Provider.Complete（含 tools schema）
3. 若 response 含 tool_calls → 执行 tool → 追加 tool result → 回到 2
4. 达到 max_turns 或 final text → 写入 result → 返回
5. 更新 session store
```

**与 DAG 的关系：** 复杂编排优先用 **多 step DAG**（llm classify → route → 专家 llm）；`agent` transform 适合 **单 Message 内闭环**（tool loop）。

### 3.4 eql 扩展（v2.1）

| 函数 | 说明 |
|------|------|
| `template(s, vars)` | 安全字符串模板（内部用于 messages） |
| `truncate(s, n)` | 截断至 token 预算 |
| `token_estimate(s)` | 粗算 token 数（用于前置校验） |

LLM 相关 **不** 在 eql 内直接发起网络调用——保持 eql 纯函数语义。

### 3.5 Sink 扩展（P2，v2.2+）

| Sink | 用途 |
|------|------|
| `pinecone` / `qdrant` / `pgvector` | RAG 向量写入 |
| `langfuse` / `langsmith` | LLM trace 导出（与 OTLP 互补） |

### 3.6 StepType：`task`（v2.3，与 Envelope 对齐）

预留 `steps.{name}.step_type: task`，表示 **异步长任务**（Agent 跑完再 emit 下游）。与 `agent` transform 的区别：

| | `agent` transform | `task` step |
|--|-------------------|-------------|
| 粒度 | 单 Message 同步/半同步 | 可跨 Message、可 checkpoint |
| 超时 | transform config | step 级 `timeout` + 恢复 |
| 状态 | session store | 引擎级 task ledger |

`task` 在 v2.3 与 Decision step 一并评估；v2.1/v2.2 不阻塞。

---

## 4. Provider 抽象

```go
// internal/llm/provider.go（规划接口，v2.1 实现）

type Provider interface {
    Name() string
    Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error)
    Embed(ctx context.Context, req *EmbedRequest) (*EmbedResponse, error)
    // Stream  v2.2+：流式写入 metadata，供 websocket sink 消费
}

type CompletionRequest struct {
    Model       string
    Messages    []Message
    Tools       []ToolDef
    Format      ResponseFormat
    Temperature *float64
    MaxTokens   *int
}

type CompletionResponse struct {
    Content    string
    ToolCalls  []ToolCall
    Usage      TokenUsage
    FinishReason string
}
```

**Adapter 优先级：**

| 阶段 | Provider | 说明 |
|------|----------|------|
| v2.1 | `openai` | 官方 + 任意 OpenAI 兼容网关（Azure/OpenRouter/本地 vLLM） |
| v2.1 | `ollama` | 本地开发 / 边缘部署 |
| v2.2 | `bedrock` | AWS 生产 |
| v2.2 | `vertex` | GCP Gemini |
| v2.3 | `anthropic` | 原生 Messages API |

Provider 注册与 Source/Sink 相同：`registry.RegisterLLMProvider("openai", factory)`。

---

## 5. 配置与 Message 约定

### 5.1 Metadata 前缀 `er-llm-*` / `er-agent-*`

| 字段 | 说明 |
|------|------|
| `er-llm-model` | 实际使用的 model |
| `er-llm-provider` | provider 名 |
| `er-llm-prompt-tokens` | input tokens |
| `er-llm-completion-tokens` | output tokens |
| `er-llm-latency-ms` | 调用耗时 |
| `er-llm-request-id` | 厂商 request id（tracing 关联） |
| `er-agent-turns` | Agent 循环轮数 |
| `er-agent-session-id` | 会话 id |
| `er-agent-tool-calls` | 本轮 tool 调用次数 |

### 5.2 错误类型（扩展 `er-error-type`）

| 值 | 说明 |
|----|------|
| `llm_rate_limit` | 429，可 retry |
| `llm_timeout` | 超时 |
| `llm_auth_error` | 401/403，确定性 |
| `llm_invalid_request` | 400，确定性 |
| `llm_content_filter` | 内容策略拒绝 |
| `agent_max_turns` | 超过 max_turns |
| `agent_tool_error` | tool 执行失败 |

### 5.3 背压与并发

- LLM transform 的 `workers` 受 `engine.max_workers` 约束
- 每条 Message 占用 `max_inflight` 槽位直至 LLM 返回（长调用会自然背压 Source）
- 可选 `config.concurrency_limit` per-stage 限制同时进行中的 LLM 请求数

---

## 6. 可观测性

### 6.1 指标（`riverpod_*` 扩展）

| 指标 | 类型 | 标签 |
|------|------|------|
| `riverpod_llm_requests_total` | Counter | pipeline, stage_id, provider, model, status |
| `riverpod_llm_latency_seconds` | Histogram | pipeline, stage_id, provider, model |
| `riverpod_llm_tokens_total` | Counter | pipeline, stage_id, provider, model, direction=prompt\|completion |
| `riverpod_llm_errors_total` | Counter | pipeline, stage_id, error_type |
| `riverpod_agent_turns_total` | Counter | pipeline, stage_id |
| `riverpod_agent_tool_calls_total` | Counter | pipeline, stage_id, tool_name, status |

### 6.2 Tracing

```
Transform Span (riverpod.transform.classify)
└── LLM Span (riverpod.llm.complete)
    ├── attributes: provider, model, tokens, finish_reason
    └── Child Span (riverpod.agent.tool) × N
```

---

## 7. 典型 Pipeline 模式

### 7.1 事件分类 + 路由

```yaml
steps:
  kafka-in:
    source: { type: kafka, decoder: json, config: { topics: [support-tickets] } }

  llm-classify:
    depends_on: [kafka-in]
    transform:
      type: llm
      config:
        provider: openai
        model: gpt-4o-mini
        response_format: json
        messages:
          - role: user
            content: "Classify: ${payload.body}"
        result: { field: payload.category }

  splitter:
    depends_on: [llm-classify]
    transform:
      type: route
      config:
        routes:
          billing: "payload.category == 'billing'"
          support: "payload.category == 'support'"
          _default: "true"

  billing-sink:
    depends_on: { splitter: { route: billing } }
    sink: { type: kafka, config: { topic: billing-queue } }
```

### 7.2 RAG 摄取

```
kafka-in → map (extract text) → split (chunk) → embed → sink (pgvector)
```

示例见 `_examples/09-rag-ingest.yaml`（Phase 1 完成后添加）。

### 7.3 多 Agent DAG

```
                    ┌─► llm (billing expert) ──► sink
kafka → llm (router)─┼─► llm (support expert) ──► sink
                    └─► llm (sales expert)   ──► sink
```

用 `route` 分流，每个 branch 独立 `llm` step——**无需** monolithic super-agent。

---

## 8. 开发路线图

### Phase 0：Agent 调用 riverpod（**当前，第一步**）

| 任务 | 交付物 | 状态 |
|------|--------|------|
| 0.1 项目 Agent Skill（skills.sh） | `skills/riverpod/SKILL.md` + `reference.md` + `skills/README.md` | ✅ |
| 0.2 文档对齐 | 本文件、README、riverpod-design §12 | ✅ |
| 0.3 `_examples` 索引在 Skill 中可发现 | Skill reference 链接 | ✅ |
| 0.4 Agent 工作流验收 | 用 Cursor Agent 完成：validate 示例 → 新建 cron pipeline → test | 待验收 |

**验收标准：** 未读源码的 Agent 仅凭 Skill 能完成「从模板创建 pipeline → validate 通过 → test 通过」。

### Phase 1a：Agent 体验增强（v2.0-beta，约 1 周）

| 任务 | 交付物 |
|------|--------|
| 1a.1 `riverpod plugins list` | 输出已注册 source/transform/sink/codec |
| 1a.2 `riverpod validate --format json` | 结构化错误（path、message、line）供 Agent 解析 |
| 1a.3 `riverpod doc --format dot` | 管道拓扑 DOT（设计文档已规划） |
| 1a.4 Skill 更新 | 引用新 CLI 子命令 |

### Phase 1b：MCP Server（v2.0-beta，约 2 周）

独立进程或 `riverpod mcp`，暴露 Agent 工具：

| MCP Tool | 映射 |
|----------|------|
| `riverpod_validate` | validate config file/dir |
| `riverpod_test` | run fixture suite |
| `riverpod_plugins_list` | list registered plugins |
| `riverpod_pipeline_status` | GET /admin/pipelines/{name}/status |
| `riverpod_reload` | POST /admin/reload/{name} |

实现计划：[2026-07-01-agent-skill.md](superpowers/plans/2026-07-01-agent-skill.md)

### Phase 2：Pipeline 内 LLM（v2.1，约 3 周）

原 Phase A，顺序后移。详见 [2026-07-01-ai-agent-foundation.md](superpowers/plans/2026-07-01-ai-agent-foundation.md)。

| 任务 | 交付物 |
|------|--------|
| 2.1 Provider 接口 + OpenAI/Ollama | `internal/llm/` |
| 2.2 `llm` / `embed` transform | `plugins/transform/llm/`, `embed/` |
| 2.3 eql 模板 + LLM 指标 | `internal/eql/template.go`, observability |

### Phase 3：Pipeline 内 Agent（v2.2，约 4 周）

原 Phase B：`agent` transform、tool loop、向量 Sink。

### Phase 4：编排与 MCP 工具双向（v2.3+）

原 Phase C；另含 **riverpod 作为 MCP tool 被外部 Agent 调用** 与 **pipeline 内 agent 调用 MCP** 的统一 tool 抽象。

---

## 9. 实现计划索引

| 文档 | 范围 |
|------|------|
| [2026-07-01-agent-skill.md](superpowers/plans/2026-07-01-agent-skill.md) | **Phase 0–1b** Agent Skill + CLI JSON + MCP |
| [2026-07-01-ai-agent-foundation.md](superpowers/plans/2026-07-01-ai-agent-foundation.md) | Phase 2 管道内 LLM（TDD 任务分解） |
| [skills/riverpod/SKILL.md](../skills/riverpod/SKILL.md) | Agent 操作手册（skills.sh 发布） |
| [skills/README.md](../skills/README.md) | 安装：`npx skills add riverpod/riverpod@riverpod` |
| [configurations.md](configurations.md) | 配置字段参考 |

---

## 10. 风险与约束

| 风险 | 缓解 |
|------|------|
| LLM 延迟高，占满 inflight | per-stage `concurrency_limit`；独立 AI pipeline 与核心 ETL 隔离 |
| Token 成本不可控 | `token_estimate` 前置校验；metrics 告警；可选 `max_tokens` 硬限 |
| Tool 调用 SSRF | HTTP tool 允许 list 域名；禁止内网段（可配置） |
| Session 内存泄漏 | memory store LRU + TTL；生产用 redis |
| 厂商 API 变更 | Provider adapter 隔离；集成测试锁定行为 |

---

## 11. 非目标（v2.3 前）

- 内置向量数据库（只做 Sink 插件）
- 可视化 Agent 编排 UI（社区 / v2.3+）
- 微调 / 训练 pipeline
- 引擎内嵌 RAG 检索（用 enrich + HTTP 或专用 transform 组合）

---

> 下一步：**Phase 1a** — `riverpod plugins list` 与 `validate --format json`；**Phase 1b** — MCP Server。Pipeline 内 LLM 见 Phase 2 计划。

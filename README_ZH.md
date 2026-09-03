# Riverpod

[English](README.md) | 中文

Go 实现的 DAG 事件路由器。以通用 `Message`（`[]byte` payload + metadata）为中心，通过 **Source → Transform → Sink** 有向无环图处理流式数据，支持条件路由、per-edge 缓冲与投递策略、重试/DLQ 与 at-least-once Ack。

> **当前状态：** v2.0-alpha 可观测性已闭合，进入 **v2.0-beta**（Sprint 2+）；详见 [开发路线图](riverpod-design.md#12-开发路线图)。

## 特性概览

| 能力 | 说明 |
|------|------|
| **DAG 拓扑** | 分支、fan-in/fan-out；边通过 `depends_on` 声明（含 `route` / `buffer` / `delivery`） |
| **协议无关** | 引擎不解析 payload；编解码由 Codec 插件完成（Source `decoder` / Sink `encoder`） |
| **eql** | CEL 表达式 + 赋值扩展（`payload.x = expr`、`del()`），用于 map / filter / 边 condition |
| **可靠性** | 背压自动传播、refCount Ack、边级 retry/DLQ、可选 disk buffer |
| **可观测性** | `riverpod_*` Prometheus 指标、健康检查；OTLP tracing 与通知为规划中能力 |
| **扩展** | Go 编译时注册；WASM（wazero）与 gRPC 进程外插件计划 v2.1 |
| **AI / Agent** | **Agent 优先：** [skills.sh Skill](skills/riverpod/SKILL.md)（`npx skills add riverpod/riverpod@riverpod`）+ CLI/Admin API；管道内 `llm`/`agent` Transform 计划 v2.1+ — 详见 [AI/Agent 指南](docs/ai-agent.md) |
| **部署** | 单二进制多 Pipeline；K8s Operator 计划 v2.1 |
| **配置** | **YAML**（CRD / CI）+ **HOCON**（Envelope 风格本地配置），解析为统一 IR；详见 [配置规范](docs/configurations.md) |

## 架构

```
YAML / HOCON / CRD  →  PipelineConfig  →  TopologyIR  →  Engine (fanOut/fanIn/Ack)
                              ↓
                    Source / Transform / Sink / Codec 插件
```

**术语：** 配置层 **step**（`steps.{name}`）→ 运行时 **stage**（`StageIR`）→ **edge**（由 `depends_on` 展开）。

## 配置示例

推荐 `steps` 写法（YAML）：

```yaml
apiVersion: riverpod/v1
kind: Pipeline
metadata:
  name: order-processing

steps:
  kafka-in:
    source:
      type: kafka
      decoder: json
      config:
        brokers: ["${KAFKA_BROKERS}"]
        topics: [orders]

  enrich:
    depends_on: [kafka-in]
    transform:
      type: map
      config:
        dsl: |
          payload.total = payload.price * payload.quantity

  orders-out:
    depends_on: [enrich]
    sink:
      type: kafka
      encoder: json
      batch: { size: 100, timeout: 1s }
      config:
        topic: orders-enriched
```

分支路由在下游 step 的 `depends_on` 中声明 `route`：

```yaml
  us-sink:
    depends_on:
      splitter: { route: us }
    sink:
      type: http
      config: { url: https://us-api.example.com/orders }
```

### Agent 优先的 AI 集成

**第一步**是让 AI Agent **操作** riverpod（写配置、校验、测试、运行），而非先在管道内嵌 LLM。

仓库提供兼容 [skills.sh](https://skills.sh/) 的 [Agent Skill](skills/riverpod/SKILL.md)：

```bash
npx skills add riverpod/riverpod@riverpod
```

教 Agent 使用 CLI、插件清单与 `depends_on` 模式。典型 Agent 工作流：

```bash
riverpod validate --config my-pipeline.yaml
riverpod test --config testdata/tests/my-suite.yaml
riverpod run --config my-pipeline.yaml
```

管道内 LLM 分类（未来 `llm` transform + `route`）将沿用同一 DAG 模型 — 见 [AI/Agent 指南](docs/ai-agent.md)。

```yaml
# 计划 v2.1+ — 尚未实现
  llm-classify:
    transform:
      type: llm
      config:
        provider: openai
        model: gpt-4o-mini
        messages:
          - role: user
            content: "Classify: ${payload.body}"
```

等价 HOCON、平坦 `pipeline[]` 兼容写法及分支路由见 [配置规范](docs/configurations.md)；设计背景见 [配置模型](riverpod-design.md#8-配置模型)。

## 仓库结构

| 路径 | 说明 |
|------|------|
| [`docs/configurations.md`](docs/configurations.md) | **配置规范**（Engine / Steps / 插件 / 边 / 变量替换） |
| [`docs/ai-agent.md`](docs/ai-agent.md) | **AI/Agent 指南**（Agent Skill、MCP 路线图、管道内 LLM 阶段） |
| [`skills/riverpod/`](skills/riverpod/) | **Agent Skill**（[skills.sh](https://skills.sh/riverpod/riverpod/riverpod)）编写 pipeline |
| [`skills/README.md`](skills/README.md) | Skills 目录与安装说明 |
| [`riverpod-design.md`](riverpod-design.md) | 需求与设计方案（主文档） |
| [`competitor-research.md`](competitor-research.md) | 竞品调研 |
| [`design-review.md`](design-review.md) | 设计评审（部分条目已过时，以主文档为准） |
| [`redesign-v3.md`](redesign-v3.md) | **v3 从零重设计提案**（定名 **Eventboat**；Agent 原生事件路由器，零自研语言 CEL+Starlark；提案，未实施） |
| [`cmd/riverpod/`](cmd/riverpod/) | CLI（`run` / `validate` / `test`） |
| [`internal/`](internal/) | 引擎、配置、拓扑、eql |
| [`plugins/`](plugins/) | Source / Transform / Sink / Codec 插件 |
| [`_examples/`](_examples/) | 常用模式演示配置（线性 ETL、分支、fan-in、边策略等） |
| [`testdata/pipelines/`](testdata/pipelines/) | CI / 单元测试用最小配置 |

## 构建与验证

```bash
go test ./...
go build -o bin/riverpod ./cmd/riverpod
bin/riverpod validate --config testdata/pipelines/linear.yaml
bin/riverpod test --dir testdata/tests
```

或使用 Makefile：

```bash
make build test validate pipeline-test
```

## 文档

- [配置规范](docs/configurations.md) — Engine、Steps、Source/Transform/Sink 插件、边与变量替换（参考 Envelope 布局）
- [AI/Agent 指南](docs/ai-agent.md) — LLM/Agent Transform、Provider 抽象、可观测性与分阶段交付计划
- [Agent Skill (skills.sh)](skills/README.md) — `npx skills add riverpod/riverpod@riverpod` 安装
- [v3 从零重设计提案](redesign-v3.md) — Agent 原生定位、零自研语言（CEL+Starlark）、配置与架构重设计（提案，未实施）
- [背景与目标](riverpod-design.md#1-背景与目标)
- [核心接口与 Message](riverpod-design.md#4-核心接口与-message-模型)
- [引擎运行时](riverpod-design.md#6-引擎运行时)
- [eql DSL](riverpod-design.md#7-dsl-语言设计-eql)
- [配置模型（设计）](riverpod-design.md#8-配置模型)
- [v2.0 定稿检查清单](riverpod-design.md#13-v20-定稿检查清单)

## 与 v1 的关系

riverpod **不向后兼容** Riverpod v1。v1 为线性 `Input → Processor → Output` + CloudEvents 绑定；v2 从 DAG、通用 Message、CEL/eql、Codec 体系与双模式部署重新设计。

## License

Apache-2.0 — 见 [LICENSE](LICENSE)。

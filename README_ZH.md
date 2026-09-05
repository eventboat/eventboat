# Eventboat（事件船）

**Agent 原生的事件路由数据面。** Pipelines as code, verified by machines,
operated by agents.

Eventboat 是一个 Go 单二进制的事件路由引擎。用 YAML 声明一条管道——
sources、transforms、sinks 通过 `from` 连成显式 DAG——Eventboat 负责持久地
执行它：事件进来（Kafka / HTTP / cron / 文件 / SQL），流过滤、映射、路由，
落到目的地——全程 at-least-once、可验证、可回放。谓词就是
[CEL](https://github.com/google/cel-go)（Kubernetes 的表达式语言），映射就
是 [Starlark](https://github.com/google/starlark-go)（Python 方言）：零自研
语言，为编写管道的 Agent 留下最大的训练语料。

**文档：** <https://eventboat.github.io/eventboat/> —— 开发者指南见
[docs/developer/](docs/developer/)，插件 / WASM / codec 参考文档见
[docs/plugins.md](docs/plugins.md)、[docs/wasm.md](docs/wasm.md)、
[docs/codecs.md](docs/codecs.md)。

> **状态：** v0.3.0，1.0 之前——配置面实际上已稳定，API 仍可能在次版本间
> 调整。变更记录见 [CHANGELOG.md](CHANGELOG.md)。
> English readme: [README.md](README.md)。License：Apache-2.0。

## 为什么选择 Eventboat

- **运行之前先验证。** `eventboat verify` 静态、确定、零副作用：插件
  JSON Schema、拓扑规则、CEL/Starlark 编译、lint。坏配置到不了生产——
  同一套 verify 同时驱动 CLI、LSP 诊断与 MCP 工具。
- **像代码一样可测。** 合约测试跑的是*真实*进程内引擎：注入 fixture、
  捕获输出，`eventboat test` 在 CI 里给管道变更把关，就像单测给代码
  把关一样。
- **可靠性是构造出来的。** 每条入口消息先持久落入 spool（SQLite，纯 Go，
  无 CGO），*然后*才对 DAG 可见；commit 跟踪器只在消息到达终态后推进
  checkpoint；崩溃恢复从 checkpoint 重放 spool。契约是 at-least-once：
  可能重复，绝不丢失。七条可靠性不变量各有专属测试（`TestInvariant_*`，
  见 [internal/engine](internal/engine/invariants_test.go)）。
- **可解释、可回放。** `explain` 符号化走查管道，或对样本消息实际执行
  （含脚本）；`replay` 把死信、spool 窗口、失败的作业运行重新注入——
  先用 `--dry-run` 预演一遍再动手。
- **Agent 原生的运维面。** MCP 服务器（stdio + HTTP，14 个工具）、带
  SSE 的 Admin REST 与只读控制台、面向编辑器的 LSP、每个命令的
  `--json`——全部是同一个 ops 服务之上的薄壳，Agent、编辑器与人看到
  的是同一份真相。
- **扩展阶梯，而非插件悬崖。** 编译期 Go 插件（自定义构建）、任意语言的
  进程外 gRPC 插件、wazero 沙箱化的 WASM transform、CESQL 边方言——
  每一级的存在理由都是性能或依赖，从来不是"复杂逻辑"。
- **一个朴素的二进制。** 纯 Go、无 CGO、distroless 容器，GHCR 上有
  linux/amd64 + arm64 镜像。

## 功能

**管道模型**——管道即三段式，`from` 连边，插件名即键：

```yaml
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: order-branching }

constants:
  vip_threshold: 10000

sources:
  ingest:
    decoder: json
    file: { path: input/events.jsonl }

transforms:
  enrich:
    from: [ingest]
    script: |
      payload.total = payload.price * payload.qty
      if payload.total > constants.vip_threshold:
          meta.tier = "vip"
      else:
          meta.tier = "basic"

sinks:
  eu-out:
    from: { enrich: { when: 'payload.region == "eu"' } }   # CEL 谓词
    encoder: json
    file: { path: output/eu.jsonl }
```

- **源：** `kafka`、`http_server`、`cron`、`file`、`sql`（MySQL /
  PostgreSQL / SQLite，keyset 分页 + 可续读水位）。
- **Transform：** `script`（Starlark）、`split`（JSON 数组逐元素成消息）、
  `wasm`（见下）——或你自己注册的 transform。
- **汇：** `kafka`、`http`、`file`、`drop`。
- **编解码：** `json`、`raw`、`csv`、`avro`、`protobuf`——在 `codecs:`
  段命名声明一次，任意节点按名引用（`decoder:` / `encoder:`）。
- **路由：** 每条边可挂 CEL 谓词，fan-in / fan-out，节点级 `workers`，
  sink 批量写出，`order_key`，显式投递策略。
- **失败语义：** 死信携带脚本 backtrace 与原因分类，按边策略重试，
  fan-out 零匹配按 *filtered* 正常提交（计数呈现，绝不无声丢弃）。

**作业管道（job pipelines）**——同一引擎上的调度/触发式批处理：cron
调度 + 补偿窗口，类型化的运行参数（`${parameters.x}`），重叠策略
（`skip` / `latest` / `all`），运行历史入同一存储，成功/失败钩子，
增量 SQL 同步的 `cursor` 水位，以及 `eventboat trigger` /
`eventboat jobs` 补数与查询。

**验证关卡**

| 关卡 | 命令 | 作用 |
|---|---|---|
| 验证 | `eventboat verify` | 静态、确定、零副作用：Schema、拓扑、CEL/Starlark 编译、lint |
| 合约测试 | `eventboat test` | 真实引擎进程内运行：任意节点注入 fixture、任意 sink 捕获，断言输出或死信 |
| 推演 | `eventboat explain` | 符号化走查、对样本消息的干跑推演、mermaid + ASCII 拓扑图 |
| 回放 | `eventboat replay` | 重注入死信 / spool 窗口 / 失败的作业运行；`--dry-run` 预演，成功后 `--delete` 清理 |
| 交互 | `eventboat repl` | 对样本消息做一次性或交互式 CEL/Starlark 求值 |

**Agent 与编辑器面**

- **MCP：** `eventboat mcp --stdio`（Agent 宿主拉起）或 `--http`——
  14 个工具：catalog、verify、test、explain、deploy、status、jobs、
  trigger、tail、dlq_query、dlq_replay、drain、pause、resume。
- **Admin REST + SSE + 只读控制台** 挂在守护进程的管理监听上（默认
  `127.0.0.1:7788`）；Bearer token 认证，非回环绑定未配置 token 拒绝
  启动。
- **LSP：** `eventboat lsp`——诊断来自真实 verify 管线，按插件目录与
  Schema 补全，字段 hover 文档。最小 VS Code 接入见
  [examples/editors/vscode](examples/editors/vscode)。
- **Schema 导出：** `eventboat plugin schema --all --dir schemas/`——
  全部插件的离线 JSON Schema 包，供 IDE 与 Agent 消费。

**扩展阶梯**

1. **编译期插件**——`pkg/plugin` 即插件 ABI：在 `init()` 里注册
   source/transform/sink/codec，四行 `main` 委托给根包的 `RunCLI`。
   你的插件在 verify、`plugin catalog`、LSP 与 MCP 面前与内置插件
   无差别。参考实现：[examples/custom-build](examples/custom-build)。
2. **进程外 gRPC 插件**——任何能跑 gRPC 的语言：静态 manifest 保证
   verify 零副作用，stdout 单行 JSON 握手认证运行时连接，`version:`
   版本钉对漂移报错，`grpc.restart` 可选快失败或受监督重生。协议与
   SDK 文档：[docs/plugins.md](docs/plugins.md)；第三方示例：
   [examples/plugins/ticker-source](examples/plugins/ticker-source)。
3. **WASM transform**——wazero 宿主、能力制沙箱、逐次调用的 wall-clock
   + 内存双预算；guest 用标准 Go 工具链构建（`GOOS=wasip1`）。定位是
   Starlark 太慢的重度逐消息计算。文档：[docs/wasm.md](docs/wasm.md)。
4. **CESQL 边方言**——`when: { lang: cesql, expr: ... }` 复用官方
   CloudEvents 解析器；官方 TCK 在 CI 内 100% 通过。

**可观测性**——OpenTelemetry 双导出：`/metrics` 上的 Prometheus 与
OTLP 推送；约 28 个 `eventboat_` 指标（吞吐、提交、按原因分类的死信、
背压、spool 深度、作业计数、时延直方图），每个作业运行一条 span，
逐消息 span 可选，`telemetry.redact` 对 `tail` 呈现做脱敏。

**安全**——整个管理面可选 Bearer token（`--admin-token` >
`EVENTBOAT_ADMIN_TOKEN` > Runtime 配置）；非回环的管理绑定未配置
token 拒绝启动；回环绑定强制 Host 白名单，防 DNS rebinding。

## 架构设计

```
                YAML（sources/transforms/sinks + from）
                                  │
                          loader ─┴─ ${VAR}/${?VAR}/${constants.*}/${parameters.*}
                                  │                           严格白名单
                          verify  ─┴─ 插件 JSON Schema、拓扑规则、
                                  │   CEL + Starlark 编译、lint
                                  │
                        静态 IR（DAG + 编译产物）
                                  │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                          ▼
   Engine（每管道）                                    Ops 服务
   source ─▶ spool（SQLite）─▶ 内存 DAG ─▶ sinks        │  ├─ CLI（--json）
                  │              commit 跟踪            │  ├─ MCP（stdio/HTTP）
                  └─ checkpoint ◀──── 各分支终态 ────────┘  ├─ Admin REST + SSE + UI
                     （sink 成功 / 死信 / filtered）        └─ LSP
```

- **加载 → 验证 → IR。** loader 严格（未知字段即错误），做一遍
  `${...}` 替换，来源为环境变量、`constants:`，以及（仅作业管道）
  触发时的 `parameters:`。verify 编译一切——插件配置对照 JSON
  Schema、每段 CEL/Starlark/CESQL 程序、图拓扑——产出静态 IR。
  verify 通过后，第一条消息流动之前管道就已完全确定。
- **引擎。** 每个管道实例把每条入口消息先落入 SQLite spool，再在内存
  中执行 DAG；commit 跟踪器统计每条消息到终态（sink 写出 / 死信 /
  filtered）的执行分支；checkpoint 只在已提交前缀上前进；崩溃恢复时
  重放 checkpoint 之后的 spool，拉取源从各自已提交的水位续读。背压经
  spool 准入闸门从 sink 传回 source。
- **一个 ops 服务。** `internal/ops` 是全部运维面；CLI 的 `--json`、
  MCP 工具与 Admin REST 端点共享同一批 Go 结构体与 JSON 形状。
  verify 不存在旁路：验证不过，`deploy` 即失败。
- **存储。** SQLite（`modernc.org/sqlite`，纯 Go）承载 spool、
  checkpoint、死信与作业历史；`--ephemeral` 换成内存存储便于本地
  开发。spool 保留窗口由 Runtime 配置的 `storage.spool_retention`
  限定。

## 如何使用

### 构建与运行

```bash
go build -o eventboat ./cmd/eventboat   # 或：docker build -t eventboat .

eventboat verify --config examples/linear/pipeline.yaml
eventboat test examples/linear          # 合约测试
eventboat run --config examples/linear/pipeline.yaml
eventboat run --config my.yaml --ephemeral        # 内存态，本地开发
eventboat run --config-dir pipelines/             # 多管道守护进程 + 管理 API
```

### Verify 优先的工作流（人或 Agent 通用）

```bash
eventboat --json verify --config pipeline.yaml    # 结构化诊断，适合 CI

eventboat explain --config pipeline.yaml --topology                 # mermaid + ASCII
eventboat explain --config pipeline.yaml --message sample.json      # 实际执行脚本
eventboat replay --config pipeline.yaml --dlq --since 2h --dry-run  # 预演
eventboat replay --config pipeline.yaml --dlq --where 'payload.region == "eu"' --delete
```

### 作业管道

```bash
eventboat run --config examples/job-sync/pipeline.yaml     # 调度 + 补偿
eventboat trigger --config examples/job-sync/pipeline.yaml \
  --parameters '{"from":"2026-09-01T00:00:00Z","to":"2026-09-02T00:00:00Z"}'
eventboat jobs list --config examples/job-sync/pipeline.yaml
eventboat jobs show <run-id> --config examples/job-sync/pipeline.yaml
```

### Agent 与编辑器

```bash
eventboat mcp --stdio          # MCP over stdio —— 接到你的 Agent 宿主
eventboat mcp --http           # MCP + Admin REST + SSE + 只读控制台
eventboat lsp                  # 语言服务器（stdio）
eventboat plugin catalog       # 全部已注册插件与版本
eventboat plugin schema --all --dir schemas/
```

### 合约测试

测试套件向真实引擎注入 fixture，对捕获或死信做断言：

```yaml
suite: order-branching
pipeline: ../pipeline.yaml
cases:
  - name: eu-order-routed-to-eu-only
    inject: { at: ingest, messages: [fixtures/eu-order.json] }
    expect:
      capture:
        at: eu-out
        messages:
          - payload.total: 12000     # 子集匹配
            meta.tier: vip
  - name: malformed-json-to-dlq
    inject: { at: ingest, raw: "{not json" }
    expect:
      dlq: { count: 1, reason_contains: decode }
```

### 自定义构建（编译期插件）

```go
package main

import (
	_ "example.com/myproject/myecho" // 在 init() 里经 pkg/plugin 注册
	"github.com/eventboat/eventboat"
)

func main() { eventboat.RunCLI() }
```

构建这个 `main`，你就有了一个私有的 `eventboat` 二进制，其中 `myecho`
与内置插件别无二致——verify、`plugin catalog`、LSP 与 MCP 全部认识它。
可运行的参考实现在 [examples/custom-build](examples/custom-build)。

### 容器与 Kubernetes

```bash
docker build -t eventboat:dev .    # CGO_ENABLED=0 → distroless/static，非 root
docker run --rm -v "$PWD/examples/linear:/work" -w /work eventboat:dev \
  run --config /work/pipeline.yaml
```

CI 持续发布 `ghcr.io/eventboat/eventboat`（`:main`、`:sha-<short>`、
版本标签；linux/amd64 + arm64）。约定挂载点为 `/pipelines` 与 `/data`；
现成的 Deployment 清单见
[examples/k8s/deployment.yaml](examples/k8s/deployment.yaml)，说明见
[docs/k8s.md](docs/k8s.md)。

## 仓库布局

```
eventboat.go           RunCLI —— 自定义构建的库入口
cmd/eventboat/         发布二进制（internal/cli 之上的薄 main）
internal/cli/          CLI 动词：verify / test / run / trigger / jobs / explain / replay / repl / lsp / plugin / mcp
internal/config/      类型化配置、严格加载、${...} 替换、codecs: 声明
internal/ir/          静态 IR：DAG、CEL/Starlark/CESQL 编译产物、拓扑、lint
internal/lang/        celhost / cesqlhost / starhost（语言沙箱宿主）
internal/wasmhost/    wazero 宿主：能力沙箱、逐调用预算
internal/rpcplugin/   gRPC 插件宿主：spawn、握手、source/sink 适配
internal/engine/      spool 准入、DAG 执行、commit 跟踪、死信、拉取源
internal/jobs/        作业运行时：调度、补偿、重叠、run 生命周期、钩子
internal/store/       SQLite + 内存版 spool/checkpoint/死信/作业历史存储
internal/registry/    插件注册：由类型化配置结构体生成 JSON Schema + ABI 版本
internal/registry/builtin/  kafka/http_server/cron/file/sql 源、script/split/wasm transform、kafka/http/file/drop 汇、json/raw/csv/avro/protobuf 编解码
internal/lsp/         语言服务器（JSON-RPC 2.0 over stdio）
internal/explain/     确定性推演 + 拓扑渲染
internal/ops/         MCP 与 Admin REST 背后的操作服务
internal/mcpserver/   MCP 工具（官方 Go SDK）
internal/admin/       Admin REST + SSE + 内嵌只读控制台
internal/obs/         OpenTelemetry：双导出、指标、span
proto/, pkg/pluginproto/   进程外插件线上协议（eventboat.plugin.v1）
pkg/plugin/           编译期插件 ABI（RegisterSource/Transform/Sink/Codec）
docs/                 plugins.md、wasm.md、codecs.md、k8s.md + 开发者指南
examples/             linear、branching、fanin、job-sync、codecs、custom-build、plugins/、editors/vscode、k8s
```

## 开发

```bash
go build ./...
go test ./...          # 含七条 TestInvariant_* 可靠性测试
go test -race ./...

# 集成套件（环境变量门控；本地自动跳过）
EVENTBOAT_KAFKA_TEST=1 go test ./internal/inttests/kafka/   # 需 Docker
EVENTBOAT_SOAK_TEST=1 EVENTBOAT_SOAK_DURATION=2m go test ./internal/inttests/soak/

bash scripts/bench-gate.sh   # CI 强制的宽松性能门
```

贡献指南与架构深读见[开发者指南](docs/developer/)；设计历史见
[CHANGELOG.md](CHANGELOG.md) 与仓库根目录带标签的设计文档。

## 许可证

[Apache-2.0](LICENSE)

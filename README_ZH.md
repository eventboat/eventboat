# Eventboat（事件船）

**Agent 原生的事件路由数据面。** Pipelines as code, verified by machines,
operated by agents.

Eventboat 是一个 Go 单二进制：事件进来（Kafka / HTTP / cron / file），沿显式
DAG 流动（过滤、映射、路由），落到目的地——全程 at-least-once、可验证、可
回放。谓词就是 [CEL](https://github.com/google/cel-go)（Kubernetes 的表达式
语言），映射就是 [Starlark](https://github.com/google/starlark-go)（Python
方言）：零自研语言，Agent 语料最大化。

> **状态：v3 POC**（[redesign-v3.md](redesign-v3.md) 的 M1 里程碑）。
> v2 实现已整体归档至 [legacy/](legacy/)，互不兼容。实现前的独立设计审查见
> [redesign-v3-review.md](redesign-v3-review.md)（结论：通过，13 项发现）。
> License：Apache-2.0。English readme: [README.md](README.md)。

## 工作原理

```
                YAML（sources/transforms/sinks + from）
                                  │
                          loader ─┴─ ${VAR} 替换、严格白名单
                                  │
                          verify  ─┴─ 插件 JSON Schema、拓扑规则、
                                  │   CEL + Starlark 编译、lint
                                  │
                        静态 IR（DAG + 编译产物）
                                  │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                          ▼
   Engine（每管道）                                    CLI / 未来 MCP
   source ─▶ spool（SQLite）─▶ 内存 DAG ─▶ sinks
                  │              settle 跟踪           │
                  └─ checkpoint ◀──── 各分支终态 ┘
                    （sink 成功 / 死信 / 可选边丢弃）
```

可靠性模型（redesign-v3.md §6.2）：入口消息**先落 spool 再对 DAG 可见**；
settle 跟踪器统计每条消息到终态的执行分支；checkpoint 只在消息 settle 后
前进；崩溃恢复 = 从 checkpoint 重放 spool。七条不变量每条一个专属测试
（`TestInvariant_*`，位于 [internal/engine](internal/engine/invariants_test.go)）。

## 快速上手

```bash
# 构建
go build -o eventboat ./cmd/eventboat

# 关卡一：verify（静态、确定性、零副作用）
eventboat verify --config examples/linear/pipeline.yaml
eventboat --json verify --config examples/branching/pipeline.yaml   # CI/Agent 用

# 关卡二：合约测试——进程内真实引擎 + fixture 注入 + 捕获
eventboat test examples/linear/tests examples/branching/tests

# 运行（持久：SQLite 存储在 ./data；--ephemeral 为本地开发内存态）
eventboat run --config examples/linear/pipeline.yaml
eventboat run --config my.yaml --ephemeral
```

管道 = 三段式 + `from` 连边；插件名即键：

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

合约测试（关卡二，§3.2 格式）：

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

三条示例管线（线性、CEL 分支、fan-in）见 [examples/](examples/)。

## POC 范围（M1）与裁剪记录

已实现：三段式配置（严格白名单 + `${VAR}`/`${?VAR}` 替换）；CEL 谓词宿主
（零自定义函数，求值错误 = 条件不通过 + 计数）；Starlark 宿主（程序预编译
复用、payload/meta 惰性 + 写时复制绑定、`json`/`math` 白名单、100k 步数预
算、backtrace 进死信）；引擎（spool/settle/checkpoint/背压/回放，SQLite
承载——`modernc.org/sqlite` 纯 Go，不手写 WAL）；死信库；强制 JSON Schema
的插件注册（源：kafka/http_server/cron/file；汇：kafka/http/file/drop；
编解码：json/raw）；CLI `verify`/`test`/`run`（`--json`）。

裁剪与超出规范的决策（对应 redesign-v3-review.md 记录）：

- **裁剪：** §7.4 M1 中的 `repl`/`plugin` 命令、conformance 语料与完整基准
  套件（保留最小 Go 基准：CEL 谓词 ≈ 290 ns/op、简单 Starlark transform
  ≈ 1.4 µs/op，开发机实测）。`explain`、`replay`、作业管道
  （`run`/`parameters`）、MCP server 与可观测性栈为 M2+。
- **部署级配置**（开放问题 #10）暂以 CLI 标志代替：`--data-dir`、
  `--ephemeral`。
- **模块：** load 白名单为 `json` + `math`；go-starlark 无可 load 的
  `strings` 模块——字符串方法是 string 类型内建（审查 R3）。
- **transform 失败**按入边 delivery 策略重试后死信（审查 R6）；fan-out 零
  匹配 = 正常 settle + 计数（审查 R7）；`split` = JSON 数组逐元素成消息，
  子消息继承父 message_id（审查 R8）。
- **spool 存原始字节 + codec 标记**（审查 R9）。崩溃后源从已 settle 水位
  恢复，未 settle 尾部可能在 spool 重放之外被源再次送出——可能重复投递，
  绝不丢失。
- **`order_key`** 在 sink 侧求值为消息键（如 Kafka 分区键）；完整 per-key
  有序分片为 P1。`workers` 提供 transform 节点级并发。
- **与框架字段撞名的插件名**在注册时拒绝（审查 R5）。

## 仓库布局

```
cmd/eventboat/        CLI：verify / test / run
internal/config/      类型化配置、严格加载、环境变量+常量替换
internal/ir/          静态 IR：DAG、CEL/Starlark 编译产物、拓扑校验、lint
internal/lang/        celhost（谓词）、starhost（Starlark 沙箱宿主）
internal/engine/      spool 准入、DAG 执行、settle、投递、死信
internal/store/       SQLite + 内存版 spool/checkpoint/死信存储
internal/registry/    插件注册（强制 JSON Schema）
internal/registry/builtin/  内置 kafka/http_server/cron/file 源、kafka/http/file/drop 汇、json/raw 编解码
internal/testkit/     注入/捕获/故障注入原语
internal/testrun/     §3.2 合约测试 runner
examples/             线性、CEL 分支、fan-in 管线与测试套件
legacy/               归档的 v2 实现（不导入、不修改）
```

## 开发

```bash
go build ./...
go test ./...          # 含七条 TestInvariant_* 可靠性测试
go test -race ./...
```

设计文档：[redesign-v3.md](redesign-v3.md)（v3 唯一设计规范）、
[redesign-v3-review.md](redesign-v3-review.md)（实现前审查）。历史文档：
[riverpod-design.md](riverpod-design.md)、[competitor-research.md](competitor-research.md)、
[review-2026-08.md](review-2026-08.md)、[design-review.md](design-review.md)。

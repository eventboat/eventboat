# Eventboat（事件船）

**Agent 原生的事件路由数据面。** Pipelines as code, verified by machines,
operated by agents.

Eventboat 是一个 Go 单二进制：事件进来（Kafka / HTTP / cron / file），沿显式
DAG 流动（过滤、映射、路由），落到目的地——全程 at-least-once、可验证、可
回放。谓词就是 [CEL](https://github.com/google/cel-go)（Kubernetes 的表达式
语言），映射就是 [Starlark](https://github.com/google/starlark-go)（Python
方言）：零自研语言，Agent 语料最大化。

> **状态：v0.1.0-beta**（[redesign-v3.md](redesign-v3.md) 的 M1 + M2 + M3 + M4
> 里程碑，加上 [redesign-v3-review-beta.md](redesign-v3-review-beta.md) 的
> beta 硬化轮——除 [CHANGELOG.md](CHANGELOG.md) 所列旋钮外无新产品面）。
> v2 实现互不兼容；其归档树已在 beta 后从工作树移除（可经 git 历史与
> v0.1.0-beta tag 考古恢复）。实现前的独立设计审查见
> [redesign-v3-review.md](redesign-v3-review.md)（M1，通过，13 项发现）、
> [redesign-v3-review-m2.md](redesign-v3-review-m2.md)（M2，通过）、
> [redesign-v3-review-m3.md](redesign-v3-review-m3.md)（M3，通过）、
> [redesign-v3-review-m4.md](redesign-v3-review-m4.md)（M4，通过）与
> redesign-v3-review-beta.md（beta 轮，通过）。定名前置调研记录：
> [docs/naming-checklist.md](docs/naming-checklist.md)。
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

# 关卡二：合约测试——进程内真实引擎 + fixture 注入 + 捕获。
# 目录模式递归遍历；无顶层 `suite:` 键的 yaml（如 pipeline）跳过并计数。
eventboat test examples

# 运行（持久：SQLite 存储在 ./data；--ephemeral 为本地开发内存态）
eventboat run --config examples/linear/pipeline.yaml
eventboat run --config my.yaml --ephemeral
```

## M3：扩展阶梯（WASM / gRPC 插件 / CESQL）

- **gRPC 进程外插件**：任意语言实现 source/sink——stdout 单行 JSON 握手 +
  静态 manifest（verify 永不 spawn 进程）+ 版本钉（不符 = verify 错误）。
  协议与 SDK 文档见 [docs/plugins.md](docs/plugins.md)；第三方视角示例
  `examples/plugins/ticker-source`（独立 Go module，只依赖协议生成代码）的
  verify→run 全链路验收进 CI。`eventboat plugin catalog` 列出全部插件与 ABI 版本。
- **WASM transform（wazero）**：`wasm:` 主字段（与 script/split 互斥）；
  能力制沙箱（默认零宿主能力）；资源 = 每次调用 wall-clock + 内存页双上限，
  `timeout_ms: 0` 为快速模式（击杀机制实测约 5× 开销，如实入档）；
  guest 用**标准 Go 工具链**即可构建（wasip1 reactor），ABI 见
  [docs/wasm.md](docs/wasm.md)。基准：重度脚本快速模式 ~2.3× 提速、分配数
  ~30000× 减少；轻脚本 Starlark 胜出（阶梯触发标准的量化依据）。
- **CESQL 可选方言**：`when: { lang: cesql, expr: ... }`，复用官方
  CloudEvents 解析器；官方 TCK（275 例）vendored 入库，**CI 内 100% 通过**；
  `data.*` 为文档化扩展（合成驼峰标识符），带下划线的 meta 键在本方言
  不可达（用 CEL）——诚实入档。

## M4：生态收口（convert / LSP / codec / Schema 分发 / repl）

- **convert（v2 → v3）**：`eventboat convert <v2配置> [-o out.yaml] [--report v3.md]`。
  三套 v2 写法（steps / pipeline[] / 顶层 edges）+ HOCON 全部解析；eql1 经
  CEL-AST 渲染为 Starlark，**"自动迁移" = 生成且通过真实编译器**（Starlark
  走 starhost、产出走全量 verify），子集外逐条进报告（原因 + 建议改法）。
  route/filter 折叠为有序边守卫；v2 无匹配静默丢弃 → v3 settle-as-filtered +
  计数（结局相同、可观测）。**legacy 全部 12 个示例/testdata convert 后
  verify 全绿进 CI**（快照 + 三例语义等价永久测试，fixture 现存于
  `internal/convert/testdata/v2/`）。legacy 归档期间只读未动；移树后可经
  git 历史与 v0.1.0-beta tag 考古恢复。
- **LSP（编辑器内写管道）**：`eventboat lsp`（stdio）。诊断 = 真实 verify
  管线（与 CLI/MCP 同一代码路径，零第二套校验）；补全 = 顶层段/框架字段/
  插件名（registry catalog）/插件字段（JSON Schema）/边属性/codec 名；
  hover = 插件字段摘要与框架字段语义。手写最小 JSON-RPC 2.0（零新增依赖；
  go.lsp.dev/protocol 需 Go 1.26 > 本仓 1.25）。最小 VS Code 扩展：
  `examples/editors/vscode`（`npm install` 后
  `code --extensionDevelopmentPath=.` 一步接入）。协议级集成测试进 CI。
- **codec 三件 + `codecs:` 命名声明段**：csv（一条消息一条记录，列名显式
  或首行 header）、avro（hamba/avro v2——LinkedIn 自己已迁移至此，goavro
  维护模式；行内 schema）、protobuf（FileDescriptorSet + 消息全名，路径相
  对管道文件，动态消息经 protojson——零新增依赖）。`codecs: {名: {type:,
  ...配置}}`，`decoder`/`encoder` 按名引用；声明名与注册名两名字空间禁遮蔽。
  CEL 类型映射表见 [docs/codecs.md](docs/codecs.md)；示例管道
  `examples/codecs` 进 CI。**catalog JSON 形状变化**：codecs 从字符串数组
  变 `{name,version,schema}` 对象数组（消费方需同步解析）。
- **Schema 独立分发**：`eventboat plugin schema <name>`（跨三段查名，文本/
  `--json`）；`plugin schema --all --dir schemas/` 批量导出
  `schemas/<kind>/<name>.json` 供 IDE/Agent 离线消费。
- **repl**（§3.6，M1 裁剪后回归）：`--cel 'expr'` / `--script f.star` 对
  `--message sample.json` 一次性求值；交互模式每行重放整个累积脚本（确定性
  会话语义）。
- **裁剪记档（M4 审查 R14）**：Pebble 性能档（replay/死信/作业历史查询面全
  是 SQL，第二后端 = 重写一套查询引擎，非锚点）；K8s Operator 降为
  Deployment 清单 + [docs/k8s.md](docs/k8s.md)（二进制自身的 verify-first
  reload 与探针面已覆盖 Operator 会做的事）。
- **顺手修复（starhost）**：补 §4.8 的 `remove()` 胶水；修复 M1 预存的
  **嵌套写入静默丢失** bug（惰性绑定下 `payload.nested.k = v` 无声无效——
  容器字段现按引用语义物化，纯标量读保持惰性；回归测试锁定）。

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
编解码：json/raw/csv/avro/protobuf）；CLI `verify`/`test`/`run`（`--json`）。

裁剪与超出规范的决策（对应 redesign-v3-review.md 记录；完整账本含 M2/M3/M4
见 [README.md](README.md)）：

- **裁剪：** §7.4 M1 中的 conformance 语料与完整基准
  套件（保留最小 Go 基准：CEL 谓词 ≈ 290 ns/op、简单 Starlark transform
  ≈ 1.4 µs/op，开发机实测；`repl` 已随 M4 回归）。`explain`、`replay`、
  作业管道（`run`/`parameters`）、MCP server 与可观测性栈已随 M2 落地。
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

## Beta 硬化轮（2026-09-04）——裁决摘要

完整台账（进本轮 / 留 beta+ 的逐条理由）与前后基准数字见
[redesign-v3-review-beta.md](redesign-v3-review-beta.md)；用户视角摘要见
[CHANGELOG.md](CHANGELOG.md)：

- **settle 持久化移出 tracker 锁**：前缀推进在锁内计算、落盘在结算
  goroutine 锁外执行；单调守卫保证 checkpoint/水位/指标乱序 flush 不回退。
  预研的"异步 worker"方案被实现期否决——不变量 7 测试中途直读存储依赖同
  goroutine 持久化序（七不变量零改动约束拍板）。观察者走尝试型持久屏障。
- **starhost 精确 dirty**：map 树写经标记闭包精确置脏，容器**读**不再置
  脏（只读脚本跳过全量写回，嵌套只读基准 -23%）；含 list 的树保守置脏
  （原生 list 变异无法拦截，边界有专属测试锁死）。
- **`max_in_flight` 在 `overlap: all` 下按管道聚合**（M2 R17 关闭）：
  并发 run 共享一个准入池，限额是管道总量。
- **`telemetry:` 段**（§5.10）：`{redact: [glob 路径], span_sample_rate:
  0.0}`。脱敏只作用于 **tail 呈现层**（掩码 `"***"`，截断前），数据面
  （spool/死信/投递）永不改动；逐消息 span 按率可选（默认 0）。
- **gRPC 插件崩溃策略**（M3 裁剪关闭）：`grpc.restart: fast-fail | restart`
  （默认 fast-fail = M3 语义原样）；`restart` 指数退避重生 + 重发配置与
  最新 Settled 状态 + 流/写重试，计数 `eventboat_plugin_restarts_total`。
- **CI 面**：golangci-lint v2 基线（零发现）；kafka testcontainers 集成
  job（真实 KRaft broker）；夜间 + 手动 soak workflow（注入故障的长跑，
  断言恰好一次结算与无 goroutine 泄漏）；bench job 升级为宽松阈值门
  （[scripts/bench-gate.sh](scripts/bench-gate.sh)）。

## 仓库布局

```
cmd/eventboat/        CLI：verify / test / run / trigger / jobs / explain / replay / convert / repl / lsp / plugin / mcp
internal/config/      类型化配置、严格加载、环境变量+常量替换、codecs: 声明
internal/ir/          静态 IR：DAG、CEL/Starlark/CESQL 编译产物、拓扑校验、lint、codec 解析
internal/lang/        celhost（谓词）、cesqlhost（CESQL 方言 + 官方 TCK）、starhost（Starlark 沙箱宿主）
internal/wasmhost/    wazero 宿主：能力沙箱、逐调用预算（guest 在 testdata/）
internal/rpcplugin/   gRPC 插件宿主：进程 spawn/握手、source/sink 适配
internal/engine/      spool 准入、DAG 执行、settle、投递、死信、拉取源
internal/jobs/        作业运行时：调度、补偿、重叠、run 生命周期、钩子
internal/store/       SQLite + 内存版 spool/checkpoint/死信/作业历史存储
internal/registry/    插件注册（强制 JSON Schema + ABI 版本，M4 起 codec 同规）
internal/registry/builtin/  内置 kafka/http_server/cron/file/sql 源、kafka/http/file/drop 汇、json/raw/csv/avro/protobuf 编解码
internal/convert/     v2 → v3 迁移工具：只读 v2 形状拷贝、eql 渲染器、守卫折叠、报告
internal/lsp/         语言服务器：最小 JSON-RPC 2.0、verify 诊断、补全、hover
internal/explain/     确定性推演 + 拓扑渲染
internal/ops/         MCP 与 Admin REST 背后的操作服务
internal/mcpserver/   MCP 工具（官方 Go SDK）
internal/admin/       Admin REST + SSE + 内嵌只读 UI
internal/runtimecfg/  kind: Runtime 部署配置
internal/obs/         OpenTelemetry：双导出、28 指标、span
internal/testkit/     注入/捕获/故障注入原语
internal/testrun/     §3.2 合约测试 runner
internal/inttests/    环境门控集成套件：kafka/（testcontainers 真实 broker）、soak/（长跑稳定性）
scripts/              bench-gate.sh——CI 宽松性能阈值门
docs/                 plugins.md、wasm.md、codecs.md、k8s.md、naming-checklist.md
examples/             线性、CEL 分支、fan-in、job-sync（sql）、codecs（csv+avro）、plugins（gRPC）、editors/vscode、k8s
```

## 开发

```bash
go build ./...
go test ./...          # 含七条 TestInvariant_* 可靠性测试、convert 快照/验收、LSP 协议集成、codec conformance
go test -race ./...

# 集成套件（仅 CI 跑；本地无环境变量时自动跳过）
EVENTBOAT_KAFKA_TEST=1 go test ./internal/inttests/kafka/    # 需 Docker
EVENTBOAT_SOAK_TEST=1 EVENTBOAT_SOAK_DURATION=2m go test ./internal/inttests/soak/

bash scripts/bench-gate.sh   # CI 的宽松性能门
```

设计文档：[redesign-v3.md](redesign-v3.md)（v3 唯一设计规范）、四份实现前
审查（redesign-v3-review*.md）。历史文档：
[riverpod-design.md](riverpod-design.md)、[competitor-research.md](competitor-research.md)、
[review-2026-08.md](review-2026-08.md)、[design-review.md](design-review.md)。

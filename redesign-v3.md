# v3 从零重设计提案 — Agent 原生事件路由器（零自研语言版）

> **状态：POC 阶段**（提案定稿：定名 Eventboat、License Apache-2.0；v3 全新实现，**不向后兼容 v2**——无迁移义务）
> **日期：2026-09-03**（修订 v1.1：零自研语言 CEL + Starlark + 性能评估；v1.2：吸收 dagu 作业模型（pipeline 级 `run`/`params`）；v1.3：拓扑结构改为**三段式 `sources`/`transforms`/`sinks` + `from` 连边 + 插件名即键**，命名体系定稿——含 DAG 描述模式调研结论，见 §5.1；v1.4：钩子段 `on`→`hooks`（GH Actions 撞形不同义），新增 consts/params 语义小节 §5.9；v1.5：**全称原则**——自造缩写全部展开：`params`→`parameters`、`consts`→`constants`、`dlq`→`dead_letter_queue`、`catchup`→`catchup_window`、`args`→`arguments`、`max_inflight`→`max_in_flight`，见 §5.1；v1.6：全称原则细化——**约定俗成的行业缩写保留**：`dlq`、`args`、`dsn` 维持缩写，回退 v1.5 对前两项的展开；v1.7：**定名 Eventboat**（§8，六轮核查 + 三选一裁决），全文占位符替换；v1.8：**License 定为 Apache-2.0**（仓库 LICENSE 落地，开放问题 #11 关闭）；v1.9：**明确 POC 阶段、不向后兼容 v2**——`convert` 降为按需工具，开放问题 #12 关闭；v1.10：按实现前审查（redesign-v3-review.md R1–R3）修正 §4.3 沙箱表：`while`/递归/顶层控制流的机制归属统一为 `syntax.FileOptions`，删除不存在的 `strings` 模块）
>
> 本文回答一个问题：**如果抛开 v2 现有实现，从零重新设计这个产品的方案、功能、配置方式和架构，应该长成什么样。**
>
> 与现有文档的关系：[riverpod-design.md](riverpod-design.md)（v2.0 主设计）、[competitor-research.md](competitor-research.md)（竞品调研）、[review-2026-08.md](review-2026-08.md)（实现评审）是本文的输入；本文不推翻 v2 的在途工作，而是给出"下一代"的目标形态与决策依据。若采纳，应作为 v3 的主设计文档起点。

---

## 目录

1. [现状诊断：什么值得保留，什么值得推倒](#1-现状诊断)
2. [战略定位](#2-战略定位)
3. [产品方案：四道机器关卡](#3-产品方案四道机器关卡)
4. [脚本与表达式层：零自研语言（CEL + Starlark）](#4-脚本与表达式层零自研语言cel--starlark)
5. [配置方式重设计](#5-配置方式重设计)
6. [架构设计](#6-架构设计)
7. [与 v2 的对比、迁移与里程碑](#7-与-v2-的对比迁移与里程碑)
8. [命名建议](#8-命名建议)
9. [开放问题](#9-开放问题)

---

## 1. 现状诊断

### 1.1 值得保留的资产

以下判断在 v2 设计、实现与两轮评审中已被验证，v3 全部继承：

| 资产 | 依据 |
|------|------|
| **Go 单二进制 + 显式 DAG 拓扑** | 与 Envelope/Spark 的重型路线形成本质区隔；拓扑可推理、可静态校验 |
| **协议无关 Message（`[]byte` + metadata）+ Codec 分层** | 连接器矩阵 N×M → N+M；引擎不解析 payload 是正确的边界 |
| **per-edge delivery 语义的概念**（retry / timeout / 死信 / required） | 概念正确（Knative DeliverySpec + Flume required/optional 的杂交优势），仅实现机制需要重构（见 §6.2） |
| **`validate` / `test` 声明式测试先行** | testrunner + fixture 是同类产品中最接近"管道单元测试"的设计 |
| **Agent 优先路线**（Skill + CLI + 校验先于运行） | 2026 年市场验证了这一方向（见 §2.1），v3 将其从"路线图"升级为"产品定位" |
| **自省文化**（design-review / review-2026-08 这类文档） | review-2026-08 抓出了真实的可靠性缺陷——这个习惯本身就是资产 |

### 1.2 推倒重来的六个理由

#### 理由一：配置层复杂度失控

v2 配置系统同时维护了：

- **两种格式**（YAML + HOCON），宣称"能力对等"，但语义已经分叉——环境变量未设置时，YAML 保留字面量、HOCON 解析报错（[docs/configurations.md](docs/configurations.md) 自认）；替换范围还遗漏 `engine` / `edgeDefaults` / `dlq` / `observability` / `codecs` 顶层段。
- **三套拓扑写法**：`steps` 嵌套块（推荐）、平坦 `pipeline[]`（兼容）、顶层 `edges:`（已废弃但仍在解析）。
- **`depends_on` 值形态爆炸**：序列 / 映射两种顶层形态，元素又可以是字符串或单键对象，一个字段承载了整条边的配置空间。
- **三层术语转换链**：step →（合体展开）→ stage + edge。合体 step（transform+sink）会生成用户从未写过的 `{name}-sink` 隐藏 id；`route` 字符串最终变成 `metadata["er-route"]` 的隐式运行时协议。
- **多层默认值继承**：`edgeDefaults` → 边级覆盖 → `engine.error_mode` → transform 级 `error_mode`，排错要追溯四层。

结论：**v3 只留一种格式、三段式拓扑、两种边元素形态，配置拓扑 = 运行时拓扑 = explain 输出**（见 §5）。

#### 理由二：DSL（eql）是"CEL 表达式 + 外挂赋值"的缝合体

eql1 的结构性问题（展开方案见 §4）：

- 无字符串模板、无语句级 if/else——拼接靠 `format()`、多分支靠嵌套三元；
- 无 fallibility 检查——`parse_json` 失败运行时才炸；
- 谓词层本可零成本复用 CEL（K8s 标准、Agent 语料巨大），却被外挂赋值语法"污染"成一个自研方言；
- 错误三态（编译/求值/缺失）语义模糊：字段缺失 → nil、类型不匹配 → false、函数错误 → 死信，三种行为挤在同一节文档里；
- 复杂逻辑直接掉进"写 Go 插件重新编译"的悬崖，中间没有逃生舱。

v3 的结论比"重设计一门语言"更进一步：**不自研语言**。谓词用 CEL 原样，映射用 Starlark，详见 §4。

#### 理由三：可靠性机制复杂但曾被证伪

review-2026-08 的核心发现（同日已修复大部分，但暴露的是**机制本身的复杂度风险**）：

- 手写 per-edge 磁盘 WAL：offset 先于投递推进 → 实际 at-most-once；无 CRC → torn write 后永久停摆。手写 WAL 是在重造一个数据库最难的部分。
- 每消息跨边 refCount Ack 链 + required 边 + 死信 + 重试的组合，正确性证明困难，评审一次性抓出 11 项并发缺陷（双重 Ack、inflight 负数、COW 串扰……）。
- 非 Kafka 源（http_server / cron）没有重放能力——"at-least-once"对它们无从谈起。
- 死信只进不出：死信没有配套的查询与回放工具，等于只做了"丢得体面"。

结论：**v3 放弃 per-edge 磁盘缓冲与跨边 refCount，改为"入口持久化 + 管道级 settle + checkpoint"模型**（见 §6.2），机制减少一半，不变量可以逐条写成测试。

#### 理由四：扩展体系空心

- WASM transform 从 v2.0-alpha 至今是占位空壳（`plugins/transform/wasm/wasm.go` 返回 not implemented）；
- gRPC 进程外插件推迟到 v2.1+；
- 内置连接器 3 source / 3 sink，数量上不可能与 Vector（几十个）/ Bento / Kafka Connect 竞争。

结论：v3 把扩展协议当作**一等交付物**而不是后续补丁（见 §6.5）；语言层用"CEL → Starlark → WASM/gRPC"的转换阶梯（§4.5）让绝大多数场景不需要走到插件那一步。

#### 理由五：可观测性半成品

指标从 9 个补到约 16 个，仍不足设计的 30+；trace 是 noop 骨架；OTLP 缺失。v3 把 OpenTelemetry SDK 作为唯一可观测底座（见 §6.6）。

#### 理由六：名字是负资产

"Riverpod" 与 Flutter 生态最流行的状态管理库 [riverpod](https://riverpod.dev/) 正面撞名。后果是双重的：

- **SEO 淹没**：搜索/文档检索几乎不可能露出；
- **Agent 语境污染**（更致命）：LLM 训练语料里 Riverpod≈Flutter 状态管理，Agent 读到本项目会产生系统性误解——这与"Agent 原生"定位直接冲突。

项目已经历 eventr → EdgeStream → Riverpod 三次改名。**v3 是最后一次低成本改名窗口**（见 §8）。

---

## 2. 战略定位

### 2.1 市场判断（2026）

对 competitor-research.md（2026-06 调研）之后的增量观察：

1. **独立流处理器赛道出现真空**：Benthos 被 Redpanda 收购并转为 Redpanda Connect（平台绑定），MIT 分叉 [Bento](https://github.com/warpstreamlabs/bento) 由 Warpstream 维护。中立、轻量、社区可信赖的"数据面"存在空位。
2. **流处理 × Agent 是 2026 最热交叉点**：Confluent 推出 [Streaming Agents](https://www.confluent.io/blog/introducing-streaming-agents/)（MCP + Flink），StreamNative 把 2026 峰会直接定位为 ["data streaming + agent infra"](https://streamnative.io/blog/announcing-data-streaming-summit-2026-the-data-streaming-agent-infra-conference)。大厂全部走**重平台**路线（Flink/Kafka + 控制面 + 商业化）。
3. **空白地带**：没有人做"**轻量、自包含、为 Agent 操作设计**"的数据面。所有竞品的 Agent 故事都是"在我们的平台上接 MCP"，而不是"把一个单二进制交给你的 Agent 全权操作"。
4. **作业面在奔向代码，数据面留守配置**（2026 结构性信号）：Temporal/Inngest/Restate/Dagster 等 code-first durable execution 引擎接管复杂作业编排，配置驱动（config-driven）的幸存地恰是数据面工具（Vector/OTel 一系）——佐证我们的切分：数据面 = 配置 + Agent 生成 + 机器验证；作业面 = 最小吸收（§5.8）。

### 2.2 定位陈述

> **一句话**：Agent 可安全操作的事件路由数据面。
>
> Pipelines as code, verified by machines, operated by agents.

展开：

- **给谁**：① 3-50 人工程团队的平台/后端工程师，用 AI Agent（或自己）搭建和运维事件管道，没有专职数据团队，不想引入 Kafka Connect/Flink/Knative 的平台成本；② Agent 本身——通过 MCP/CLI/Schema 消费这个产品（Agent 是"第二用户"，与第一用户同等重要）。
- **是什么**：一个 Go 单二进制。事件进来（Kafka/HTTP/cron/file/SQL），沿显式 DAG 流动（过滤、映射、路由），落到目的地（Kafka/HTTP/file），全程 at-least-once、可验证、可推演、可回放；定时/触发的**作业管道**（§5.8）覆盖批式数据搬运。
- **不是什么**：不是流处理平台（无 SQL/窗口/状态计算——那是 Flink 的事），不是集成平台（不做连接器军备竞赛），不是可视化编排器（Agent 写配置，渲染只读图给人看），**不是通用作业执行器**（跑命令/容器/审批流是 [dagu](https://github.com/dagu-org/dagu) 的领域——但它的作业编排语义被我们的作业管道吸收，见 §5.8 与 §7.5）。
- **类比**：事件路由领域的 SQLite——小、自包含、可靠、随处可见；外加一个 SQLite 从未有过的性质：**它的全部能力都有一个机器可操作的接口**。

### 2.3 非目标（明确不做）

| 不做 | 理由 |
|------|------|
| 连接器数量竞赛 | 打不赢 Vector/Bento；用插件协议 + 精选内置替代（§3.5） |
| 可视化编排 UI | 与"配置即代码 + Agent 主笔"定位相反；只做只读 DAG 渲染 |
| K8s Operator / 控制面 / 多租户 | v3 主形态是单二进制 + 标准 Deployment；Operator 降到 P2（§6.7） |
| exactly-once | at-least-once + 幂等写入指引；不承诺做不到的事 |
| 管道内 LLM 编排引擎 | LLM 只是普通 transform（可延后），编排用 DAG 表达；引擎保持无 LLM 感知 |
| 自研脚本/映射语言 | 谓词复用 CEL、映射复用 Starlark（§4）；语言本身不是差异化，验证与运维闭环才是 |
| 通用作业执行器（命令/容器/审批流） | dagu/Airflow 的领域；我们只吸收其"作业编排语义"用于批式数据搬运（§5.8） |
| 窗口/流式 SQL/有状态 join | 超出"路由器"定位；有状态处理留给专用系统 |

---

## 3. 产品方案：四道机器关卡

产品功能不再按"组件清单"组织，而是围绕 **Agent 的完整生命周期闭环**组织：**生成 → 验证 → 推演 → 上线 → 运维 → 回放**。核心是一句话：**Agent 能做的事情，机器必须先能证明它做对了。**

```
┌─────────┐   ┌─────────┐   ┌────────────┐   ┌──────────┐
│  verify │ → │  test   │ → │ simulate / │ → │ operate  │
│ 写完就查 │   │ 合约测试 │   │ explain    │   │ MCP/运维  │
└─────────┘   └─────────┘   └────────────┘   └──────────┘
     ↑                                            │
     └────────── 回放/死信回灌/报错自修复 ←────────┘
```

### 3.1 关卡一：verify（写完就查）

`eventboat verify --config p.yaml [--json]`，CI 与 Agent 共用同一入口。全部检查**静态、确定性、零副作用**：

1. **Schema 校验**：管道配置与每个插件配置块都按注册的 JSON Schema 严格校验（未知字段 = 错误，不是警告）；node 层框架字段按白名单枚举（§5.3）。
2. **拓扑不变量**：node 名跨三段全局唯一；`from` 引用存在；source 无入边、sink 无出边；无环；至少一条 source→sink 通路；无孤立节点。
3. **表达式与脚本编译**：
   - 全部 `when` / filter 谓词按 **CEL 编译**（cel-go，含类型检查；有 payload schema 时未知字段 warning）；
   - 全部 `transforms.*.script` 按 **Starlark 编译 + resolve**（go-starlark 的 resolver 在编译期抓未定义名、函数 arity 错误），外加宿主 lint（禁用模块/函数引用白名单检查，见 §4.4）。
4. **作业配置校验**：`run.schedule` cron 语法合法；`overlap`/`catchup_window`/`retention` 取值合法；`parameters` 声明（type/default/enum/pattern/required）自洽且被 `args`/脚本引用时类型匹配；作业管道的 source 必须具备 pull 能力（§5.8）。
5. **语义 lint**（超出"对错"的告警，`--strict` 时升级为错误）：
   - `when` 恒真/恒假（对照 schema 常量字段可判定的分支）→ 死分支；
   - 不可达 sink（无任何入边可达）；
   - required 边配了 `drop` 策略 buffer（语义矛盾：丢消息还声称 required）；
   - sink `batch.size` 大于下游建议上限（插件 schema 可声明）；
   - 未使用的 `constants` / codec / 顶层 `def`；
   - Starlark 脚本引用了 `time` / `random` 等禁用模块或非确定函数（§4.4）。
6. **输出**：人类可读 + `--json`（`{severity, code, file, line, message, hint}`），错误信息带行列与修复建议——这是给 Agent 迭代用的（Agent 靠报错修代码）。

### 3.2 关卡二：test（合约测试）

继承 v2 testrunner 思想并强化为**合约测试**：

```yaml
# tests/orders.yaml
suite: orders-pipeline
pipeline: ../pipelines/orders.yaml
cases:
  - name: vip-order-routed-to-eu
    inject: { at: ingest, messages: [fixtures/vip-order.json] }
    expect:
      capture: { at: eu-out }
      messages:
        - payload.total: 12000        # 子集匹配
          meta.tier: vip
  - name: malformed-json-to-dlq
    inject: { at: ingest, raw: "{not json" }
    expect:
      dlq: { count: 1, reason_contains: "parse" }
      capture: { at: eu-out, count: 0 }
```

增强点：黄金文件快照（`--update-golden`）、样例变体批量生成（同 schema 随机扰动）、注入位置任意 node、死信断言、**时钟注入**（确定性，见 §4.4）、`--json` 输出。测试在进程内跑真实引擎（继承 testutil generator/capture 模式），不需要起服务。

### 3.3 关卡三：simulate / explain / replay（上线前推演 + 事后回放）

这是 v3 最具差异化的能力：**引擎能解释自己**。

**`eventboat explain --config p.yaml --message sample.json`** — 给定一条样例消息，输出确定性推演（不运行引擎，纯静态 IR + 求值）：

```
message sample.json enters at node "ingest" (source kafka / decoder json)

  ingest → enrich            always (no condition)
  enrich: transform.script (starlark, 4 statements, budget=100k steps)
  enrich → eu-out            when meta.region == "eu"        ✓ MATCH
  enrich → us-out            when meta.region == "us"        ✗ no match
  eu-out: sink kafka         batch=100/1s, retry=5×backoff

settle: 1 branch → settles when kafka ack (or retry exhausted → dead letter)
```

`--message` 可省略，此时输出符号化推演（各边条件的字段依赖与取值域）。`explain --topology` 输出渲染好的 DAG（mermaid/ASCII，nodes + edges 与配置一一对应）。**术语说明**：`node`（节点）是概念词，用于 explain/status/内部 IR；它不是配置键——配置里节点以 `sources/transforms/sinks` 三段的条目存在（§5.3）。

**`eventboat replay`** — 事后回放，把"数据面"变成可调试系统：

- `replay --dlq --since 2h --where 'meta.dlq.reason contains "parse"'` → 从死信库筛出死信，重新注入任意 node（可先 `--dry-run` 看路径）；
- `replay --spool --from <checkpoint-id>` → 按持久化入口重放一段窗口（灾备演练/升级验证）；
- `replay --job <run-id>` → 按作业 run-id 回放该次运行的死信子集（= dagu 的"restart failed"，见 §5.8）；
- 回放默认写 `is_replay=true` 进 meta，sink 可识别。

### 3.4 关卡四：operate（MCP + Admin，运行中闭环）

**MCP Server 是一等公民**（不是 v2 路线图里的"Phase 1b 择期"）：

| MCP tool | 作用 |
|----------|------|
| `catalog` | 插件清单 + 每个插件的 JSON Schema + 版本（Agent 据此生成合法配置，杜绝"发明不存在的插件"） |
| `verify` / `test` | 与 CLI 完全同语义，返回结构化诊断 |
| `explain` | 消息路径推演 |
| `deploy`（reload） | 提交新配置：先 verify → 通过则 per-pipeline 优雅替换（drain 旧、起新）→ 返回变更摘要 |
| `status` | 管道/node/边级运行态：速率、积压、在途、最近错误（SSE 订阅实时更新） |
| `jobs` | 作业历史查询：run-id、parameters、状态、起止、行数/投递/死信计数 |
| `trigger` | 手动触发作业管道，可带 `parameters`（回补场景，§5.8） |
| `tail` | 按 node/按边抽样最近 N 条消息（脱敏规则适用） |
| `dlq_query` / `dlq_replay` | 死信检索与回灌 |
| `drain` / `pause` / `resume` | 优雅排空与暂停（暂停 = 停止 source 拉取，spool 继续兜住已有源） |

配套：Admin REST（MCP 的 HTTP 化子集，供人/脚本用）、内嵌**只读** DAG 可视化页（单静态页，读 `/admin/status.json` 画 mermaid，后续扩展作业历史视图）、全局 `--json`、日志级别动态调整。**写操作全部走"先 verify 再生效"的单一通道**——不存在绕过校验的热改路径。

### 3.5 连接器策略：精选内置 + 协议长尾

| 层 | 内容 | 质量标准 |
|----|------|----------|
| 内置 source | `kafka`、`http_server`、`cron`、`file`（tail）；`sql`（mysql/postgres，P1，拉取型） | 生产级：重试、背压、offset/checkpoint/水位、指标、故障注入测试 |
| 内置 sink | `kafka`、`http`、`file`（滚动）、`drop` | 生产级：批量、幂等指引、死信语义 |
| 内置 codec | `json`、`raw`、`csv`(P1)、`avro`(P1)、`protobuf`(P1) | 每个 codec 带 schema 推断与 CEL 类型映射 |
| 表达式/脚本 | `when`（CEL）、`transforms.*.script`（Starlark）、`split` | 见 §4；WASM transform 为 P2 逃生舱 |
| 长尾 | gRPC source/sink 插件协议（进程外任意语言实现） | 协议带版本协商 + schema 声明（§6.5） |

**拉取型源能力位**：插件 schema 声明 `capabilities: [pull]`（可被作业调度：分页、水位、取尽信号）。`run.mode: job` 引用非 pull 源 = verify 错误。调度、补偿、重叠、历史全部由引擎在 pipeline 层一次实现（§5.8），源插件只回答"按参数取下一批"。

判断依据：连接器广度是 Vector/Bento 的护城河，正面竞争无胜算；但"精选内置 + 开放协议"足以覆盖路由器的核心场景，其余交给生态。`sql` 源入选内置是因为 DB→Kafka 同步是目标用户前三的场景，且 Go 纯驱动（无 CGO）成本低。

### 3.6 CLI 命令总览

```
eventboat run        --config / --config-dir          运行（含热重载信号）
eventboat verify     --config [--strict] [--json]     静态验证（关卡一）
eventboat test       --config / --dir [--json]        合约测试（关卡二）
eventboat explain    --config [--message f.json]      路径推演（关卡三）
eventboat replay     --dlq / --spool / --job [...]    死信/窗口/作业回放（关卡三）
eventboat trigger    <pipeline> [--parameters json]   手动触发作业（§5.8）
eventboat jobs       list | show <run-id>             作业历史
eventboat convert    --from v2 --to v3                配置与 eql 迁移（§7.3）
eventboat repl       [--script f.star] [--cel 'expr'] 脚本/表达式 REPL（§4.4）
eventboat plugin     list | schema <plugin>           插件清单与 Schema
eventboat mcp        [--stdio | --http]               启动 MCP Server（关卡四）
```

---

## 4. 脚本与表达式层：零自研语言（CEL + Starlark）

> v1.0 提案曾设计自研映射语言 eql2；v1.1 修订改为**零自研语言**。决策依据：语言本身不是这个产品的差异化（验证与运维闭环才是），而自研语言的成本（设计/编译器/conformance/文档/训练语料从零开始）远超一个轻量路由器该背的负担。行业佐证：Vector/Benthos 自研 DSL 体验好，但背后是全职团队在养语言本身（Benthos 归 Redpanda 后 Bloblang 演进明显放缓）；嵌入方想要低成本的安全脚本能力，主流答案是复用现成语言（Bazel/Buck2/Tilt 用 Starlark，Caddy 用 expr，K8s 生态用 CEL，dagu 走了 expr/自研 action 的折中）。

### 4.1 决策：两个现成语言，各在其最强形态

| 层 | 语言 | 为什么是它 |
|----|------|-----------|
| **谓词**（`when` / filter，每条出边、每条消息都要求值的最热路径） | **CEL，原样使用，零扩展** | 为这个场景而生：K8s Admission / Envoy / Argo 全在用；Agent 语料极充足；cel-go 编译期类型检查 + 函数调用编译期解析（求值快）+ 天然保证终止。v2 的教训不是"选了 CEL"，而是"CEL + 外挂赋值魔改"——v3 谓词层一个自定义函数都不加 |
| **映射**（`transforms.*.script`，字段级变换与过程逻辑） | **Starlark（go-starlark）** | 为"配置里嵌脚本"而生（Bazel/Buck2/Tilt 先例）；Python 方言 → 上手成本最低、Agent 语料量最大的语言体系；沙箱与终止性语言级内建（§4.4） |
| **CloudEvents 互操作**（可选） | CESQL 方言 | 实现规范 ≠ 造轮子；换 CloudEvents 生态互操作 + 官方 TCK（§4.7） |

两层语法并存（CEL 表达式 + Python 过程代码）的取舍：两者各在自己最强的形态，且**都有海量训练语料**——比"一门新语言的两种模式"（语料为零）划算得多。

### 4.2 CEL 集成规范（谓词层）

- **语法原样**：不注册自定义函数、不加自定义语法。K8s 写什么，这里就写什么。
- **求值环境**：`payload`（codec 解码后的结构）、`meta`（消息元数据）与 `constants`（管道级常量）。
- **编译期检查**：cel-go 类型检查；有 payload schema 时启用静态字段检查（未知路径 error 或 warning，按 schema 严格度配置）。
- **错误契约**（对齐 CESQL 规范精神）：求值错误 = 该条件**不通过**（MUST NOT pass）+ 计数指标；不静默、不 panic。
- **性能**：谓词编译为 cel-go Program，一次编译逐消息求值——这是全引擎最高频的求值路径，交给编译型表达式语言正是分层的主要理由之一（§4.6）。

```yaml
# 边条件（CEL）
from: { enrich: { when: 'meta.region == "eu" && payload.total > 100' } }
```

### 4.3 Starlark 集成规范（映射层）

#### 形态与绑定

`transforms.<node>.script` 是**一段 Starlark 语句序列**（无需函数包装），引擎预绑定全局：

```yaml
transforms:
  enrich:
    from: [ingest]
    script: |
      payload.total = payload.price * payload.qty
      payload.label = "order-%s-%s" % (payload.id, meta.region)
      if payload.total > 10000:
          meta.tier = "vip"
      else:
          meta.tier = "basic"
```

- `payload` / `meta` / `constants` 是宿主绑定：`payload`/`meta` 为自定义 `starlark.Value`——**惰性绑定**（按字段访问把 Go 侧已解码结构转换为 Starlark 值，避免逐消息全量转换）+ **写时复制**（首次赋值才 clone，v2 的 parsedData/COW 设计直接复用），支持属性读写、下标与字典协议迭代；`constants` 为冻结值（只读）。
- 允许（且鼓励）定义 `def` 函数组织逻辑；复杂脚本可拆到独立 `.star` 文件由 `load()` 引入（同配置目录白名单内，M1 先支持管道内联，文件级 P1）。

#### 沙箱配置（全部为 go-starlark 现成机制，非魔改）

| 机制 | 配置 | 效果 |
|------|------|------|
| 递归 | `syntax.FileOptions.Recursion = false`（默认） | 禁递归 |
| `while` 循环 | `FileOptions.While = false`（默认） | 循环只能写有限序列的 `for`，保证终止 |
| 顶层控制流 | `FileOptions.TopLevelControl = true` | `script` 是语句序列而非函数体，顶层 `if/for` 必须可用（本节示例全部依赖它——实现前审查 R2 抓出的规范遗漏） |
| 步数预算 | `Thread.SetMaxExecutionSteps(n)`（默认 100k，可配） | 硬性计算上限——**既是安全阀，也是每消息 CPU/延迟上限** |
| 模块 | 宿主白名单加载：`json`、`math`（确定性子集）；go-starlark **没有**可加载的 `strings` 模块——字符串方法内建于 `string` 类型天然可用（实现前审查 R3）；**不加载** `time`/任何 I/O | 无网络、无文件、无时钟、无熵源 → 确定性可回放 |
| 冻结语义 | Starlark 语言内建 | 模块级全局不可变，多线程安全复用 |

#### 错误模型（Starlark 的"无异常"恰好是优点）

Starlark 规范明文：**语言内没有任何错误处理机制**——`fail()` 或任何运行时错误立即中止，错误带 backtrace 交给宿主。这把 v3 的错误契约变成自然映射：

- 求值错误 = 中止 = 该消息沿边 `delivery` 走重试 → 死信，**backtrace 自带行号**写入死信记录与 span；
- 不存在"错误被 catch 掉影响控制流"的确定性漏洞（这也是 Starlark 自己为确定性设计的性质）；
- 少量 `safe_` 前缀宿主糖函数（如 `safe_json_decode(s, default)`）提供"失败取默认值"的兜底——这是胶水函数，不是改语言。

#### 确定性

时钟/熵不可达（模块不加载）；时间与标识由引擎在**入口统一盖章**进 `meta.ingest_time` / `meta.message_id`（确定性 UUIDv7 由 spool 分配，回放保留原值）。因此 **test / simulate / replay / 生产对同一条输入逐字节一致**——关卡三成立的前提。

### 4.4 验证语义：两层各自能拦住什么

| 检查 | CEL（谓词） | Starlark（映射） |
|------|-------------|------------------|
| 语法错误 | ✅ 编译期 | ✅ 编译期 |
| 类型不符 | ✅ 编译期（有 schema 时含字段级） | ❌ 动态类型——由 test 关卡 + 运行时死信兜底 |
| 未定义名 / 函数 arity | ✅ | ✅（go-starlark resolver 编译期抓） |
| 禁用模块/函数引用 | — | ✅（宿主白名单 + lint） |
| 求值期失败 | → 条件不通过 + 计数 | → backtrace → 死信（行号可见） |

诚实声明：相比自研 eql2（或 VRL），**丢失了映射层的编译期 fallibility/类型检查**。这是"零自研语言"的明确代价，用三道防线换：verify 的 resolver/lint 检查 + test 关卡的 fixture 兜底 + at-least-once 语义下带行号的 死信（错误暴露时机从 verify 挪到 test/运行期，但不丢消息）。

### 4.5 转换阶梯（简化后）

```
CEL          谓词/路由条件（编译型表达式，最热路径零解释器税）
  ↓
Starlark     映射 + 过程逻辑（沙箱解释执行，覆盖 ~95% 场景）
  ↓
WASM / gRPC  重计算/任意语言/外部依赖（近原生，进程隔离）
```

相比 v1.0 提案（eql2 → Starlark → WASM 三档），前两档合并为一档语言体系，阶梯更短，心智更少。WASM/gRPC 的触发标准不是"逻辑复杂"（Starlark 已覆盖），而是 §4.6 的性能或依赖标准。

### 4.6 性能：天花板在哪、怎么对冲

**客观数据**：

- starlark-go 官方实现文档明说：**递归树遍历解释器**（无字节码/JIT）；
- 官方发布帖的非正式基准：单线程约为 **CPython 一半**；
- Go 嵌入式语言横评（Scriggo Fibonacci 35）参照系：Tengo 3.5s / GopherLua 6.5s / goja 7.1s / Yaegi 25s——starlark-go 大致在 GopherLua 一档，快于 JS/yaegi；
- cel-go / expr 这类编译型表达式语言，简单求值比任何脚本 VM 快约一个数量级。

**对本产品算账**：典型 transform 是"数据形状"脚本（10-30 条语句、字段读写、少量字符串/集合操作），估算**单条 2-10μs**，且大部分耗时在原生 Go 实现的 stdlib（解释器只负责调度）。单核 10-25 万次 transform/秒，worker 池多核线性扩展（Starlark 程序不可变可多线程并发执行，无 GIL）。v3 目标段管道为千～万级 msg/s——transform 阶段 CPU 占比个位数百分比；真正的大头是 codec 解析与 sink 网络 I/O。

**会真正撞墙的场景**：每条消息重计算（大 payload 上的循环/正则/加密，解释器税 10-100 倍）、脚本内 parse 数十 KB 以上 JSON、单管道 >20 万 msg/s 的高频小消息。

**六层对冲**（进设计，不进希望）：

1. **预编译**：`SourceProgram` 管道加载时编译一次，程序不可变、多线程复用——逐消息零解析成本；
2. **谓词/映射分层**：每条边 × 每条消息的 `when` 走 CEL 编译执行——最热路径不经过解释器；
3. **步数预算一 knob 两用**：`SetMaxExecutionSteps` 既是安全阀也是每消息 CPU 硬上限（最坏情况延迟有界可配），配 wall-clock context 取消；
4. **batch API**：Transform 批进批出，脚本一次调用处理一批，摊薄调用开销；
5. **惰性绑定 + COW**：自定义 Value 按字段访问转换（避免逐消息全量构建 Starlark dict）——这个转换成本大概率高于解释器本身，是首要优化点；
6. **可观测 + 逃生舱**：每步脚本耗时直方图、步数预算耗尽计数进指标（慢脚本生产可见）；重负载走 WASM（近原生）/gRPC 档。

**基准纪律**（M1 验收项）：conformance 语料附带三类脚本（简单/常规/重度）× 消息速率的基准套件，进 CI 回归门，数字写进文档供用户自行对照。

**终极兜底**：若某天真不够用，生态自带答案——Meta 的 starlark-rust（字节码编译器 + 静态类型检查，Buck2 同款）。但 CGO 边界与构建复杂度意味着必须先证明瓶颈再考虑；宿主接口预留替换缝即可。

### 4.7 CESQL 可选方言

`when` 支持 opt-in 的 CESQL 方言（CloudEvents 生态互操作）：

```yaml
- from: { enrich: { when: { lang: cesql, expr: "type = 'com.example.order' AND region = 'EU'" } } }
```

语义映射：CESQL 上下文属性 → `meta`；`data.*` 扩展路径可触达 `payload`（文档明示为规范扩展）；纯模式（仅上下文属性）跑官方 [CESQL TCK](https://github.com/cloudevents/spec/blob/main/cesql/README.md) 进 CI。主语法保持 CEL；CESQL 是互操作出口，不是主方言。

### 4.8 eql1 → v3 迁移（比 v1.0 提案大幅变好）

| eql1 | v3 | 迁移 |
|------|-----|------|
| 谓词 `payload.status == "paid"`（本就是 CEL） | CEL 原样 | **零迁移** |
| `payload.x = expr`（赋值） | `payload.x = expr`（Starlark 属性写，语法同形） | 自动 |
| `del(payload.x)` | `remove(payload, "x")`（宿主胶水函数） | 自动 |
| `format("%s-%d", a, b)` | `"%s-%d" % (a, b)` | 自动（模式可识别） |
| 嵌套三元多分支 | 直接写 `if/elif/else` | 自动（convert 直接展平） |
| `metadata.x` | `meta.x` | 自动 |
| route transform + `er-route` | 边 `when`（CEL） | 自动（生成边条件，输出评审清单） |
| eql1 注册的 snake_case 自定义函数 | Starlark stdlib / 宿主糖函数近似物 | 人工（convert 报告逐条） |

预估 95% 以上语句可自动迁移（谓词零成本 + 赋值同形是两大功臣）。

### 4.9 与备选方案对比

| 维度 | **v3：CEL+Starlark** | 自研 eql2（v1.0 提案） | Bloblang (Benthos) | VRL (Vector) | goja (JS) | Lua | jq (gojq) |
|------|---------------------|----------------------|--------------------|--------------|-----------|-----|-----------|
| 语言维护成本 | **零**（两个现成实现） | 高（编译器/conformance/文档全自建） | 高（Benthos 团队） | 高（Vector 团队） | 零 | 零 | 零 |
| 用户上手 | CEL≈K8s 经验；Starlark≈Python | 全新语法 | 新语法（设计优秀） | 新语法（Rust 风格） | 最低 | 低 | 中（复杂度上去后可读性崩） |
| Agent 语料 | **最大**（K8s CEL + Python 系） | 零（发布前） | 中 | 中 | 最大 | 大 | 大 |
| 编译期校验 | 谓词 ✅；映射部分（resolver+lint） | ✅（最强） | 部分 | ✅（fallibility 标杆） | ❌ | ❌ | ❌ |
| 终止性/沙箱 | ✅ 语言级内建 | ✅ 按设计 | 有限 | 有限 | 需中断机制拼装 | 需 debug hook 拼装 | 有限 |
| 确定性（可回放） | ✅（模块白名单+入口盖章） | ✅ | 弱 | 弱 | 弱 | 弱 | ✅（纯） |
| 映射性能 | 解释执行（§4.6 有数） | 自控上限 | 专用 VM | 编译到 Rust | 慢 | 中 | 中 |
| Go 嵌入成熟度 | cel-go（CNCF 级）+ go-starlark（Google，Bazel 系） | — | 不可嵌（自成体系） | 不可嵌（Rust） | 成熟 | 维护放缓 | 成熟 |

结论行：**没有现成语言能在"编译期安全 + 上手 + 语料 + 嵌入成本"全维度同时最优；CEL + Starlark 组合放弃唯一的"映射层编译期类型检查"，换零语言维护成本 + 最大语料 + 语言级沙箱——对这个体量的产品是正确的交换。**

---

## 5. 配置方式重设计

### 5.1 命名体系：按层级分区的语义环境 + 三段式拓扑

配置词汇按**层级**划分语义环境，每层只用该领域的标准词，不跨层混用：

| 层级 | 词汇 | 语义来源 |
|------|------|----------|
| 资源层 | `apiVersion` / `kind` / `metadata` | K8s 词汇（CRD 血统） |
| 执行策略层 | `run` / `parameters` / `constants` / `hooks` / `limits` | 作业面词汇（dagu 同位；limits 是 K8s 习惯词） |
| 拓扑层 | `sources` / `transforms` / `sinks` + `from` | 数据流词汇（Vector/Flume 谱系）+ 图词汇（边 = from→to） |
| 算子层 | `decoder` / `encoder` / `workers` / `order_key` / `batch` / `script`·`split`·`wasm` | 数据流词汇 |
| 边属性层 | `when` / `route` / `buffer` / `delivery` / `required` | 投递词汇（DeliverySpec 谱系） |
| 观测定制层 | `telemetry` | OTel 词汇（OTel 自身内部配置同名） |

**为什么是三段式（模式学依据）**。业界 DAG 描述可归为七种范式，按我们的两条硬约束筛选——①边必须是一等对象（per-edge `when`/`delivery` 是核心语义）排除状态机路由（Step Functions 的 Next 模式，入边隐式挂不了属性）、标签匹配（Fluentd，拓扑不可枚举）、线性+嵌套分支（Benthos switch，v1 病根）；②配置必须是数据（schema 校验、Agent 生成、verify）排除代码引用推断（Airflow/dbt/Temporal 一系）。剩下两种：

| 范式 | 代表 | 与产品类型的关系 |
|------|------|------------------|
| **P1 邻接表·单容器**（下游声明依赖） | GitHub Actions `needs`、Argo `dependencies`、Tekton `runAfter`+`when`、dagu `depends` | **作业面系统的通用语法**（任务是同质执行单元） |
| **P2 按角色分段**（引用连边）✅ | Vector `sources/transforms/sinks`+`inputs`、OTel `receivers/processors/exporters/connectors`、Flume | **数据面系统的主流语法**（组件是异质角色） |

我们是数据面、且作业语义不进拓扑（pipeline 级 `run`）→ **P2**。2026 年的行业分裂进一步佐证：作业面正全面 code-first 化（Temporal/Inngest/Restate/durable execution 浪潮），留守 config-driven 的恰是数据面工具。P2 的演进路径有实证：OTel 通过增加 `connectors` 段支持双角色组件——未来若做跨管道连接器，加段即可（纯加法，见开放问题 #8）。Tekton 的 `when` 挂在任务（入边守卫）上是与我们 `from: {x: {when}}` 最接近的先例。

**三段式同时完成的减负**（相对"单容器 + nodes/步骤名"方案）：

| 决定 | 内容 |
|------|------|
| 容器消灭 | 拓扑不再需要统一容器名（`steps`/`nodes` 的命名问题消失）；`node` 降级为概念词（explain/status/IR），不是配置键 |
| `type` + `config` 双包装消灭 | **插件名即键**：`kafka: {...}`、`sql: {...}`——少一层嵌套少一个冗余 token |
| 字段分层规则 | **node 层只有框架字段白名单**（`from`/`decoder`/`encoder`/`workers`/`order_key`/`batch` + transform 主字段），**插件块内只有插件字段、零保留字**——v2 式"保留字与插件字段撞名"结构性消失 |
| kind 结构化 | 所在段即类型；v2 的合体 step（transform+sink）**物理上无法表达**（病根切除）；代价：换类型要跨段移动（低频可接受） |
| transform 主字段互斥 | `script`（Starlark）/ `split` / `wasm`（未来）三选一，种类由结构表达 |

其余命名决定：`depends_on`→`from`（字段指"边"而非"组件"，与 explain 同语）；`transform.map`→`script`；`defaults`→`edge_defaults`；`run.mode: continuous | job` 二值；`observability`→`telemetry`；`on`→`hooks`（GH Actions 的 `on:` 是"何时触发"语义，撞形不同义）。**全称原则（v1.5，v1.6 细化）**：自造缩写一律展开——`params`→`parameters`、`consts`→`constants`（前身 `vars`）、`catchup`→`catchup_window`、`max_inflight`→`max_in_flight`、cron 源时间字段用 `expression`；MCP 工具与 CLI 标志跟随概念名（`--parameters`、`replay --dlq`、`dlq_query`）。**约定俗成的行业缩写保留**：`dlq`（Kafka Connect/RabbitMQ/SQS 生态通用）、`args`（命令行传统）、`dsn`。**保留的专有名词**（是名字不是缩写）：CEL、cron、MCP、OTLP、JSON、URL、codec、REPL。否决记录：`receivers/processors/exporters`（绑遥测语境）、`input/filter/output`（filter 名不副实、input 与 source 撞车）、`inputs` 边字段（指组件不指边，带属性时语义拧）、`junctions`/`stations`（单容器方案随 P1 一起落选）、`stages`/`tasks`（v2 术语掘墓/作业面撞车）。

### 5.2 决策总表

| 决策 | v2 | v3 | 理由 |
|------|----|----|------|
| 格式 | YAML + HOCON"对等" | **仅 YAML**（YAML 1.2，天然接受 JSON） | 双格式双维护且已分叉；CRD/CI/Agent 全都要 YAML |
| 拓扑结构 | steps 单容器 + 三套写法 | **三段式 `sources`/`transforms`/`sinks` + `from`**（§5.1 模式学） | 数据面主流语法；kind 结构化；容器命名问题消失 |
| 插件引用 | `type:` + `config:` 双层 | **插件名即键** | 少一层嵌套；字段分层规则消灭保留字冲突 |
| 合体 step | transform+sink 自动展开 | **无法表达**（结构杜绝） | 消灭隐藏 id；配置拓扑=运行时拓扑 |
| from 形态 | 序列/映射两种顶层 × 元素字符串/对象 | **两种元素形态**：`"name"` 或 `{name: {attrs}}` | 收敛 |
| route | transform 写 `er-route` + 边引用 | **边 `when` + 命名 route（糖）** | 静态可见、explain 可推演 |
| 谓词语言 | eql（CEL 魔改） | **CEL 原样**（§4.2） | K8s 标准、语料最大、编译期检查 |
| 映射语言 | eql（CEL+赋值缝合） | **Starlark**（§4.3） | Python 方言、语料最大、沙箱内建 |
| 术语 | step→stage→edge 三层 | **node（概念）+ edge**；配置按三段组织 | 一层心智模型 |
| 定时/批处理 | 无（cron source 只产固定 payload） | **pipeline 级 `run`**（§5.8） | 调度/补偿/重叠/历史是引擎职责，不进源插件 |
| 变量替换 | 部分字段、unset 行为分叉 | **全字段生效**；`${VAR}` unset=错误、`${?VAR}` 可选省略 | 严格且一致 |
| 环境差异 | 配置内散落 `${ENV}` | **base + overlay**（`verify a.yaml + prod.yaml`） | 同一管道多环境可 diff |
| 插件配置 | 手写文档 + 宽松解析 | **JSON Schema 注册**，严格校验，`plugin schema` 可取 | IDE/Agent/LSP 同源 |
| 默认值继承 | edgeDefaults→边→engine→transform 四层 | **两层**：`edge_defaults` → 边级 | 可追溯 |
| 资源 vs 运行时 | 混在管道配置里（engine/observability 段） | **分离**：Pipeline 资源只放"管道是什么"（`limits`/`telemetry` 仅本管道定制）；全局端点/存储路径/admin 端口在部署级配置文件 | CRD 血统的必然要求 |

### 5.3 唯一写法：三段式 + from

```yaml
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: orders }

edge_defaults:
  delivery: { retries: 3, backoff: exponential }

sources:
  ingest:
    decoder: json                          # 框架字段（node 层白名单）
    kafka:                                 # 插件名即键（插件字段只在此块内）
      brokers: ["${KAFKA_BROKERS}"]
      topics: [orders]

transforms:
  enrich:
    from: [ingest]                         # 边：字符串元素（无条件）
    script: |                              # 主字段：script | split | wasm 互斥
      payload.total = payload.price * payload.qty
      payload.label = "order-%s-%s" % (payload.id, meta.region)

sinks:
  eu-out:
    from: { enrich: { when: 'meta.region == "eu"' } }    # 边：对象元素（CEL 条件）
    encoder: json
    kafka: { topic: orders-eu }
  us-out:
    from: { enrich: { when: 'meta.region == "us"' } }
    http: { url: https://us-api.example.com/orders }
```

要点：

- 节点名跨三段全局唯一；`from` 可跨段引用任意上游（fan-in 混合引用合法：`from: [sourceA, transformB]`）。
- `from` 只接受两种元素形态：字符串（无条件边）或**单键对象**（键=上游名，值=边属性：`when`/`route`/`buffer`/`delivery`/`required` 全部可选）。
- 一条边 = `{from, to} + attrs`，`explain --topology` 输出的边与配置一一对应。
- 最小线性管道的地板（约 10 行）：三段各一个条目 + 两个 `from`——**DAG 的全部仪式感只剩 `from` 一行一条**，这是显式边的必要重量，不再减（隐式链式 = dagu chain 模式 = "两种写法"，已否决）。

### 5.4 route 显式化

```yaml
sinks:
  vip-out:
    from: { classify: { route: high-value } }    # 命名 route
```

`route: high-value` 是**编译期糖**：等价于 `when: 'meta.route == "high-value"'`，其中 `meta.route` 由上游 `classify` 节点的脚本写入——但不同于 v2，这个展开结果**在 verify 输出与 explain 里可见**（`route 'high-value' → compiled to when meta.route == "high-value"`），不再是隐式协议。上游必须存在对 `meta.route` 的赋值，否则 verify 报"悬空 route"。

### 5.5 变量替换与 overlay

- 替换适用于**所有字符串值**（修复 v2 盲区）；`${VAR}` 未设置 = verify 错误；`${?VAR}` 未设置 = 整个键省略。
- 作用域引用：`${parameters.name}`（作业参数，§5.8/§5.9）、`${constants.name}`（管道常量，§5.9）均可出现在任何字符串值中（含 `when`；脚本内直接读绑定名 `constants.x`/`parameters.x`）。
- overlay：`eventboat verify base.yaml + overlays/prod.yaml`。overlay 是补丁文档（map 深合并、list 整体替换、`<<remove>>` 标记删除），合并结果作为一个整体再走 verify——**永远不存在"未验证的合并结果"**。三段式让补丁路径更短更稳（`sinks.eu-out.kafka.topic` 而非 `nodes.eu-out.sink.config.topic`）。
- 密钥：配置里不落密钥；`config` 值支持 `${VAR}` 已覆盖 99% 场景；secret 引用（`secret://`）列 P2。

### 5.6 插件配置的 JSON Schema

每个插件注册时同时注册 schema（含字段类型、默认值、约束、描述文本）：

- Loader 校验两层：node 层框架字段按白名单枚举（未知框架字段=错误），插件块按插件 schema 严格校验（未知字段=错误）——**两套命名空间物理隔离，互不冲突**；
- `plugin list --json` / MCP `catalog` 输出全量 schema（按段分组，与 registry 同构）——**Agent 生成配置的依据**；
- 编辑器/LSP 复用同一 schema 做 YAML 内嵌段的补全与校验。

这是"Agent 不发明不存在的插件/字段"的机制性保证——v2 靠 SKILL.md 里一句"不要发明插件类型"的口头约定。

### 5.7 before / after 全景对照

v2（现状，含三套写法与隐式展开）：

```yaml
apiVersion: riverpod/v1
kind: Pipeline
metadata: { name: order-processing }
steps:
  kafka-in:
    source: { type: kafka, decoder: json, config: { brokers: ["${KAFKA_BROKERS}"], topics: [orders] } }
  enrich:
    depends_on: [kafka-in]
    transform:
      type: map
      config:
        dsl: |
          payload.total = payload.price * payload.quantity
  splitter:
    depends_on: [enrich]
    transform:
      type: route
      config: { routes: { eu: 'metadata.region == "eu"', us: 'metadata.region == "us"' } }
  eu-sink:
    depends_on:
      - splitter: { route: eu }
    sink: { type: kafka, encoder: json, config: { topic: orders-eu } }
  us-sink:
    depends_on:
      - splitter: { route: us }
    sink: { type: http, config: { url: "https://us-api.example.com/orders" } }
```

v3（同语义）：

```yaml
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: orders }
sources:
  ingest:
    decoder: json
    kafka: { brokers: ["${KAFKA_BROKERS}"], topics: [orders] }
transforms:
  enrich:
    from: [ingest]
    script: |
      payload.total = payload.price * payload.qty
sinks:
  eu-out:
    from: { enrich: { when: 'meta.region == "eu"' } }
    encoder: json
    kafka: { topic: orders-eu }
  us-out:
    from: { enrich: { when: 'meta.region == "us"' } }
    http: { url: https://us-api.example.com/orders }
```

差异：少一个 route transform step（路由回归边）、零隐式协议、`metadata`→`meta`、`type+config+dsl` 三层套娃消灭（插件名即键）。行数相当，但**每一个运行时行为都能在配置里指出来**，且两段脚本对任何懂 K8s CEL 和 Python 的人零学习成本。

### 5.8 作业管道：定时/触发式批处理（吸收 dagu 作业模型）

**问题**："每天凌晨 1 点查 MySQL（分页）→ 补齐字段 → 投递 Kafka"这类批式数据搬运，不能用"给每个源实现一遍调度"来解（污染源插件设计），也不能用"cron 源 + 查询 transform"来解（transform 是纯函数，沙箱禁止 I/O；且 source 无入边是铁律）。

**方案**：作业是 **pipeline 级**的一等运行形态（dagu 同位设计）。`run` 块出现在管道顶层时，该管道成为**作业管道**——整条管道按作业生命周期运行：

```yaml
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: orders-nightly-sync }

run:
  mode: job                  # continuous（默认，无此块）| job
  schedule: "0 1 * * *"      # job 可选；无 schedule = 仅手动/触发器启动
  overlap: skip              # 上一作业未结束时：skip（默认，计指标）| all（并行）| latest（取消旧的）
  catchup_window: 2h          # 停机错过调度的补偿窗口：窗口内补跑一次，窗口外跳过 + 告警
  skip_if_successful: true   # 同周期已成功则跳过
  retention: { history: 90d }

constants:
  source_system: nightly-sync
  default_region: unknown

parameters:                   # 作业参数：声明带类型；触发时传入，未传用 default
  from: { type: string, default: cursor }   # cursor / now 为引擎绑定值
  to:   { type: string, default: now }

hooks:                       # 生命周期钩子（dagu handler_on 同位；内联 sink 同样插件名即键）
  failure: { http: { url: "${ALERT_WEBHOOK}" } }

edge_defaults:
  delivery: { retries: 3, backoff: exponential }

sources:
  pull:
    sql:                     # 拉取型源：零调度字段
      driver: mysql
      dsn: ${MYSQL_DSN}
      query: |
        SELECT id, order_no, amount, region, updated_at
        FROM orders
        WHERE updated_at >= :from AND updated_at < :to
        ORDER BY updated_at, id
      args: { from: "${parameters.from}", to: "${parameters.to}" }
      cursor: { column: updated_at }            # 增量水位（见下）
      pagination: { key: [updated_at, id], page_size: 5000 }   # keyset 分页
      emit: row                                  # 每行一条消息（emit: page 配 transform.split）

transforms:
  enrich:
    from: [pull]
    script: |
      payload.source_system = constants.source_system
      if not payload.region:
          payload.region = constants.default_region
      payload.amount_txt = "%.2f" % payload.amount

sinks:
  out:
    from: [enrich]
    encoder: json
    order_key: 'payload.order_no'
    kafka:
      brokers: ["${KAFKA_BROKERS}"]
      topic: orders-sync
```

**语义要点**：

1. **职责切割**：调度、错过补偿、重叠策略、作业历史、参数绑定——全部由引擎在 pipeline 层实现一次；源插件只实现"按参数取下一批"（`capabilities: [pull]`：分页、水位、取尽信号）。verify 检查 `run.mode: job` 的所有源具备 pull 能力。
2. **作业生命周期**：`pending → running（源分页拉取 + DAG 执行）→ settling（在途消息 settle 完毕）→ success | partial（有死信）| failed | canceled`；每次运行产生一条**作业历史记录**（run-id、parameters、起止、行数/投递/死信计数），存同一 SQLite store，按 `run.retention` 清理。
3. **水位跟随 settle**（不变量，与 §6.2 同族）：`cursor` 只推进到"已 settle 消息的 max(cursor_column)"——凌晨跑到第 40 万行崩溃，重启（补偿窗口或手动触发）从上次水位续传，不重跑整夜。
4. **背压互动**：作业内分页受 spool 高水位节制（spool 高 → 暂停翻页），夜间大流量由 spool 吸收、Kafka sink 按自身节奏消费。
5. **回补（backfill）靠 parameters**：同一条管道，凌晨跑增量（默认 from=cursor），白天 `mcp trigger orders-nightly-sync --parameters '{"from":"2026-08-01","to":"2026-09-01"}'` 跑指定区间——参数进触发上下文，`args` 绑定被覆盖。constants 与 parameters 的完整语义对比见 §5.9。dagu rich parameters 的 `eval`（运行时执行命令取默认值）**不吸收**（违反纯函数边界）。
6. **"restart failed" = `replay --job <run-id>`**：按作业 run-id 回放该次运行的死信子集到任意 node——零新机制，dagu 对应能力的语义等价物。

**作业面 vs 数据面**的边界（为什么不直接用 dagu）：dagu 每步每运行执行一次、数据是命令输出经文件传递、重试=重跑整步；v3 逐行 settle + 水位续传 + 逐行 死信。**作业编排（何时跑/重叠/历史/参数）吸收自 dagu，逐消息可靠性是本产品的本分**。

### 5.9 constants 与 parameters：两种值的两种生命周期

两个段都是"具名值"，但生命周期、变更面、审计面完全不同——这是它们必须分开的原因：

| | `constants`（管道常量） | `parameters`（作业参数） |
|---|---|---|
| 何时定值 | 管道加载时 | 运行触发时（调度触发用 default；手动 `trigger --parameters` 覆盖；`cursor`/`now` 引擎绑定值起跑时解析） |
| 谁能改 | overlay 补丁（部署面） | 触发传参（操作面） |
| 进作业历史 | 不进 | **进**（每次运行实参随 run-id 记录——回补可审计："那次跑的是哪个区间"） |
| 合法范围 | 任何管道 | 仅 `run.mode: job`（否则 verify 错误） |
| 可见性 | 任何字符串 `${constants.x}`；CEL/Starlark `constants.x`（冻结只读） | 任何字符串 `${parameters.x}`（如 sql 源 `args` 绑定）；该次运行内脚本/谓词 `parameters.x`（运行内冻结，确定性不受影响） |
| 解决的问题 | 多处引用的阈值/默认值的**单一事实来源** + 按环境（overlay）可变 | **回补**：同一条管道平时跑增量、手动触发跑指定区间，不改配置 |

```yaml
constants:
  vip_threshold: 10000        # when 与脚本同时引用的阈值：改一处生效；EU 区 overlay 补成 8000
  default_region: unknown

parameters:
  from: { type: string, default: cursor }   # 凌晨定时跑 = 增量（cursor）
  to:   { type: string, default: now }      # 白天手动触发传区间 = 回补
```

**为什么不合并成一个段**：合并后要么"常量"变得能被一次 trigger 覆盖（可变面扩大——写 `vip_threshold` 的人不会想让它被某次运行改掉），要么参数无法按次传入（回补做不了）。dagu 同样把常量与参数分成两个段（它的键名用了缩写 consts/params），结论相同；v3 按全称原则写作 `constants`/`parameters`。

**parameters 的类型化声明**：`type`（string/integer/number/boolean）、`default`、`required`、`enum`、`pattern`、`min/max`——verify 期检查声明自洽与引用处类型匹配。dagu parameters 的 `eval`（运行时执行命令取默认值）不吸收（违反纯函数边界，§7.5）。

### 5.10 顶层段全景（含可选段）

| 段 | 必填 | 语义 | 备注 |
|----|------|------|------|
| `apiVersion` / `kind` / `metadata` | ✅ | 资源标识 | K8s 血统 |
| `sources` / `transforms` / `sinks` | ✅（至少一源一汇） | 拓扑三段 | §5.3 |
| `run` | 作业管道必填 | 作业策略（mode/schedule/overlap/catchup_window/skip_if_successful/retention） | §5.8 |
| `parameters` | 作业管道可选 | 作业参数声明 | §5.8/§5.9 |
| `constants` | 可选 | 只读常量（脚本/谓词可见） | §5.9 |
| `hooks` | 可选 | 生命周期钩子（failure/success） | 内联 sink；原 `on` 与 GH Actions 撞形不同义，改名 |
| `limits` | 可选 | 本管道资源上限（max_in_flight/drain_timeout/workers 总额） | K8s 习惯词 |
| `edge_defaults` | 可选 | 边属性默认值（唯一一层继承） | |
| `codecs` | 可选 | 命名 codec 复用（如带 schema 的 avro），`decoder`/`encoder` 按名引用 | v2 同名延续 |
| `dlq` | 可选 | 死信策略（retention 等）；死信存储本身是引擎机制不是 sink | 约定俗成缩写，保留 |
| `telemetry` | 可选 | 本管道遥测定制（额外标签/span 采样）；**全局端点在部署级配置** | 资源 vs 运行时分离 |

---

## 6. 架构设计

### 6.1 总体分层

```
 YAML(+overlay) ──loader──▶ Config(typed, versioned)
                                │
                          ◀─ verify ── (schema / 拓扑 / CEL+Starlark 编译 / 作业配置 / lint)
                                │
                          Static IR  = DAG(nodes,edges) + 编译产物(CEL Program / Starlark Program) + schema 摘要
                                │
        ┌───────────────────────┼────────────────────────────┐
        ▼                       ▼                            ▼
   Engine(per-pipeline supervisor)                      Admin/MCP/CLI
   spool → DAG 执行(settle 跟踪) → sinks                      ▲
        │                   │                                 │
        │             jobs(调度/准入/历史)                      │
        └── OTel(metrics/traces) ── Prometheus/OTLP ───────────┘
```

三层职责：**Config 层**管语法与合并；**Static IR 层**管一切可静态确定的真相（DAG、编译后的程序、类型——explain 在这层即可运行）；**Runtime 层**只消费 IR，不含任何"理解配置"的逻辑。分层依赖方向明文化（吸收 dagu 工程纪律）：**可变运行时状态只许存在于 engine/jobs/store，不得进入 config/ir；storage 不得被 service 反向依赖；HTTP/MCP handler 不得直连存储适配器**。

### 6.2 可靠性模型：spool + settle + checkpoint

替代 v2 的"per-edge 磁盘缓冲 + 跨边 refCount"：

```
source ──▶ [spool: 每管道一条 append-only 持久队列] ──▶ 内存 DAG 执行 ──▶ sinks
                    │                                      │
                    └── checkpoint(消费位点) ◀── settle 跟踪器 ◀── 各分支终态
```

- **入口持久化**：source 消息先写 spool（含解码后形态或原始字节+codec 标记、入口元数据 `ingest_time/message_id`），**落盘后才算接收**。崩溃恢复 = 从 checkpoint 重放 spool。
- **settle 跟踪**：每条消息的执行分支集合在 fan-out 时确定；每个分支到达终态（sink 成功 / 死信 / `required:false` 边失败且策略允许丢弃）后递减；归零 = 消息 **settled**。
- **checkpoint**：settled 消息的 spool 位点持久化 + source 位点提交（Kafka: consumer offset；file: 文件 offset；http_server: 无位可提交，spool 即真相；sql: 水位列，见 §5.8）。
- **投递语义**：sink 失败按边 `delivery`（retries×backoff、timeout）重试；耗尽 → **死信库**（存 spool 同一存储，含完整原始消息 + 错误 + Starlark backtrace/CEL 错误 + 重试史）；死信写入成功 = 该分支终态（settle）。
- **背压**：spool 高水位 → 暂停 source 拉取（对作业管道 = 暂停翻页）；spool 低水位恢复。node 间为有界 channel（内存级 per-edge buffer 保留为削峰参数，不再是可靠性机制）。

**可测试不变量**（每条一个专属测试，这是对 review-2026-08 的结构性回应；不变量集本身由 conformance 测试锁定，见 §6.9）：

1. spool 落盘成功之前，消息对 DAG 不可见；
2. settled 之前，checkpoint 不前进；
3. 任意时刻 kill -9，重启后从 checkpoint 重放的集合 ⊇ 未 settled 消息集合（at-least-once）；
4. 死信写入失败时消息不得 settle（死信本身带重试，死信不可用 = 管道降速而非丢消息）；
5. `required:false` 边的失败只影响自身分支，不阻塞其他分支 settle；
6. 同一消息重复投递到幂等 sink 的结果是安全的（文档化的用户责任 + `meta.message_id` 供幂等键使用）；
7. 拉取源水位只推进到已 settle 消息的 max(cursor_column)（作业管道续传正确性）。

**被删除的机制**（复杂度减法）：per-edge 磁盘缓冲、跨边 per-message refCount、磁盘 buffer 的三态（memory/disk/overflow）与 when_full 矩阵——全部由"入口持久化 + settle"覆盖，且语义只强不弱。

### 6.3 存储选型：不手写 WAL

| 候选 | 优势 | 劣势 | 结论 |
|------|------|------|------|
| **SQLite（modernc.org/sqlite，纯 Go）** | 单文件、WAL 模式成熟、**死信/spool/作业历史可用 SQL 直接查**、运维工具链免费、崩溃恢复久经考验 | 写吞吐上限（路由器规模足够：单机万级 msg/s 级别） | ✅ **推荐默认** |
| Pebble（CockroachDB 的 LSM） | 写吞吐高 | 专属 API，查死信要自写工具；运维面生 | 备选（高吞吐 profile） |
| 自研 WAL（v2 现状） | 完全可控 | torn write/CRC/恢复全要自己证——已被评审证伪 | ❌ 移除 |

spool 表设计要点：append-only 消息表 + checkpoint 表 + 死信表（索引：时间、node、原因、route）+ **作业历史表**（run-id、parameters、状态、计数）；`replay` 命令即 SQL 查询 + 重新注入；`--ephemeral` 标志可关闭持久化（本地开发/测试）。dagu 从 JSON 文件迁到 SQLite 的历史与 [SQLite 做 durable 执行引擎](https://www.morling.dev/blog/building-durable-execution-engine-with-sqlite/)的实践是这个选型的旁证。

### 6.4 背压与调度

- per-pipeline supervisor（panic 隔离、独立生命周期）；transform worker 池（`workers` 参数）；sink 单写入者 + 引擎拥有的批量器（继承 v2 正确决策：攒批归引擎，sink 只管写）。
- **顺序性**：废除 v2 "ordering: ordered 强制 max_in_flight=1" 的全管道全局开关，改为可选 `order_key`（CEL 表达式）：无 = 完全并发；有 = 按 key 哈希分片，片内有序。顺序语义在 explain 输出中明示。
- 多管道资源隔离：每管道 worker/inflight 上限独立（`limits` 段）；全局配额兜底（部署级配置）。

### 6.5 扩展体系

| 档 | 机制 | 交付节奏 |
|----|------|----------|
| 内置 Go 插件 | 编译时注册（继承 registry 模式）+ **强制 JSON Schema** | M1（sql 源 P1/M2） |
| 表达式/脚本 | CEL（谓词）+ Starlark（映射）宿主（§4） | M1 |
| transform: WASM（wazero） | 能力制沙箱；触发标准 = §4.6 性能/依赖需求 | M3 |
| source/sink: gRPC 进程外插件 | 协议带版本协商、health、schema 声明；任意语言实现 | M3 |

插件 ABI 版本化：`catalog` 输出带版本；配置引用的插件版本与运行时不符 = verify 错误（杜绝"文档说有、二进制没有"的 v2 WASM 式空壳问题——空壳不再可能，因为注册必须带 schema，schema 不存在 = verify 即失败）。

### 6.6 可观测性

- **OpenTelemetry SDK 为唯一底座**：metrics + traces 原生 OTLP 导出，Prometheus exposition 作为可选出口（修 v2 "只有 Prometheus 且 trace 是 noop"的缺口）。
- span 结构：`pipeline → source → node(每类) → edge(条件求值结果) → sink`；**Starlark 求值错误带 backtrace 行号、CEL 求值错误带表达式位置**，进 span 事件与死信记录。
- 指标集以"四道关卡可编程检查"为标准设计（每个 SLI 都能被 MCP `status` 取到），首版 ~25 个，宁缺毋滥（对照 v2 设计 30+ 实际 16 的教训：写下的 = 实现的）。含脚本专属指标（每步耗时直方图、步数预算耗尽计数）与作业专属指标（运行时长、行数、重叠跳过次数）。
- 配置分层：管道内 `telemetry` 段只做本管道定制（额外标签/span 采样）；全局端点与导出器在部署级配置文件（§5.10 资源 vs 运行时分离）。
- 通知：`hooks.*` 生命周期钩子（§5.8）覆盖作业告警；webhook 告警规则为 P2。

### 6.7 部署形态与 HA

- **主形态**：单二进制 + 一份配置目录。`run` 起全部管道；SIGHUP / `POST /admin/deploy` 触发"verify → drain 旧 → 起新"（per-pipeline 依次替换，未变更管道不动）。
- **K8s**：一份 Deployment + 探针（`/live`、`/ready`=spool 健康与管道就绪）+ ConfigMap 挂载 + reload API。**Operator 降到 P2**。
- **HA**：单活实例 per pipeline（spool 在本地盘）；故障 = 重调度 + 从 source 位点/spool 恢复（at-least-once 兜底）；作业管道靠补偿窗口（`catchup_window`）+ 水位续传。多活分片（按 `order_key` 分区）列 P2 非目标——诚实：v3 不承诺水平扩展，承诺单机可靠与快速故障恢复。

### 6.8 模块划分（新 internal/ 布局）

```
cmd/eventboat/      run verify test explain replay trigger jobs convert repl plugin mcp
internal/
  config/            typed config、loader、overlay 合并、env 替换
  ir/                静态 IR：DAG(nodes,edges)、编译产物、lint、explain 求值器
  lang/              语言宿主（零自研语言，全部是"胶水"）
    celhost/         CEL 谓词编译/求值（cel-go 封装）
    starhost/        Starlark 宿主：预编译、惰性绑定+COW payload、
                     沙箱(白名单/预算)、safe_ 糖函数、backtrace 提取
    conformance/     CEL/Starlark 行为语料 + lint 规则测试 + 基准套件
  engine/            supervisor、调度、settle 跟踪、背压、checkpoint
  jobs/              作业面：cron 调度、catchup_window 补偿、准入队列、run 生命周期与历史
  store/             spool + 死信 + 位点 + 作业历史（SQLite/Pebble 后端抽象）
  registry/          插件注册 + JSON Schema 清单（catalog 数据源，按三段分组）
  wasmhost/          wazero 宿主（M3）
  rpcplugin/         gRPC 插件协议（M3）
  obs/               OTel 封装（metrics/traces 导出）
  api/               OpenAPI 源（生成 Admin REST server 类型、TS client、MCP schema）
  mcp/               MCP server（tools 实现）
  admin/             Admin REST + SSE + 内嵌只读 UI
  testkit/           注入/捕获/时钟注入（testrunner 与 repl 共用）
  conformance/       管道语义规范的可执行测试（拓扑/delivery/作业语义，见 §6.9）
```

与 v2 的关键区别：`lang/` 与 `ir/`（含 explain）成为一等模块；`jobs/` 承载作业面（调度不进任何源插件）；`store` 取代 `buffer`（WAL）；`api/` + `mcp/` + `admin/` 构成操作面，**OpenAPI 单源生成**（server 类型 / TS client / MCP tools schema 三方同源，消除漂移——吸收 dagu 的 API-first 纪律）。

### 6.9 工程纪律（吸收自 dagu 的实现文化）

1. **规范驱动的 conformance 测试**：管道语义（拓扑规则、delivery/settle/checkpoint、作业生命周期、verify 行为本身）写成可执行规范 + 二进制级一致性测试，进 CI——v2 的"文档说一套、代码做一套"（review-2026-08 发现的口径脱节）从机制上杜绝。
2. **API-first 单源生成**：`api/` 的 OpenAPI 源生成 Go server 类型、前端 client、MCP schema；禁止手改生成物。
3. **分层依赖方向明文化**（§6.1）；bug fix 先写能失败的回归测试再修。

---

## 7. 与 v2 的对比、迁移与里程碑

### 7.1 逐领域对比

| 领域 | v2 | v3 |
|------|----|----|
| 定位 | DAG 事件路由器（Agent 优先是路线图项） | Agent 原生事件路由器（四道关卡即产品） |
| 配置 | YAML+HOCON、三套拓扑写法、合体展开、四层默认值 | 仅 YAML、**三段式 sources/transforms/sinks + from、插件名即键**、overlay、两层默认值 |
| 谓词语言 | eql（CEL 魔改） | **CEL 原样**（K8s 标准，零扩展） |
| 映射语言 | eql（CEL+赋值缝合） | **Starlark**（Python 方言，go-starlark 沙箱宿主） |
| 语言维护成本 | 自研方言（漂移中） | **零**（两个现成实现 + 胶水） |
| 定时/批处理 | 无（cron source 只产固定 payload） | **作业管道**：pipeline 级 `run`（schedule/overlap/catchup_window/retention）+ `parameters` 回补 + 作业历史 |
| 可靠性 | per-edge 磁盘 WAL + 跨边 refCount | 入口 spool + settle + checkpoint（不变量可枚举测试） |
| 存储 | 自研 WAL（分段+CRC+位点） | SQLite/Pebble（不手写 WAL） |
| 死信 | 死信 sink（只进不出） | 死信库 + `replay`（查询、筛选、按 run-id 回灌） |
| 推演 | 无 | `explain`（消息级路径推演）+ `simulate` 语义 |
| Agent 面 | Skill 文档 + CLI + Admin HTTP | MCP 一等公民 + Schema catalog + JSON 输出 + explain + trigger |
| 可观测 | Prometheus 16 指标、trace noop | OTel 原生（OTLP+Prometheus）、span 带脚本行号、SSE 实时态 |
| 扩展 | Go 编译注册；WASM 空壳；gRPC 推迟 | 注册强制 Schema；CEL/Starlark/WASM/gRPC 阶梯 |
| 部署 | 单二进制 + Operator 规划 | 单二进制 + 薄 K8s 清单；Operator P2；单活 HA |
| 质量纪律 | 文档/代码口径曾脱节 | conformance 测试锁语义 + API-first 生成 |

### 7.2 eql → v3 迁移对照速查

见 §4.8 表；迁移工具 `convert` 同时处理配置结构（§5.7 对照）与脚本（§4.8 对照），输出双清单（已自动迁移 / 需人工）+ 逐项 diff 报告。谓词零迁移 + 赋值同形使自动迁移覆盖率从 v1.0 提案估算的 ~90% 提升到 ~95% 以上。

### 7.3 `convert` 语义

**POC 阶段无迁移义务**：`convert` 是按需工具，不阻塞任何里程碑——v2 用户可继续留在归档的 v2 上，需要时再迁移。

- 输入：v2 `PipelineConfig`（任意一种写法）+ eql1 程序；
- 输出：v3 配置（三段式规范形态）+ CEL 谓词 + Starlark 脚本 + 迁移报告；
- 不可自动项（eql1 自定义函数近似物、HOCON 专属写法等）逐条列出原因与建议改法；
- `convert` 本身是纯函数，进 CI 做快照测试。

### 7.4 里程碑

| 阶段 | 内容 | 验收标准 |
|------|------|----------|
| **M1 内核与语言** | engine（spool/settle/checkpoint/背压）+ **CEL 谓词宿主 + Starlark 映射宿主**（预编译、惰性绑定+COW、沙箱白名单、步数预算、safe_ 糖函数、lint）+ verify/test + 内置 P0 插件（kafka/http_server/cron/file 源；kafka/http/file/drop 汇；json/raw codec）+ CLI（run/verify/test/repl/plugin） | §6.2 七条不变量各有一条专属测试且通过；**§4.6 基准套件（三类脚本 × 速率）进 CI 回归门，数字写进文档**；conformance 语料（CEL/Starlark 行为 + lint 规则）进 CI；`_examples` 全部 verify+test 通过 |
| **M2 作业管道与操作面** | **作业面（`run`/`parameters`/`hooks`/`catchup_window`/`overlap` + 作业历史 + `sql` 源 + `trigger`/`jobs` 命令）** + explain/replay + MCP server + Admin REST + SSE + 内嵌只读 UI + OTel 全量 | 一个 Agent 仅凭 MCP tools 完成"生成配置→verify→test→explain→deploy→观察→手动触发回补→修错→再部署"闭环（真实 Agent 会话录制验收）；作业中断续传（kill -9 后从水位续跑）有专项测试 |
| **M3 扩展阶梯** | WASM transform（wazero）+ gRPC source/sink 协议 + 插件 SDK 文档 + CESQL 方言（TCK 进 CI） | 第三方按文档实现一个 gRPC source 插件并跑通全链路；TCK 纯模式 100% 通过；基准证明 WASM 档对重度脚本的收益 |
| **M4 生态** | Schema 发布（`plugin schema` 独立分发）、LSP、csv/avro/protobuf codec、性能 profile（Pebble 后端）、Operator（薄封装）、convert 工具完善 | IDE 内写管道有补全与诊断（v2 示例 convert 仅作为 convert 工具自身的按需验收） |

排序理由：语言与可靠性内核是地基（M1）——零自研语言让 M1 的语言部分缩小为"宿主胶水"；M2 把"Agent 原生"与"作业管道"一起变成可验收的闭环（两者共享 trigger/parameters/history 机制）；M3 才谈扩展；M4 是生态放大器。

### 7.5 与 dagu 的关系（借鉴边界）

| 吸收 | 拒绝 |
|------|------|
| pipeline 级调度（schedule/overlap/catchup_window/skip_if_successful） | `${steps.<id>.outputs.x}` 步间输出引用——数据走控制流是作业面模型，与我们的边数据流相悖（v2 隐式依赖的病根） |
| rich parameters（type/default/required/enum/pattern） | dagu `params`/`preconditions` 的 `eval`（执行命令取值）——破坏纯函数与沙箱 |
| 生命周期钩子（`hooks.*` ← handler_on） | executors 全家桶（docker/k8s/ssh/mail/llm）——通用作业执行器不是本产品定位 |
| 作业历史 + retention | `graph\|chain` 双执行模式、嵌套 depends——配置复杂度反面教材 |
| "restart failed" → `replay --job` | human.task / approval 人工审批——超定位 |
| conformance 测试纪律、API-first 生成、分层依赖约束 | — |

根本差异：dagu 是**作业面编排器**（每步每运行执行一次，数据=命令输出经文件传递，重试=重跑整步）；v3 是**数据面路由器**（算子常驻，逐消息 settle/水位/死信）。凌晨 DB→Kafka 这类批式搬运 = **dagu 的作业编排语义 + 我们的逐消息可靠性**。

---

## 8. 命名决定：Eventboat

**定名 Eventboat**（事件船）。经六轮候选生成与逐个核查、最终三选一裁决（候选：Eventboat / Packetboat / 保持 Riverpod），2026-09-03 定稿。

### 8.1 方法论：命名铁三角

易读、独特 token、域名可注册——三者通常最多得二：易读的常用词域名全被占（Ferry：.io/.dev/.sh/.tools 四个全占）；独特的造词读不出来（Zaurak/Eridanus，用户直接否决"太难读"）。**"常用词 × 常用词"的复合词是唯一同时满足三者的解**：两个小学词汇拼成全球唯一的 token，读起来零障碍，域名整段空闲。

### 8.2 候选台账（六轮，2026-09 核查）

| 候选 | 判定 | 依据 |
|------|------|------|
| **Eventboat** | ✅ **选定** | 软件/GitHub 空间**零占用**；eventboat.io/.dev/.sh **全部未注册**；唯一同名是 eventboat.ch（瑞士马焦雷湖游船旅游公司，不同行业不同商标类别）；Agent 语境零先验 |
| Packetboat | ❌ | 语义最美（"packet/数据包"的词源正是邮政班船的包裹）但 packetboat.app 是活跃的开源文件传输客户端（相邻赛道）；10 字母；两词拼写歧义 |
| 保持 Riverpod | ❌ | §1.2-6 已论证：与 Flutter Riverpod 撞名 → Agent 语境污染，与"Agent 原生"定位自相矛盾；v3 未实现时是改名成本最低的窗口 |
| Ferry | 决赛圈 | 软件空间干净、易读满分、故事好；域名全占（变体 getferry.dev 可解但先天失分） |
| Sampan | 决赛圈 | 零占用 + sampan.io 可注册 + 中文词源彩蛋；"小船"意象压体量感 |
| Ferryman / Mailboat | 出局 | .dev 被占 / "mail"把心智锁死成邮件工具 |
| Shunter / Headgate / Culvert / Weir | 出局 | 用户判"不够酷"；Weir 拼读硬伤（类 weird） |
| Zaurak / Ister / Eridanus / Achernar | 出局 | 用户判"太难读"；Ister/Eridanus 域名亦被占 |
| Valeyard | 出局 | Doctor Who 反派，LLM 语境被 IP 占据（与 Riverpod≈Flutter 同构问题） |
| Cursa / Confluo / Alpheus / Kaikos / Strymon / Rivus / Plico | 出局 | 任天堂 IP / Berkeley RISE 数据系统 / 同域依赖图工具 / KaiOS 近形 / 知名效果器品牌 / 公司名拥挤（VC、电池、车队）/ 天文台框架 |
| Trawler / Hydrofoil / Riverboat / Schooner / Skiff / Kayak 等船族 | 出局 | 多重占用（Riverboat 被 theopenlane 基于 riverqueue 的 Go 作业队列占用，River 家族在 Go 生态已占满；Skiff 被 Notion 收购的邮箱产品占用） |

### 8.3 语义与用法

- 故事一句话：**"Eventboat 把事件从一岸运到另一岸"**——沿固定航线（DAG）多港停靠（fan-out），把事件卸到对的码头（sink）。水系谱系自然延续（EdgeStream → Eventboat）。
- 复合模式在英语里完全自然（同族真实词：mailboat / tugboat / fireboat / ferryboat）；中文团队"事件船"零障碍。
- 二进制名 `eventboat`；CLI：`eventboat verify / test / run / deploy`；指标前缀 `eventboat_*`；`apiVersion: eventboat/v3`；skills.sh 与 MCP 发布名同源。
- 判据复核：① 全球唯一可注册 ✅（域名/软件空间双空）② LLM 语境干净 ✅（零先验，发布后语料独占）③ 易读无歧义 ✅ ④ 隐喻贴分流/路由 ✅。

### 8.4 定名前置条件（不可逆动作前必须完成）

购域名/改仓库名等不可逆动作前：① 商标检索（软件类，美/欧/中）；② pkg.go.dev / npm / crates.io / PyPI 注册名核查；③ 三模型实测（"eventboat 是什么"确认零先验）；④ GitHub org `eventboat` 可用性。通过后执行：域名注册（io/dev/sh 全购）、仓库与 module path 更名（`github.com/eventboat/eventboat`）、README/文档/占位符全量替换（本文档已完成）。

---

## 9. 开放问题

1. **payload schema 的来源**：内联 JSON Schema 起步，是否接 Schema Registry（Confluent 兼容）？倾向 P2 观察。
2. **跨管道共享脚本**：`load()` 引用同目录 `.star` 文件的边界（哪些文件可 load、缓存与失效）——M1 仅管道内联，P1 设计文件级白名单。
3. **safe_ 糖函数最小集**：只保留 `safe_json_decode` 起步还是加 `safe_int/safe_float`？过多会重新发明 fallibility 语义，过少则逼用户写 `if` 预检——按示例反馈定。
4. **Starlark 惰性绑定的迭代一致性**：自定义 Value 惰性转换时，`payload.items()`/`for k in payload` 的物化时机与 COW 交互需 conformance 用例锁死。
5. **CESQL 扩展 `data.*` 的合规表述**：扩展模式下不能宣称"CESQL 兼容"，措辞需为"CESQL 子集 + 文档化扩展"；纯模式才挂 TCK 徽章。
6. **spool 的原始字节 vs 解码形态**：存原始字节 + codec 标记（回放兼容 codec 升级）vs 存解码后形态（回放快）——倾向前者，基准验证。
7. **Starlark 性能的终极兜底**：starlark-rust 替换缝的接口形态——留到基准证明瓶颈后再设计。
8. **跨管道触发与连接器**：`sink: {type: pipeline}`（dagu `dag.run` 对应物）或 OTel 式 `connectors` 第四段（P2）——三段式结构已预留加段演进路径。
9. **catchup_window 语义细节**：错过多个调度窗口时补跑几次（建议最多一次，带告警）；窗口内作业仍在 running 时的行为。
10. **部署级配置文件的形态**：全局遥测端点/admin 端口/存储路径的文件命名与结构（资源 vs 运行时分离的另一侧，§5.10）。
11. ~~License~~ **已定（2026-09-03）：Apache-2.0**——含专利授权，利于生态与未来商业化选项；仓库 `LICENSE` 文件与两份 README 已同步。
12. ~~v2 代码的存续策略~~ **已定（2026-09-03）**：POC 阶段全新实现、**不向后兼容 v2**——v2 代码整体归档（`legacy/`），不导入不修改；`convert` 工具按需，不阻塞任何里程碑。

---

## 附：一句话总结

v2 证明了"Go 单二进制 DAG 路由器"这个物种 viable；v3 要证明的是下一件事：**把整个产品做成 Agent 能端到端负责的系统——Agent 负责生成（用它们最熟悉的 CEL 和 Python），机器负责证明，引擎负责不丢消息；定时与触发的批式搬运，用同一条管道、同一套验证、同一个数据面。**

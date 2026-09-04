# redesign-v3.md M4 范围独立设计审查报告（实现前关卡）

日期：2026-09-04
评审对象：`redesign-v3.md`（v1.13）的 M4 范围——§7.4 M4（Schema 独立分发、LSP、csv/avro/protobuf codec、Pebble profile、Operator、convert 完善），对照 M1+M2+M3 实现（dba587d）与三份前序审查的已记档决策。任务书给定两个验收锚点：**IDE 内写管道有补全与诊断**；**v2 精选示例 convert 后全绿**——其余项按优先级实现，允许裁剪记档。
评审方法：全文细读 M4 相关章节 + 依赖假设逐项对照 pkg.go.dev / 官方仓库 / Go module proxy 实测核查 + legacy v2 全部示例与 loader/eql 源码逐行核对（convert 的输入形态全集以此为准）。

## 总体结论

**通过，无阻塞项。** 可以进入实现。

- **无 🔴 阻塞项**；**6 项 🟡 中**（R1–R6：convert 的 legacy 复用方式、eql1 渲染器的子集边界与机检闭环、route/filter 的边守卫转换、`codecs:` 段必须在 M4 落地、registry codec 注册面扩展、LSP 手写最小 JSON-RPC 选型）；**10 项 🔵 低**。
- 两个非锚点整项评估结论：**Pebble profile 裁剪记档**（R14）；**Operator 降为示例 Deployment 清单 + 文档**（R14，任务书预授权的形态）。
- `repl` 评估为低成本（一次性求值 + 行循环，复用 starhost/celhost），带上（R15）。

---

## 一、依赖与选型核查（对照 pkg.go.dev / 官方仓库，2026-09-04）

| 假设 | 核查结果 | 判定 |
|---|---|---|
| LSP：go.lsp.dev/jsonrpc2 + protocol | jsonrpc2（github.com/go-language-server/jsonrpc2）2026-06-27 仍有发布，定位已更新为"LSP + MCP 的 JSON-RPC 2.0 实现"，**活跃**；但其姊妹包 go.lsp.dev/protocol 现实现 LSP 3.18、**要求 Go ≥1.26**——本模块 go 1.25.0，protocol 包不可用。单用 jsonrpc2 而不用 protocol，意味着 LSP 类型（InitializeParams/CompletionItem/PublishDiagnosticsParams……）仍需手写，此时该依赖的剩余价值只有帧解析与并发管道（Content-Length 帧 ~30 行） | ✅ **裁决 R6：手写最小 JSON-RPC 2.0**（零新增依赖；与 M3 "只依赖保守面"的纪律一致）。手写范围见 R6 详述 |
| avro：hamba/avro vs linkedin/goavro | goavro README 自述**维护模式**："LinkedIn 内部已迁移到 hamba/avro，大规模场景显著更快"。hamba/avro（v2，`github.com/hamba/avro/v2`）活跃维护，声明支持最近两个 Go 版本，API 面覆盖 generic（`map[string]any`）编解码 | ✅ **选型：hamba/avro/v2**（新项目按维护方自己的建议） |
| protobuf：动态 schema | `google.golang.org/protobuf v1.36.12` 已在模块图（M3 gRPC 引入），`protodesc` + `dynamicpb` + `protojson` 全在官方面内——**零新增依赖**。动态 schema 处理面：配置给 FileDescriptorSet 二进制文件（`protoc --descriptor_set_out` 产物）+ 消息全名；decode = proto.Unmarshal 到 dynamicpb 消息 → protojson → `map[string]any`；encode 反向。双重 JSON 转换的代价在文档如实记录（protobuf codec 不是热路径定位） | ✅ 官方面实现，descriptor .pb 产物入库（同 wasm guest 纪律：CI 可从 .proto 重建） |
| csv | 标准库 `encoding/csv`（引号/转义/CRLF 全覆盖），无需三方库 | ✅ 自写（任务书预判一致） |
| HOCON（convert 输入） | `github.com/gurkankaymak/hocon v1.2.23`——legacy v2 用的同一库（legacy/go.mod 实证），公开库可正常依赖；其 CRLF 前处理 workaround（legacy hocon.go:17-21）一并复制 | ✅ 引入该依赖（仅 convert 用） |
| eql1 解析器 | legacy 的 eql 是 "CEL + 两个正则语句形式"（assignRe/delRe），核心是 cel-go 编译。**不可导入**（`legacy/internal/...` 是独立模块 `github.com/riverpod/riverpod` 的 internal 包，Go 可见性规则禁止），且 legacy 用 cel-go v0.22（`github.com/google/cel-go`），v3 用 `cel.dev/cel-go v0.32`（模块路径都不同） | ✅ 裁决 R1：复制 ~80 行语句分派 + 用 v3 celhost 同源 cel-go 做 AST 渲染 |
| vscode 扩展 | 最小形态 = 源码目录（package.json + extension.js + README），`code --extensionDevelopmentPath` 激活；依赖 `vscode-languageclient`（npm，本地 `npm install`，不发布 marketplace） | ✅ 源码交付，不入 CI（无 VS Code 运行时；LSP 协议层集成测试是 CI 侧验收） |

**结论：新增依赖仅两个——hamba/avro/v2（codec）与 gurkankaymak/hocon（convert 只读输入）；LSP 零新增。**

---

## 二、问题清单

严重度：🔴 阻塞 / 🟡 中（实现必须处理，不改设计结论）/ 🔵 低（缺口补决策）。

### R1 🟡 convert 不能复用 legacy loader 代码——只读复制 v2 形状进 v3

- **证据**：任务书问"legacy/ 下的 v2 loader 能否作为只读解析库复用"。结论**不能**：`legacy/internal/config` 属于独立模块 `github.com/riverpod/riverpod` 的 internal 包，`github.com/eventboat/eventboat` 依法不可导入（Go internal 可见性规则；这不是"改 legacy"能解决的——模块边界本身禁止）。同理 eql 包（`legacy/internal/eql`）与 HOCON 映射（`legacy/internal/config/hocon.go`）。
- **裁决**：在 `internal/convert/v2config/` **只读复制** v2 的形状知识：PipelineConfig/StepConfig/StageConfig/DependsOnList 等 struct（yaml 标签逐字照搬）+ depends_on 三形态 Unmarshal + steps/pipeline[]/edges 三写法的规范化语义（NormalizeSteps 的展开规则：合体 step → `N` + `N-sink` 隐式边；top-level edges 覆盖 depends_on 同键边）。HOCON 用同一公开库 gurkankaymak/hocon 重写 ~100 行映射（照 legacy hocon.go 的语义）。**legacy/ 目录零改动**（纪律不变）。复制件标注来源行号，便于对账。
- **诚实声明**：这意味着 v2 形状知识存在一份 v3 侧拷贝。可接受：convert 是按需工具（§7.3），v2 已冻结归档，拷贝不会再漂移。

### R2 🟡 eql1→Starlark：AST 渲染器走"子集 + 机检闭环"，不做完整翻译器

- **证据**：§4.8 承诺 95%+ 语句自动迁移，逐条映射表已定（谓词零迁移、赋值同形、del→remove、format→%、三元展平、metadata→meta、route→边 when、自定义函数人工）。 eql1 的 RHS 是合法 CEL（v2 编译期保证），v3 已有 celhost（同源 cel-go v0.32）——可以把 RHS 解析成 AST 再渲染成 Starlark，而不是纯正则。但 CEL→Starlark 不是全同构：`/` 整除语义（截断 vs floor）、宏（has/all/exists）、字符串方法（contains/startsWith 的调用形态）、`in` 等。
- **裁决**：**语句层正则分派（照抄 eql1 的 assignRe/delRe）+ RHS 经 cel-go 解析为 AST + 受控渲染器**。渲染器支持子集：字面量（int/uint/float/string/bool/null）、标识符、成员访问（`a.b` 同形）、下标（`a["k"]`/`a[i]` 同形）、一元/二元运算符（`&& || ! == != < <= > >= + - * %`；`/` 渲染为 `//` 并在报告标注负数语义差异）、三元（语句顶层 → if/elif/else 展平，嵌套层 → Starlark 条件表达式 `a if c else b`）、列表/字典字面量、函数映射表（`size`→`len`；`string`→`str`；`startsWith/endsWith`→`startswith/endswith`）。**子集外的任何节点 → 该语句进报告**（原因 + 建议改法），绝不猜。
- **机检闭环**（关键纪律）：convert 对每条产出物当场编译——script 用 `starhost.Compile`、when 用 celhost（经 ir.Build 全量 verify）——编译不过的语句降级进报告。**"自动迁移"的定义 = 生成且机器验证通过**，不是"生成了就算"。
- **`remove()` 缺口**：§4.8 映射表引用 `remove(payload, "x")` 宿主胶水函数，M1 实现只加了 `safe_json_decode`。本里程碑补上（starhost 新增 `remove(dict, key)` 薄胶水，与 safe_ 家族同纪律）。

### R3 🟡 route transform 与 filter transform 的转换：折叠为边守卫，语义差异记档

- **证据**：v2 的 route transform 是"隐式协议"（写 `metadata["er-route"]`，边 route 属性展开为条件）；v3 规范形态就是边 `when`（§5.7 对照明言"少一个 route transform step"）。filter transform 在 v3 无对应节点——v3 的过滤即边 `when`。
- **裁决**：
  - **filter 节点消失**：其下游每条边的条件 = `P && C_i`（P 为 filter 谓词，C_i 为该边原条件；无则即 P）。拓扑收缩 1 节点，报告逐条记录。
  - **route 节点消失**：route_order 顺序（显式或字典序 + `_default` 殿后）按 v2 first-match 语义编译为**有序边守卫**：第 i 条路由的边条件 = `p_i && !(p_1 || … || p_{i-1})`；无 route 属性的普通边 = `(p_1 || … || p_n)`（存在字面量 `true` 的路由时化简为 true）。谓词保持 CEL 原文（仅 `metadata.`→`meta.` 前缀改写），不经 Starlark。
  - **语义差异（诚实记档）**：v2 无匹配路由 → 消息静默丢弃；v3 全部出边不匹配 → settle 为 filtered + `eventboat_fanout_no_match_total` 计数（可观测性变好，消息结局相同）。v2 脚本若引用 `metadata["er-route"]`，报告提示人工确认。

### R4 🟡 `codecs:` 命名声明段必须在 M4 落地（任务书开放点的裁决）

- **证据**：§5.10 列 `codecs:` 为可选段（"命名 codec 复用（如带 schema 的 avro），decoder/encoder 按名引用"），M1 裁剪记档。但现状 `decoder: json` 是**纯名字符串**，registry 的 `NewCodec(name, nil)` 恒传 nil 配置——avro/protobuf/csv 全部需要携带 schema 配置，**没有 codecs: 段就没有任何途径给 codec 传配置**。任务书问"M4 是否随之落地"：不是可选项，是新 codec 的前置依赖。
- **裁决**：落地，形状：
  ```yaml
  codecs:
    orders-avro:            # 命名声明：名字 → 类型 + 配置
      type: avro
      schema: |
        {...avro JSON schema...}
  sources:
    ingest:
      decoder: orders-avro  # 按名引用（名字空间：命名声明 ∪ 内置 codec 名）
  ```
  - loader：`codecs:` 进顶层白名单，`map[name]→{type, ...其余字段即该 codec 的配置}`；`decoder`/`encoder` 保持字符串语义不变。
  - **遮蔽禁令**：命名声明与内置 codec 名冲突（如声明 `json:`）= verify 错误（`cfg_codec_shadow`）——两个名字空间不静默覆盖。
  - verify：命名声明解析 type → `codec_unknown`；配置对照该 codec 的 JSON Schema 严格校验（与 source/sink 同诊断格式 `plugin_schema`）。这要求 registry 的 codec 注册面升级（R5）。
  - engine：`e.codec(name)` 先查命名声明（携带配置实例化），再落内置。默认 `json` 行为不变。

### R5 🟡 registry codec 注册面：补 schema 与版本（内部 API 破坏性变更）

- **证据**：`RegisterCodec(name, factory)` 无 schema 无版本（对比 `RegisterSource(name, version, schema, capabilities, factory)`）；`Catalog().Codecs` 是 `[]string` 纯名字。builtin/codecs.go 里的 `jsonCodecSchema`/`rawCodecSchema` 常量已是死文档（M1 裁剪的痕迹）。§6.5 的原则是"注册必须带 schema"（防 v2 WASM 式空壳），codec 面当时例外了。
- **裁决**：`RegisterCodec(name string, version int, schema string, factory ...)`；`Catalog().Codecs` 升为 `[]CodecMeta{Name, Version, Schema}`。破坏面：builtin 注册点、若干测试、`plugin catalog` 输出与 MCP `catalog` 工具的 JSON 形状（codecs 从字符串数组变对象数组）——全部随本变更同步，`--json` 消费者（Agent/CI）记档该形状变化。外部 gRPC 插件不带 codec 的 M3 裁剪不变。

### R6 🟡 LSP 实现选型：手写最小 JSON-RPC 2.0 + 8 方法；数据源零新逻辑

- **证据**：见第一节——go.lsp.dev/protocol 要求 Go 1.26 不可用；单用 jsonrpc2 仍需手写全部 LSP 类型，依赖净收益趋零。LSP 侧需要的全部能力：stdio 上的 Content-Length 帧 JSON-RPC 2.0（请求/响应/通知/server→client 通知），方法集 = `initialize/initialized/shutdown/exit` + `textDocument/didOpen|didChange|didClose` + `textDocument/completion|hover`，server→client 仅 `textDocument/publishDiagnostics`。
- **裁决**：`internal/lsp` 手写（约 300 行帧 + 分派 + 类型），`eventboat lsp` 子命令以 stdio 运行。**零新逻辑原则的落法**：
  - **诊断** = 文档内容 → `config.LoadBytes` + `ir.Build`（与 CLI verify / MCP verify 逐字节同路径——ops.Verify 同一函数体），`Diagnostic.Line`（1 基）→ LSP 0 基 range；didChange 触发重编（verify 本为静态快路径，无需 worker 进程）。
  - **补全** = 按缩进/光标所在段推导上下文（顶层段 → 节点块 → 插件块 → from 对象），数据源全部现成：节点框架字段白名单（config.sections.go）、registry catalog（按段分组的插件名）、插件 JSON Schema 的 properties（名/类型/描述）、`codecs:` 命名声明（R4 落地后含 codec 名）。启发式基于行缩进扫描（yaml.v3 容错解析辅助），YAML 半成品上工作。
  - **hover** = 光标下 token：插件名键 → kind/version/字段摘要；插件块内字段 → schema description；框架字段 → 固定描述表。返回 markdown。
- **测试**：进程内 io.Pipe 双工直接驱动 server（不 spawn 二进制）：initialize 握手 → didOpen 坏配置 → 断言 publishDiagnostics 往返（错误码/行号）→ didChange 修复 → 断言诊断清空 → completion 请求断言插件名集合 → hover 断言描述。CLI 冒烟另测（`eventboat lsp` 能启动并应答 initialize）。
- **vscode 扩展**（最小形态）：`examples/editors/vscode/`——package.json（documentSelector: yaml + eventboat 配置项 `eventboat.path` 指向二进制）+ extension.js（vscode-languageclient 启动 stdio server）+ 语言配置片段 + README（`npm install && code --extensionDevelopmentPath=.`）。不发布 marketplace（任务书豁免）。

### R7 🔵 protobuf/csv 的文件路径与行内 schema：相对管道文件解析

- avro `schema` 与 csv `columns` 行内（YAML 值）；protobuf 的 `descriptor_set` 是文件路径，**相对管道文件目录**解析（与 wasm `module` 同规则；engine 经 IR.Config.File 拿目录）。快照/示例的 .pb 产物入库，CI 可从 .proto 重建（同 wasm guest 双轨纪律）。

### R8 🔵 convert 的 v2→v3 字段映射全集（任务书"§4.8 表逐条实现"的配置侧对应物）

| v2 | v3 | 处置 |
|---|---|---|
| `steps:` / `pipeline[]` / 顶层 `edges:` | 三段式 + `from` | 自动（三写法全解析；edges: 覆盖语义照搬） |
| `depends_on`（序列/映射/单键对象元素） | `from`（字符串/单键对象元素） | 自动（`condition`→`when`，`metadata.`→`meta.`） |
| `source:{type,decoder,config}` | `sources.<n>:{decoder,<plugin>:config}` | 自动 |
| 合体 step（transform+sink） | transform + `<n>-sink` 显式 sink | 自动（隐式边显式化） |
| `transform.map.dsl` | `script`（R2 渲染器） | 自动 + 机检 |
| `transform.filter` / `transform.route` | 边 `when` 守卫（R3） | 自动 + 报告 |
| `metadata:`（eql 内） | `meta.` | 自动（前缀改写） |
| `del(path)` | `remove(root, "leaf")` | 自动（starhost 补胶水） |
| `engine.max_inflight` | `limits.max_in_flight` | 自动 |
| `engine.drain_timeout` | `limits.drain_timeout` | 自动 |
| `edgeDefaults` | `edge_defaults`（buffer size→max_events；`required` 同名） | 自动（字段级差异见下） |
| `delivery:{retry:{max,backoff},timeout}` | `delivery:{retries,backoff,timeout_ms}` | 自动（5s→5000） |
| v2 `codecs:`（name/type/config + `ref` 引用） | v3 `codecs:` 命名声明（R4 形状） | 自动（第三步落地后） |
| `apiVersion/kind/metadata` | 同形（`eventboat/v3`） | 自动 |
| `engine.max_workers` / `error_mode` | 无对应（v3 无全局 worker/error_mode） | **报告**（建议逐节点 `workers:`） |
| `engine` 其余、`observability:` | Runtime 部署配置 | **报告**（提示 kind: Runtime 形态，不自动生成） |
| `dlq:` 段 + 无入边 dlq-sink step | v3 死信库（store 机制）+ `replay --dlq` | **自动删除 + 报告**（dlq-sink 节点移除） |
| `delivery.dlq: <sink>` | 无对应（死信进库） | **报告** |
| `buffer.strategy: drop_newest`、disk 型 buffer | 无对应（v3 内存 buffer 恒阻塞 + spool 兜底） | **报告** |
| sink `ordering: ordered` / `max_in_flight` | `order_key`（语义不同：按键分片序非全序） | **报告**（建议 order_key 业务键） |
| `batch.max_bytes` | v3 Batch 无此字段 | **报告**（丢弃） |
| cron `schedule`（6 字段含秒）+ `timezone` | `expression`（5 字段） | 自动去秒字段；**timezone 报告**（v3 cron 无时区配置） |
| http sink `method` | v3 恒 POST | `POST` 静默丢弃；其他值**报告** |
| eql `now()` 自定义函数、格式化函数等 | Starlark stdlib / 宿主糖 | **报告**（逐条，§4.8 "人工"行） |

- **输出**：`eventboat convert <v2-config> [-o out.yaml] [--report report.md]`。无 `-o` 打印到 stdout；`--report` 缺省时报告随 stdout 尾部输出。纯函数、确定性（节点排序按 v2 拓扑依赖序而非 map 迭代序），快照测试进 CI。
- **验收**：`legacy/_examples` 全部 8 例（三写法 + HOCON）+ `multi-pipeline/` 2 例 + `testdata/pipelines/linear.{yaml,conf}` —— convert 后 `eventboat verify` 全绿（需要 env 的示例在测试内 t.Setenv）。语义等价人工抽查三例记入收官报告。
- **（收官对账，2026-09-04）三例抽查已转正为永久测试** `internal/convert/semantic_equivalence_test.go`：① 01 线性——filter 折叠后 total>20 的守卫与 v2 filter 对同一脚本产物判定一致（50 过 / 10 滤）；② 02 路由分支——三个有序守卫对 us/eu/apac 的匹配模式与 v2 route first-match 逐 sink 一致（每 region 恰一汇命中）；③ 06 边投递——retries=3/backoff=exponential/timeout_ms=5000 数值等价，analytics 边 required:false，dlq-sink 与 dlq 段消失（死信入库）。12/12 fixture 验收全绿，快照入 CI。

### R9 🔵 Schema 独立分发

- `eventboat plugin schema <name>`：跨三段查名（sources/sinks/codecs），文本模式 = 头部（kind/name/version）+ pretty schema；`--json` = `[{kind,name,version,schema}]`。
- `--all --dir schemas/`：批量导出 `schemas/<kind>/<name>.json`（供 IDE/LSP/Agent 离线消费；正文即 schema，文件名可预测）。外部 gRPC 插件不在其列（manifest 属于插件仓）。

### R10 🔵 Catalog JSON 形状变化记档

- `plugin catalog --json` 与 MCP `catalog` 的 `codecs` 字段从 `["json","raw"]` 变 `[{"name":…,"version":…,"schema":…}]`。README 决策账本记档（Agent 消费方需要同步解析）。

### R11 🔵 LSP 与 overlay/多文件

- LSP 单文档即完整管道（v3 一管道一文件，overlay 属 verify CLI 组合用法）——didOpen 的文档独立验证，不做跨文件 workspace 支持（记档裁剪）。

### R12 🔵 `plugin schema` 名字冲突

- 不同段理论可重名（如源与 codec 同名）。schema 导出按段限定；CLI 单名查询命中多段时列出全部并标注 kind。

### R13 🔵 打磨项确认

- **看门狗日志穿透 node 名**：`nodes.go:27` 的 NewInvoker 传入的 logf 包一层 `[node <name>]` 前缀（一行跟进，M3 审核 B 裁决遗留）。
- **`eventboat repl`**：§3.6 表在册。最小实现：`--cel 'expr'` / `--script f.star` 对 `--message sample.json` 一次性求值（打印 bool/值或脚本后 payload/meta）；无参时交互行循环（Starlark 语句累积执行，`cel:` 前缀走 CEL，`payload`/`meta` 跨行保持）。复用 starhost/celhost + testkit 无新逻辑，~150 行——**带上**。JSON 输出 `--json` 跟随全局约定。

### R14 🔵 非锚点整项评估：Pebble 裁剪；Operator 降为清单 + 文档

- **Pebble profile**：store 抽象存在，但 replay/dlq_query/作业历史的查询面全是 SQL（`--where` 过滤、run-id 关联、位点半读）——Pebble 后端意味着重写一套 KV 索引查询引擎，工作量 ≈ 半个 M2，且非验收锚点、POC 无吞吐压力（SQLite 单机万级 msg/s 已达标）。**裁剪记档**。
- **Operator**：v3 主形态 = 单二进制 + Deployment（§6.7），Operator 本就 P2。交付 `examples/k8s/deployment.yaml`（探针 /live /ready、ConfigMap 挂载、reload 说明）+ docs/k8s.md 一页。**裁剪为清单 + 文档**（任务书预授权形态）。

### R15 🔵 convert 报告格式

- Markdown：头部摘要（自动/人工计数、verify 结论）；每输入一节：结构变换清单（route/filter 折叠、合体展开、dlq 删除、edges 覆盖）+ 语句级对照表（原文 → 产物 | auto/manual）+ 人工项卡片（**原因 + 建议改法**，§7.3 措辞）。人工项卡片必须可独立阅读（不依赖 diff 上下文）。

### R16 🔵 CI

- convert 快照与验收、LSP 协议测试、codec conformance 全部是普通 go test——现有 CI job 零改动吸收。CI 注释中 go 版本说明（"1.23 matches go.mod"）与实际 go.mod（1.25.0）漂移，顺手修正为说明性文字。

---

## 三、实现顺序与里程碑对账

任务书顺序即实现顺序，每步一个绿 commit：

1. **convert**（锚点二）：v2config 只读解析 + eql 渲染器 + 拓扑映射 + 报告 + 快照/验收测试 + starhost `remove()`。
2. **LSP**（锚点一）：internal/lsp + `eventboat lsp` + 协议集成测试 + vscode 最小扩展 + docs。
3. **codec 三件 + codecs: 段**：registry codec 面扩展（R5）→ csv/avro/protobuf（各带往返 + 错误路径 conformance）→ codecs: 段（loader/verify/engine）→ examples 各加一条新 codec 管道进 CI → CEL 类型映射文档（docs/codecs.md：avro long→int、double→double、union→dyn；proto int32/64→int、uint→uint、repeated→list；csv 显式列类型）。
4. **schema 导出 + 打磨**：`plugin schema` / `--all --dir`（R9）+ 看门狗 node 名 + repl + k8s 清单与文档 + README/README_ZH + 规范回写 v1.14（codecs: 段形状、convert 语义差异记档、catalog 形状变化、M4 裁剪清单）。

规范偏差处理：本审查所有裁决均以"规范已预留的实现自由度"或"记档裁剪"处理，无静默偏离；§5.10 codecs: 段落地后规范补形状示例。

## 四、结论

M4 是生态放大器：convert 与 LSP 两个锚点的全部数据源都已在 M1–M3 就位（verify 管线、registry schema、celhost/starhost 编译器），本里程碑的新增代码面是**协议胶水（JSON-RPC/vscode）+ 格式胶水（csv/avro/protobuf/HOCON）+ 单向翻译器（convert）**，不触碰架构面。依赖新增仅 hamba/avro/v2 与 gurkankaymak/hocon。**开工。**

# Riverpod v3 从零重设计提案 — Agent 原生事件路由器（含 EQL 重设计）

## 交付物

仓库根目录新增 `redesign-v3.md`（中文，风格对齐 riverpod-design.md，约 800-1000 行），并在 README.md / README_ZH.md 文档索引加一行链接（注明"提案/未实施"）。不改代码、不动现有设计文档。

---

## 一、现状诊断

**v2 做对的（保留）**：Go 单二进制 + 显式 DAG + 协议无关 Message + Codec 分层（N×M→N+M）、per-edge delivery 语义、`validate`/`test` 声明式测试、Agent 优先路线。

**v2 做错的（推倒）**：
1. **配置**：YAML/HOCON 双格式双倍维护且语义已分叉；三套拓扑写法并存；depends_on 值形态过多；step→stage→edge 术语转换 + 合体 step 隐藏展开；route 依赖 `metadata["er-route"]` 隐式协议。
2. **DSL（eql）**：CEL 表达式 + 外挂赋值的缝合体——无字符串模板（拼串靠 format()）、无 if/else、无 fallibility 类型检查、复杂转换直接掉进 Go 插件悬崖；谓词层重复造 CESQL 轮子且无 TCK；错误三态（编译/求值/缺失）语义模糊；非确定性函数（now()）导致 replay/simulate 无法精确。
3. **可靠性**：手写 per-edge 磁盘 WAL 被 review-2026-08 证实实际 at-most-once、torn write 停摆；跨边 refCount Ack 链复杂难证；非 Kafka 源无重放；DLQ 只进不出。
4. **扩展**：WASM 空壳、gRPC 插件推迟、连接器数量打不赢 Vector/Bento。
5. **可观测**：指标缺口一半、trace noop。
6. **命名**：与 Flutter Riverpod 正面撞名，SEO 与 Agent 语境双重污染。

## 二、战略定位

**一句话**：Agent 可安全操作的事件路由数据面 —— "pipelines as code, verified by machines, operated by agents"。

- 市场判断：Benthos 被 Redpanda 收购、MIT 分叉 Bento 由 Warpstream 维护，独立轻量流处理器现真空；Confluent/StreamNative 的 Agent 化是重平台路线。空白 = 轻量、自包含、为 Agent 操作设计的数据面。
- 非目标：连接器军备竞赛、可视化编排器、K8s 重平台、exactly-once。
- 换名建议：3-5 个候选名 + 判据（可注册、无撞名、Agent 语境可识别）。

## 三、产品方案 — 四道机器关卡（Agent 全闭环）

1. **verify**：JSON Schema 严格校验 + 拓扑不变量 + eql2 静态编译/类型/fallibility 检查 + 语义 lint。
2. **test**：fixture 合约测试（保留 testrunner，强化黄金文件/变体/schema 合约）。
3. **simulate/explain**：`riverpod explain` 输出消息路径确定性推演（进入 X → 命中条件 → 走边 → 落 Sink，含 delivery 语义）；`riverpod replay` 从 DLQ/spool 回放。
4. **operate**：MCP Server 一等公民（list_plugins/validate/test/reload/status/tail_dlq/replay/drain）+ Admin REST + 内嵌只读 DAG 可视化；全局 `--json`。

连接器策略：内置精选 6-8 个（kafka/http_server/cron/file 源，kafka/http/stdout/drop 汇）做到生产级，长尾走插件协议。

## 四、EQL 从零重设计（本次新增的核心章节，工作名 eql2）

### 4.1 设计目标（按优先级）
1. **Agent 主笔**：小而规则的语法、一种显而易见的写法、错误信息带行列定位可自修复——为"逐 token 生成 + 按报错迭代"的 Agent 工作流优化。
2. **verify 期安全**：静态类型 + VRL 风格 fallibility 追踪——有风险的函数必须显式处理，否则 verify 直接失败，把运行时错误挪到上线前。
3. **全函数性（totality）**：纯函数、无副作用、无网络、无用户层循环（只有 map/filter 等组合子）——保证终止。
4. **确定性**：非确定函数（now/random/uuid）显式标注 + 时钟注入——simulate/replay 与生产逐字节一致。
5. **互操作**：CloudEvents CESQL 作为**可选兼容方言**（`lang: cesql` 时跑官方 TCK），不自创谓词方言去竞争。

### 4.2 双层语言架构
- **谓词层（when）**：单布尔表达式，eql2 语法为主，CESQL 为可选兼容方言（换 CloudEvents 生态互操作 + 免费 TCK）。错误契约沿 CESQL：求值出错 = 不通过 + 计数。
- **映射层（map）**：语句语言。1:1 变换（一进一出）；split 是独立 transform 插件而非语言特性——语言保持可全量静态验证。

### 4.3 eql2 语法规格草案（文档含完整示例）
- 根对象：`payload` / `meta`；路径：点号 + `?.` 可选链 + `[]` 索引/切片。
- 语句：`payload.total = <expr>`（赋值）、`let x = <expr>`、`del payload.old`、`if / else if / else { }`（语句级 if 补齐）。
- **字符串模板**（推翻 v2 的"不做模板"决策）：`"order-{payload.id}-{meta.region}"`，`{{`/`}}` 转义，编译期类型检查——Agent 拼接场景第一需求。
- **fallibility 三件套**：`expr ?? fallback`（兜底）、`expr!`（断言，失败走 DLQ 且带行列）、`try(f, fallback)`；`parse_json`/`int()`/正则等风险函数不处理则 verify 报错。
- 缺失语义：无 schema 时 `payload.x` 返回 nil + verify warning；有 schema 时未知路径 verify error；`?.` 链安全。
- 组合子：`payload.items.map(x, x.price * x.qty)` / `.filter` / `.sum` 等 stdlib，替代循环。
- 复用：config 级命名映射块（`mappings:` + `apply`），初期不做用户自定义函数。
- route 从语言中移除：路由回归边属性（命名 route = 分组条件的编译期糖），消灭 `er-route` 隐式协议。

### 4.4 实现与工具链
- 表达式层基于 cel-go 求值内核（保留其生产级实现），语句层编译为类型化 AST + schema 槽位缓存（避免逐消息重复解析路径）。
- **转换阶梯**：eql2（90% 场景，全验证）→ Starlark `script` transform（9%，Python 子集、沙箱、步数上限、冻结输入——Agent 最顺手的全功能逃生舱）→ WASM/gRPC（1%）。三档覆盖，不再让复杂逻辑直接掉 Go 插件悬崖。
- 工具：`riverpod eql check|fmt|shell`（REPL）、自带 conformance 语料库（黄金文件 + fuzz）进 CI、LSP 列 P2；语法文档 + JSON Schema 供 IDE/Agent。
- 迁移：`riverpod convert` 覆盖 eql1 常见赋值（语法近亲），报告不可自动迁移项。

## 五、配置方式重设计

- 只留 YAML，`apiVersion: <新名>/v3` 显式版本化。
- 拓扑唯一写法 steps + depends_on；砍掉 flat pipeline[]、legacy edges:、合体 step。
- depends_on 收敛为 `"name"` 或 `{name: {attrs}}` 两种元素；route 展开为编译期可见的 condition 边。
- 变量替换全字段生效 + 严格语义（`${VAR}` unset=error、`${?VAR}` 可选）；base + env overlay 分层。
- 每个插件配置带 JSON Schema（IDE/Agent/LSP 共用），Loader 严格拒绝未知字段。
- 文档含 before/after 配置对比示例（含 eql→eql2 对照）。

## 六、架构设计

- 分层：Config（typed/versioned）→ Static IR（校验后 DAG + 预编译 eql2 程序 + schema 摘要）→ Runtime。
- 存储：不手写 WAL——嵌入式存储（SQLite modernc.org/sqlite 或 Pebble，权衡写入文档）统一承载 spool/offsets/DLQ；可靠性模型 =「durable ingest → 内存 DAG → sink retry/DLQ → pipeline 级 checkpoint」。
- Ack 模型：每消息 settle 追踪（所有匹配路径成功或 DLQ 即 settle，settle 才提交 source offset），替代跨边 refCount，不变量简单可测。
- 扩展三档：Go 编译时注册 / WASM transform / gRPC source/sink（+Starlark script 档）。
- 运行时：per-pipeline supervisor、channel 背压传导、优雅停机 drain。
- 可观测：OpenTelemetry SDK 原生（OTLP + Prometheus 双出口）、span 贯穿 DAG、eql2 求值带行列入 span/DLQ metadata。
- 部署：单二进制为主，K8s = Deployment + reload API，Operator 降 P2；HA 靠 source 可重放 + spool 本地，单活实例。
- 新 internal/ 模块划分图（eql2 独立成包：lexer/parser/typechecker/stdlib/conformance）。

## 七、与 v2 对比 + 迁移与交付

- 逐领域差异对比表（含 eql vs eql2 专项表）。
- `riverpod convert`：配置 + eql 一起迁。
- 里程碑：M1 引擎核心 + spool + eql2 规格与编译器（含 conformance）+ verify/test → M2 MCP + explain/replay + CESQL 方言（TCK）→ M3 插件协议（Starlark script / WASM / gRPC）→ M4 生态（Schema 发布、LSP、内嵌 UI、Operator）。

## 实施步骤

1. 写入 `redesign-v3.md`：上述全部章节 + 代码级示例（eql2 语法示例、配置 before/after、模块图、对比表）。
2. README.md / README_ZH.md 文档索引各加一行链接（标注提案/未实施）。
3. 不改代码、不动现有设计文档。
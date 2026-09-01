# riverpod v2 设计评审报告

> **注意（2025-06-24）：** 部分建议已被 `riverpod-design.md` v2.0-draft 吸收或推翻（如独立 `edges:`、`Planner` 层、`StageIR.predicate` 等）。以 `riverpod-design.md` 为准；下文保留历史评审脉络。

> 评审对象：`riverpodouter-v2-design.md`（v2.0-draft，2025-06-24）
> 评审方法：文档逐节细读 + 6 大主流开源竞品深度对比
> 竞品：Benthos/Redpanda Connect、Vector、Kafka Connect、Knative Eventing、Argo Events、Fluentd、Flume、OpenTelemetry Collector、CloudEvents CESQL、Cloudera Envelope

完整竞品调研附件见 `competitor-research.md`。

---

## 一、总体评价

设计质量在同类项目中处于**中上水平**。最值得肯定的三点：

1. **拓扑层 DAG 一等公民**（stages + edges）— 比 Benthos 的"线性 pipeline + 嵌套 `workflow` processor"更干净，比 Vector 的"`inputs:` 隐式边"更显式。这是相对所有竞品的真实优势。
2. **per-edge buffer + per-edge condition** — 把"背压"和"路由"两个关注点统一在边上，比 Vector（buffer 只能在 sink 前、condition 只能在 transform 内）更组合化。
3. **refCount Ack + Go/WASM 双插件 + 单二进制/Operator 双形态** — 与 Benthos、Vector 的成熟做法对齐，方向正确。

但存在 **7 处设计自相矛盾 / 关键缺口**，以及 **10 项可向竞品借鉴的更优设计**。下文逐条列出。

---

## 二、设计文档自身的问题（必须修正）

### 2.1 Sink 接口与引擎职责自相矛盾【高优先级】

§5.5 Sink 接口为 `Write(ctx, msgs []*Message) error`，§9.4 又说"v2 改为 Sink 接口自带批处理能力"，§6.1 引擎图里却是 `fanOut ──[Buf]──▶ Sink.Write()`。

**矛盾：** 引擎的 fanOut 是**逐条**往边缓冲送消息的（一条 Message 进来，匹配的出边各克隆一份入 buf）。那么 Sink 收到的 `[]*Message` 是谁攒的？批次的触发条件（size/timeout/max_bytes）由谁执行？如果由 Sink 自己从单条入参攒批，那 Sink 必须自己跑一个 goroutine 从某 channel 收——但接口签名是同步 `Write(msgs)`，没有入参 channel。

**修正建议：**
- 要么 Sink 接口改为 `Sink.Consume(ctx, in <-chan *Message) error`（push 模式，Sink 自管攒批，与 Source 对称），引擎只负责把边缓冲的另一端交给 Sink；
- 要么引擎在 Sink 前面挂一个 `Batcher` 节点（可视为一种特殊的 Transform），由 Batcher 攒批后调用 `Sink.Write(msgs)`。
- 推荐**前者**——与 Source 对称、与 Benthos/Vector 一致，且让 Sink 自己决定攒批策略更自然。文档应明确写出"Sink 持有一个 goroutine 从 in channel 拉取并攒批"。

### 2.2 Transform 接口无法表达 fan-in【高优先级】

§5.4 `Process(ctx, msg *Message) ([]*Message, error)` 是**单条入、多条出**。§6.1 又说"fanIn 合并多条入边的消息到一个队列供下游 Stage 消费"。

**问题：** fanIn 在引擎层做了，但 Transform 接口看不到批次边界。一旦下游 transform 需要做**窗口聚合**（§9.2 列了 `aggregate`）、**去重**（`deduplicate`）、**排序**——这些都需要看到"一批"消息而不是逐条——就无处安放。当前接口只能做逐条 map/filter/split。

**修正建议：**
- Transform 接口改为 `Process(ctx, msgs []*Message) ([]*Message, error)`（**批次入、批次出**，与 Benthos `ProcessBatch` 一致）；
- 引擎 fanIn 把多条入边的消息攒成一批后调用；
- 简单的逐条 transform 在内部 `for _, m := range msgs` 即可；
- 这同时解决了 §9.2 `aggregate`/`split` 的实现路径，也让 `route` transform 能基于批次做更聪明的分发。
- 这是一个**架构级改动**，越早定越好。

### 2.3 refCount 在条件路由下无法静态计算【高优先级】

§6.3 "Message 创建时，refCount = 拓扑中该消息可达的 Sink 节点数"。

**问题：** §4/§7 的 DAG 边上带 `condition`（DSL 表达式）。一条消息走哪条边**取决于运行时求值**，无法静态算"可达 Sink 数"。例：`splitter` 有三条出边分别 `channel=='us'`/`'eu'`/`else`，一条消息只会匹配其中一条，但静态"可达"是 3。如果 refCount=3，消息只到 1 个 sink，refCount 永远减不到 0，Ack 永不触发 → 消息泄漏 / Source 永不 commit offset。

**修正建议：**
- refCount 在**fanOut 时动态确定**：求值所有出边 condition，匹配的边数 = 本次 refCount。每条匹配边克隆的消息持有同一个原子计数器。
- 文档应明确："refCount = 该消息**本次 fanOut 实际匹配的出边数**累加到所有 sink 的副本数"，而非"拓扑可达 sink 数"。
- 这也意味着同一消息经过 transform 拆分成 N 条后，每条子消息各自维护自己的 refCount，父消息的 refCount 等于子消息 refCount 之和（Benthos 的 split 即如此）。

### 2.4 Source 接口对 pull 源不友好【中优先级】

§5.3 `Consume(ctx, out chan<- *Message) error` 是 push 模式，注释说"适合 Kafka/HTTP 等持续流源"。

**问题：** §9.1 列了 `aws_s3`（poll）、`aws_sqs`（长轮询）、`mysql_cdc`（binlog tail）、`file`（tail）。S3 poll 是典型的"定时拉取、可能一次拉一批对象、可能空轮询"——用 push channel 表达很别扭（要在 Consume 里自己起 ticker + 内部 goroutine + 控制并发）。这与"渐进复杂度"原则相悖。

**修正建议：**
- 保留 push 接口作为默认，但同时提供 `PollingSource` 辅助抽象（类似 Benthos 的 `AsyncBatcher`/`AuxInput`），让 pull 源实现 `Poll(ctx) ([]*Message, error)`，由框架负责定时调用、背压、并发。
- 文档应给 pull 源一个示例实现骨架，避免每个 poll 源重复造轮子。

### 2.5 eql 的错误处理语义未定义【高优先级】

§8.7 只说"字段不存在返回 nil 不 panic；类型转换失败返回 error 并标记消息"。

**问题：** 关键问题没回答：
- mapping DSL 里某行求值出错，**后续行是否继续执行**？
- filter DSL 求值出错，**消息是过还是不过**？（CESQL 明确规定"filter MUST NOT pass on error"）
- edge condition 求值出错，**消息走还是不走这条边**？走 default？丢弃？
- 标记为 errored 的消息在下游 `.error` 字段可见，那它还会被进一步 transform/sink 处理吗？还是直接进 DLQ？

**修正建议：**
- 直接借鉴 **CESQL 的错误契约**：filter/condition 求值出错 → 默认 false（不通过），同时记一个 `dsl_error` metric + 可选投递到 error channel。
- mapping 出错 → 默认整条 mapping 失败、消息标记 errored、走 stage 级 retry/DLQ；提供 `try() / catch()` 操作符让用户在 DSL 内兜底（参考 Bloblang `.catch()`、VRL `?`/`??`）。
- **强烈建议引入 VRL 的 fallibility 标记**：编译期区分 fallible / infallible 操作，未处理的 fallible 调用编译失败。这是 VRL 相对所有 DSL 的最大优势，riverpod 应抢占。
- 文档应新增 §8.8 "错误语义"，把以上四类显式写清。

### 2.6 eql 与 CESQL 重复造轮子【高优先级】

§8 的 eql 过滤语法（`==`/`!=`/`>`/`in`/`contains`/`matches`/`has`/`&&`/`||`/`!`）与 [CloudEvents SQL](https://github.com/cloudevents/spec/blob/main/cesql/spec.md) 几乎完全重叠。CESQL 是 CNCF 标准化的 CloudEvents 过滤语言，**自带 TCK**（`cesql_tck/` YAML 测试集），Knative 已有 Go 实现。

**问题：** 自己造 eql 等于放弃了一个免费的标准化生态位 + 一套现成测试用例 + 与 Knative/CloudEvents 生态的互操作。

**修正建议：**
- **eql 的 predicate 子集直接实现 CESQL**，并跑通官方 TCK；
- eql 的 mapping 部分保留自有语法（CESQL 不覆盖赋值/管道），但作为 CESQL 的超集；
- 把 `data.*` 路径访问作为 CESQL 的 riverpod 扩展命名空间（CESQL 明确不覆盖 `data` 字段，留了扩展口）；
- 在文档里明确写"eql = CESQL + mapping 扩展"，而非"自研 DSL"。

### 2.7 次要问题

- **§5.1 Message 的 `Route []string` 冗余**：拓扑层已保证无环，运行期再记路径是双重保险但每条消息都背负这个 slice 是不必要的内存开销。改为 dev 模式可选开启。
- **§5.1 私有字段 `ctx/ackFn/errCount` 放在公开 struct 里**：Go 里同一 package 仍可见，跨 package 不可见，但 struct 字段对齐会被空隙填充浪费内存。建议拆为 `Message`（公开）+ `messageInternal`（引擎持有，按 ID 关联），或用 `sync.Pool` 复用。
- **§7.5 推导模式自动 id `transform-0`**：配置热加载做 diff 时，用户改 transforms 顺序会导致 id 全部变化 → 误判为"全部变更" → 全 pipeline 重启。应要求推导模式也允许用户给 `name:`，缺失时才用序号。
- **§7.5 推导模式不支持 fan-in**：合理，但应在 §7.2 简单示例旁注明限制，避免用户踩坑。
- **§11.3 每 Pipeline 一个 Deployment**：资源浪费严重。Benthos streams mode 单进程多 pipeline、Vector aggregator 单 StatefulSet 多 pipeline 都是更优解。文档应把"聚合模式"作为默认，"独立 Deployment"作为需显式开启的隔离选项。
- **§6.5 优雅停机第 5 步 "Transform.Stop() → Source 资源释放"**：顺序写反了，Source 应最先停（已停产出），最后释放资源；Transform.Stop 应在 Sink.Stop 之前或并行。
- **§10 指标命名前缀**：前缀应统一为 `riverpod_`，避免命名债。

---

## 三、与竞品对比的关键缺口（应吸收的更优设计）

按优先级排序。

### 3.1 【P0】磁盘缓冲（WAL buffer）— 来自 Vector / Fluentd / Benthos

**现状：** §6 Edge Buffer 仅 `size` + `strategy`（block/drop_newest/drop_oldest），全是内存 channel。进程崩溃 = 在途数据全丢。

**竞品做法：**
- Vector：`disk` buffer（WAL，fsync 500ms，checksum，崩溃可恢复）+ `overflow` 拓扑（memory 满 → 自动溢写到 disk）；
- Fluentd：`file` chunk（每个 chunk 一个文件）+ chunk key 分区；
- Benthos：`sqlite` buffer（磁盘）。

**建议：** Edge buffer 增加 `type: memory|disk`，disk 类型用 WAL 实现；提供 `overflow` 策略让 memory 边自动溢写到配套的 disk 边。这是 at-least-once 语义的真正底线——没有 disk buffer，"refCount Ack + Kafka offset commit"在崩溃场景下形同虚设。

### 3.2 【P0】批处理一等公民 — 来自 Benthos

**现状：** §5 Message 是单条；§9.4 batch 只在 Sink 出现；§9.2 transform 列表里 `aggregate` 是 P2。

**竞品做法：** Benthos 的**一切皆 batch**——`message.Batch` 是 `[]*Message`，所有 processor 接收和返回 batch，`group_by`/`for_each`/`archive`/windowed 聚合都是一等公民。

**建议：** 配合 §2.2 的 Transform 接口改造，让 batch 贯穿全链路。否则 riverpod 永远只能做"逐条事件路由"，做不了窗口聚合、跨消息去重、批量丰富——而这正是 Benthos/Vector 的核心场景之一。

### 3.3 【P0】CESQL 标准化 — 来自 CloudEvents / Knative

详见 §2.6。这是低成本高收益的生态位抢占。

### 3.4 【P1】OTel Connector 模式：路由边作为一等节点 — 来自 OpenTelemetry Collector

**现状：** riverpod 的"路由 + fan-out"要用 `route` transform + 多条带 condition 的边表达，3 个节点表达 1 个概念。

**竞品做法：** OTel Collector 的 `routing` connector 是**同时是 Sink（上游管道的出口）和 Source（下游管道的入口）**的单节点，配 `action: move|copy` + `default_pipelines`：
- `move` = switch-case 首匹配即走（默认）；
- `copy` = fan-out 同时走多个下游；
- `default_pipelines` = 兜底。

**建议：** 在 riverpod 里引入 `Router` 作为一种特殊 Stage（兼具 Sink + Source 语义），用 `action: move|copy` + `default` 表达。常见 fan-out 路由从"1 transform + N 边"压缩为"1 Router 节点"。这与现有 DAG 模型兼容（Router 仍是图中的一个节点）。

### 3.5 【P1】VRL 风格的编译期 fallibility — 来自 Vector

**现状：** eql 的错误处理是运行时的（§2.5 已述）。

**竞品做法：** VRL 每个函数标注 fallible/infallible，编译器**拒绝**未处理的 fallible 调用。操作符：`f!()`（断言不失败，失败 abort）、`f()?`/`x = f() ?? default`（失败兜底）、`x, err = f()`（元组解构显式处理）。

**建议：** eql 引入 fallibility 类型系统。这是 DSL 体验的最大差异化点，比"自研一门 jq-like"更有说服力。

### 3.6 【P1】Kafka Connect 的 predicate-on-transform

**现状：** riverpod 要"条件性应用某个 transform"必须新建一个分支边。

**竞品做法：** Kafka Connect SMT 每个 transform 可带 `predicate`，predicate 不匹配则该 transform 跳过该记录，DAG 拓扑不变。

**建议：** StageIR 增加 `predicate` 字段（CESQL 表达式），引擎在调用 `Process` 前对每条消息求值，false 则透传。配置上 `mask_ssn.predicate: "metadata.env == 'prod'"` 极其简洁。

### 3.7 【P1】Fluentd chunk key 分区缓冲

**现状：** 每条 edge 一个单一 channel，无分区概念。

**竞品做法：** Fluentd `<buffer tag, time>` 按 chunk key 分区，每分区独立队列 + 独立 flush；保证分区内顺序、分区级背压。

**建议：** EdgeBufferConfig 增加 `key: [metadata_field, ...]`，引擎按 key hash 分桶，每桶独立 channel。Kafka 多 partition 保序、多租户隔离都依赖这个。

### 3.8 【P1】Flume 的 required vs optional 边

**现状：** 所有出边地位平等，一条边失败影响整批 Ack。

**竞品做法：** Flume 的 `selector.optional.CA = file-channel-2` 标记某条边为可选——可选边失败不影响主链路 Ack。

**建议：** Edge 增加 `required: true|false`（默认 true）。审计/监控类 sink 设为 false，失败只记 metric 不回滚。这是生产场景常见诉求。

### 3.9 【P1】Knative DeliverySpec + EventType CRD

**现状：** retry/DLQ/timeout 散落在 StageIR 各字段；无事件类型注册。

**竞品做法：** Knative 把 `retry`/`retryAfterMax`/`timeout`/`deadLetterSink` 抽成 `DeliverySpec`，可作为 per-edge 或 per-sink 的正交配置块；`EventType` CRD 注册事件类型供发现。

**建议：**
- 抽出 `DeliverySpec` 作为 Edge/Sink 的统一交付配置块，与路由 condition 解耦；
- K8s 模式下提供 `EventType` CRD，DSL predicate 可按事件类型名引用（避免魔法字符串）。

### 3.10 【P2】Argo 的复合事件触发（join + reset）

**现状：** riverpod 的 DAG 是无状态逐事件路由。

**竞品做法：** Argo Sensor 的 `conditions: "dep01 && dep02"` 在多个 Source 的事件间做逻辑join，`conditionsReset` 按时间窗清除陈旧半匹配。

**建议：** 作为 v2.2+ 的高级特性，引入 `Join` Stage——在 DAG 汇聚点上做跨源事件相关，配 `reset` 窗口。这是 riverpod 相对所有竞品可独占的差异化能力（Benthos/Vector 都不原生支持）。

### 3.11 【P0】Planner 层 — 来自 Cloudera Envelope

**现状：** riverpod 的 Sink 同时承担"传输"（Kafka/HTTP/Kudu）和"写入语义"（append/upsert）两个职责，耦合在一个插件里。无法声明式表达 SCD Type 2、bi-temporal、merge-into 等常见 ETL 写入模式。

**竞品做法：** Envelope 在 Transform 和 Output 之间插入 **Planner** 层，专门决定"如何把到达的记录应用到目标"：

| Planner | 语义 |
|---|---|
| `append` | 仅插入 |
| `upsert` | 按 key 插入或更新 |
| `overwrite` | 全量覆盖 |
| `delete` | 按 key 删除 |
| `history` | **Type 2 SCD** — 跟踪 effective-from/to 区间 + current flag，按 event time 排序 |
| `bitemporal` | **双时态** — event-time + system-time 双重有效期 |
| `eventtimeupsert` | 按 event time + 自然 key upsert |

```hocon
planner {
  type = history
  fields.key = [clordid]
  fields.timestamp = [transacttime]
  fields.values = [symbol, orderqty, leavesqty]
  fields.effective.from = [startdate]
  fields.effective.to = [enddate]
  field.current.flag = currentflag
  carry.forward.when.null = true
}
```

**建议：** riverpod 引入 Planner 作为 Sink 的装饰层（或 Transform 与 Sink 之间的独立层）。P0：`append`/`upsert`/`drop`。P1：`history`(SCD2)/`merge`。P2：`bitemporal`。这是 riverpod 进入 ETL 场景的**关键缺失**——没有它，SCD2/bi-temporal 必须在每个 Sink 插件里各自实现或交给用户写 Transform，重复且易错。

### 3.12 【P0】Translator 层 — 来自 Cloudera Envelope

**现状：** riverpod 的 Source 同时承担"传输"和"解析"——Kafka source 必须知道 payload 是 JSON 还是 Avro。这导致连接器矩阵组合爆炸（`kafka_json`/`kafka_avro`/`http_csv`…）。

**竞品做法：** Envelope 在 Input 内部插入 **Translator** 子组件，把字节解析与传输解耦：

| Translator | 用途 |
|---|---|
| `avro` | Avro 解码 + schema |
| `delimited` | CSV / 分隔符 |
| `kvp` | 键值对（如 FIX `tag=value\u0001`） |
| `protobuf` | Protobuf 解码 |
| `raw` | 透传 |

```hocon
input {
  type = kafka  brokers = "..."  topics = [fix]
  translator { type = kvp  delimiter.kvp = "\u0001"  delimiter.field = "=" }
}
```

**建议：** riverpod 的 Source 增加可选 `translator` 子配置（或等价的 `decode` Transform 语法糖）。P0：`json`/`raw`。P1：`avro`/`protobuf`/`csv`。连接器矩阵从 N×M 降为 N+M。

### 3.13 【P1】`dependencies`/`depends_on` 作为无条件边的语法糖 — 来自 Envelope / Vector

**现状：** riverpod §7.3 显式 DAG 模式要求 `stages:` + `edges:` 两段声明，即使是简单的线性链或 fan-in 也要单独列出每条边。

**竞品做法：** Envelope 和 Vector 都把边声明**内联到子节点**——Envelope 用 `dependencies = [a, b]`，Vector 用 `inputs = [a, b]`。无条件的线性/fan-in 场景无需单独 `edges:` 段：

```yaml
pipeline:
  - id: enrich
    depends_on: [kafka-source]        # 内联边，等价于 edges: [{from: kafka-source, to: enrich}]
  - id: splitter
    depends_on: [enrich]
  - id: us-sink
    depends_on: [splitter]
    condition: "metadata.channel == 'us'"   # 有 condition 时仍用显式 edge 或内联 condition
```

**建议：** StageIR 增加可选 `depends_on: [ids]` 字段，作为无条件边的语法糖。配置加载时展开为 EdgeIR。**保留** `edges:` 段用于需要 per-edge condition/buffer/delivery 的复杂场景。这样简单配置更友好（对标 Envelope/Vector），复杂配置仍显式（保留 riverpod 优势）。

### 3.14 【P1】HOCON 配置格式或 HOCON 特性 — 来自 Envelope

**现状：** riverpod 用 YAML，§11.2 需要 `--set-env KAFKA_BROKERS=kafka:9092` CLI 参数手动注入环境变量。

**竞品做法：** Envelope 用 [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md)，天然支持：
- **环境变量自动覆盖**（Typesafe Config 内置，无需 CLI flag）；
- **`${substitution}`** 引用其他配置键，DRY；
- **扁平点号路径**（`application.executor.memory = 4G` ≡ 嵌套块），减少缩进；
- **`#` 和 `//` 双注释**；
- **非缩进敏感**（`{}` 分界，不像 YAML 的 tab/space 陷阱）。

**建议：** 短期在 YAML 之上实现 env 变量自动覆盖 + `${}` 替换（对标 HOCON 特性）；中期评估提供 HOCON 作为可选配置格式（Go 有 `github.com/gurkankaymak/hocon` 等解析器，需评估成熟度）。至少在文档里把"env 变量如何注入配置"写清楚，避免用户依赖 `--set-env` 的 ad-hoc 方案。

### 3.15 【P2】Decision + Task 步骤 — 来自 Envelope

**现状：** riverpod 的路由是逐消息的（per-edge condition），无法在**拓扑层**根据运行时数据决定"整个子图是否执行"。

**竞品做法：** Envelope 的 `decision` step 在运行时求值（`literal`/`step_by_key`/`step_by_value`），`if-true-steps: [a, b]` 决定哪些依赖步骤实际运行——**动态拓扑剪枝**。`task` step 是纯副作用步骤（发通知、更新 metastore），与数据流解耦。

**建议：** v2.1+ 引入 `decision` Stage（运行时决定子图执行）和 `task` Stage（生命周期副作用，如启动时预热缓存、停机时写 manifest）。这是 riverpod 走向"管道即应用"的进阶能力。

---

## 四、组件生态与工程化建议

### 4.1 连接器广度

Benthos ~100+ 连接器、Vector ~120+、riverpod 计划 35 个。**差距巨大但路线图合理**（先 P0 再 P1/P2）。建议：
- P0 阶段就**抄 Benthos 的插件注册模式**（`init()` 注册 + 全局 registry + 公开 `plugin.RegisterSource/Sink/Transform`），降低后续扩展成本；
- 把 Benthos 的连接器配置 schema 抄过来作为参考实现（Kafka/S3/PubSub 的配置项都是经过生产验证的）。

### 4.2 Schema Registry / Protobuf / Avro

文档完全没提。Benthos 有 `protobuf`/`schema_registry_decode`/`avro`/`parquet` processor + Confluent Schema Registry + Buf Schema Registry 集成。这是数据管道场景的刚需。

**建议：** 在 §9.2 Transform 增加 P1 `protobuf_decode/encode`、`avro_decode/encode`、`schema_registry_validate`；Message.Metadata 约定 `content-type` + `schema_subject` + `schema_version` 三个标准键。

### 4.3 配置 DX 工具

Benthos 有 `lint`/`echo`/`create`/`blobl`（REPL）/单元测试框架。riverpod §11.5 只列了 `run/validate/test/doc`。

**建议：** 增加 `riverpod eql`（DSL REPL）、`riverpod lint`（配置 lint，未列入 run 前的静态检查）、`riverpod test`（fixture 驱动的管道测试，对标 Benthos 的 `--set`/`test` 框架）。CI 友好度直接影响采纳率。

### 4.4 多 Pipeline 运行时（streams mode）

Benthos `streams` mode 用 REST API 动态增删 pipeline，单进程多 pipeline。riverpod §11 单二进制模式默认单 pipeline，K8s 模式默认每 pipeline 一个 Deployment。

**建议：** 单二进制模式增加 `--streams-dir`（目录内每个文件一个 pipeline，热加载），并提供 `POST /streams/{name}` REST API。这比"每 pipeline 一个进程"或"每 pipeline 一个 Pod"资源效率高一个数量级。

---

## 五、修订后的关键接口建议（综合以上）

```go
// Source — push 模式
type Source interface {
    Stage
    Consume(ctx context.Context, out chan<- *Message) error
}

// Pull 源辅助接口（框架提供定时调用 + 并发控制）
type PollingSource interface {
    Stage
    Poll(ctx context.Context) ([]*Message, error)
    Interval() time.Duration
}

// Transform — 批次入、批次出（关键改动）
type Transform interface {
    Stage
    Process(ctx context.Context, batch []*Message) ([]*Message, error)
}

// Sink — push 模式，自管攒批（关键改动）
type Sink interface {
    Stage
    Consume(ctx context.Context, in <-chan *Message) error
    Flush(ctx context.Context) error
}

// Planner — 解耦写入语义与 Sink 传输（来自 Envelope）
// 坐在 Transform 出口与 Sink 之间，决定"如何把记录应用到目标"
type Planner interface {
    Stage
    Plan(ctx context.Context, incoming []*Message, sink LookupSink) ([]*Mutation, error)
}
type MutationType int
const (
    MutationInsert MutationType = iota
    MutationUpdate
    MutationUpsert
    MutationDelete
)
type Mutation struct {
    Type MutationType
    Key  []byte
    Row  []byte
}

// Translator — 解耦字节解析与 Source 传输（来自 Envelope）
// Source 可选挂载，把 raw payload 解码为结构化 payload
type Translator interface {
    Decode(ctx context.Context, raw []byte) ([]byte, map[string]any, error)
}

// Edge — 增加磁盘缓冲、分区、required
type Edge struct {
    From      string
    To        string
    Condition string             // CESQL 表达式
    Predicate string             // 对 To 端 Transform 的条件应用（Kafka Connect 风格）
    Buffer    EdgeBufferConfig
    Delivery  *DeliverySpec      // retry/timeout/DLQ，正交于路由
    Required  *bool              // 默认 true；false = best-effort 边
}

type EdgeBufferConfig struct {
    Type     BufferType          // memory | disk | overflow(memory→disk)
    Size     int
    Key      []string            // 分区键（Fluentd chunk key 风格）
    Strategy BufferStrategy     // block / drop_newest / drop_oldest
    DiskPath string              // type=disk 时
}

// StageIR 增加 depends_on（无条件边语法糖，展开为 Edge）+ planner + translator
type StageIR struct {
    ID         string
    Type       StageType
    Plugin     string
    DependsOn  []string           // 内联无条件边（Envelope/Vector 风格语法糖）
    Workers    int
    Batch      *BatchConfig
    Retry      *RetryConfig
    DLQ        *DLQConfig
    Planner    *PlannerConfig     // Sink 前置：写入语义（append/upsert/history/…）
    Translator *TranslatorConfig  // Source 后置：字节解析（json/avro/csv/…）
    Predicate  string             // Transform 条件应用（Kafka Connect 风格）
    Config     map[string]any
}
```

对应的 YAML 简化示例（线性场景，用 `depends_on` 语法糖，无需 `edges:` 段）：

```yaml
pipeline:
  - id: kafka-source
    type: source
    plugin: kafka
    config: { brokers: [localhost:9092], topics: [orders] }
    translator: { type: json }              # Envelope 风格，解耦解析

  - id: enrich
    type: transform
    plugin: map
    depends_on: [kafka-source]              # 内联边，无需单独 edges 段
    config: { dsl: ".payload.total = .payload.price * .payload.quantity" }

  - id: orders-kudu
    type: sink
    plugin: kudu
    depends_on: [enrich]
    planner: { type: upsert, fields.key: [order_id] }   # Envelope 风格，写入语义
    config: { connection: "...", table: "orders" }
```

---

## 六、结论与下一步

### 设计文档需立即修正的 5 个架构级问题：
1. **Sink 接口**（§5.5）—— 改为 push `Consume(in <-chan)`，消除攒批职责矛盾。
2. **Transform 接口**（§5.4）—— 改为批次入批次出，否则窗口/聚合/fan-in 无处安放。
3. **refCount 计算**（§6.3）—— 改为 fanOut 时动态计算，否则条件路由下 Ack 永不触发。
4. **eql 错误语义**（§8）—— 新增 §8.8 显式定义四类错误行为；考虑实现 CESQL + fallibility。
5. **Planner/Translator 分层缺失**（§5/§9）—— 引入 Planner（写入语义）和 Translator（字节解析）层，否则 Sink/Source 插件组合爆炸且无法声明式表达 SCD2/bi-temporal。

### 应吸收的更优设计（按 ROI 排序）：
1. CESQL 标准化（低成本高生态收益）
2. **Envelope Planner 层**（解耦写入语义与 Sink 传输，解锁 SCD2/bi-temporal 等 ETL 场景）
3. **Envelope Translator 层**（解耦字节解析与 Source 传输，连接器矩阵 N×M → N+M）
4. 磁盘缓冲 / overflow 拓扑（at-least-once 的真正底线）
5. 批处理一等公民（解锁窗口/聚合场景）
6. **`depends_on` 无条件边语法糖**（Envelope/Vector 风格，简单配置更友好）
7. **HOCON 特性或格式**（env 自动覆盖 + `${}` 替换，告别 `--set-env` ad-hoc 方案）
8. VRL 风格编译期 fallibility（DSL 体验差异化）
9. OTel Connector 路由节点（压缩常见 DAG 模式）
10. Kafka Connect predicate-on-transform（条件性应用 transform 的简洁表达）
11. Fluentd chunk key 分区缓冲（多 partition/多租户场景刚需）
12. Flume required/optional 边（审计 sink 不影响主链路）
13. Knative DeliverySpec + EventType CRD（交付关注点正交化 + 事件发现）
14. Argo join + conditionsReset（差异化高级能力，v2.2+）
15. **Envelope Decision + Task 步骤**（运行时拓扑剪枝 + 生命周期副作用，v2.1+）

### 保留并发扬的自身优势：
- 拓扑层 DAG 一等公民
- per-edge buffer + per-edge condition 的组合
- Go + wazero WASM 双插件（比 Vector 已废弃的 WASM 更现代）
- 单二进制 + Operator 双形态

**总体判断：** 设计方向正确，但 §5/§6 的接口定义存在 4 处会导致实现期返工的硬伤，应在新代码动工前修订；DSL 策略应从"自研 eql"调整为"CESQL 兼容 + mapping 扩展 + fallibility 类型系统"以抢占生态位；缓冲层必须补齐磁盘缓冲否则可靠性承诺站不住；**应引入 Envelope 的 Planner/Translator 分层**，否则 riverpod 在 ETL 场景缺乏声明式写入与解析能力，Sink/Source 插件会陷入组合爆炸；配置体验上应吸收 Envelope/Vector 的 `dependencies` 内联边语法糖和 HOCON 的 env 变量自动覆盖特性，让简单场景的配置真正"简单"。完成上述修订后，riverpod 的拓扑模型将显著优于 Benthos 的"线性+嵌套 workflow"，与 Vector 持平，并在 ETL 写入语义（Planner）上补齐相对所有流路由竞品的短板。

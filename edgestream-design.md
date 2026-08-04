# edgestream 需求与设计方案

> 版：v2.0-draft（edgestream，原 EdgeStream v2）
> 日期：2025-06-24
> 状态：定稿前修订（经竞品深度对比 + 40 项架构决策澄清）

---

## 目录

1. [背景与目标](#1-背景与目标)
2. [竞品调研与设计决策](#2-竞品调研与设计决策)
3. [整体架构](#3-整体架构)
4. [核心接口与 Message 模型](#4-核心接口与-message-模型)
5. [Codec 体系](#5-codec-体系)
6. [引擎运行时](#6-引擎运行时)
7. [DSL 语言设计 (eql)](#7-dsl-语言设计-eql)
8. [配置模型](#8-配置模型)
9. [组件生态规划](#9-组件生态规划)
10. [可观测性设计](#10-可观测性设计)
11. [部署架构](#11-部署架构)
12. [开发路线图](#12-开发路线图)
13. [v2.0 定稿检查清单](#13-v20-定稿检查清单)

> **AI/Agent** 作为 v2.1+ 关键特性，独立设计文档：[docs/ai-agent.md](docs/ai-agent.md)

---

## 1. 背景与目标

### 1.1 v1 回顾

EdgeStream v1 是一个轻量级 Go 事件路由库/二进制程序，围绕 `Input → Processor → Output` 模式处理 `CloudEvents`。

| 能力 | 实现 |
|------|------|
| 管道模型 | 线性 Pipeline：Source → Input-Processors → Global-Processors → Output-Processors → Sink |
| 事件标准 | CloudEvents 原生，traceid/spanid 注入 |
| 并发控制 | Per-pipeline worker pool + backpressure limiter（CAS + 状态机） |
| 可靠性 | 重试、DLQ、指数退避 |
| 批量输出 | Sink 外包装 batch layer |
| 可观测性 | Prometheus metrics、OTLP tracing、Webhook 通知 |
| 插件机制 | FactoryRegistry，Go 编译时注册 |
| 输入源 | HTTP、Kafka、Cron |
| 输出目标 | HTTP、Kafka、gRPC、Drop、Log |
| 处理器 | Logger、JSON 解析、Goja JS 脚本、Filter |

### 1.2 edgestream 目标

从架构层面重新设计，功能覆盖 v1 核心能力的同时，解决以下结构性不足：

- **管道拓扑过于简单** — 仅支持线性管道，无条件路由、分支合并
- **事件模型绑定 CloudEvents** — 无法处理非 CE 协议的数据管道场景
- **无内置数据转换语言** — 依赖 Goja JS 脚本，性能差，调试困难
- **连接器生态薄弱** — 仅 3 输入、5 输出、4 处理器
- **部署形态单一** — 仅单二进制，无 K8s 原生支持
- **无配置热加载、事件重放、磁盘缓冲等生产特性**

### 1.3 设计原则

- **渐进复杂度** — 简单场景 `depends_on: [a,b]`；复杂场景在同字段用对象形态声明 buffer/delivery/route
- **DAG 而非 Mesh** — 有向无环图约束，避免事件风暴和死锁
- **Message 为中心** — `[]byte` payload + metadata，协议无关
- **引擎不解析 payload** — 格式解析下沉到 Codec 插件
- **双模式部署** — 单二进制 + K8s Operator，共享核心引擎
- **CEL 为表达式层** — 不自研谓词语言，采用 CEL 标准 + cel-go 生产级实现

---

## 2. 竞品调研与设计决策

### 2.1 对标项目

| 项目 | 核心模式 | 关键启发 |
|------|---------|---------|
| **Benthos/Redpanda Connect** | Input→Processor→Output，Bloblang | Message 模型、COW 消息复制、batch.policy、AckFunc、streams mode |
| **Vector** | Sources→Transforms→Sinks DAG | VRL fallibility 类型系统、disk buffer + overflow、route.\<id> 分支、`inputs` 内联边 |
| **Kafka Connect** | Source/Sink + SMT | predicate-on-transform、DLQ conventions、commitRecord 可选 ack、schema registry |
| **Knative Eventing** | Broker-Trigger | DeliverySpec、EventType CRD、CE extension attributes for DLQ |
| **Argo Events** | EventSource→Sensor→Trigger | conditions + conditionsReset（复合事件触发） |
| **Fluentd** | tag 路由 + buffer chunk | chunk key 分区缓冲、@ERROR label |
| **Flume** | Source→Channel→Sink | required vs optional 边、transactional channel |
| **OpenTelemetry Collector** | Receivers→Processors→Exporters | Connector 模式（move/copy）、OTTL、Extension |
| **CloudEvents CESQL** | CE 属性过滤 | 评估后选用 CEL 替代（语法更协调、类型系统更强、K8s 生态） |
| **Cloudera Envelope** | Steps + HOCON | `dependencies` 内联边、Translator 层、Decision/Task 步骤、DQ rules（Planner 不采纳，写入语义由各 Sink 自管） |

完整竞品调研详见 `competitor-research.md`，设计评审详见 `design-review.md`。

### 2.2 核心设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 向后兼容 | 不兼容 v1 — 全新架构 | 从架构层重新设计 |
| 管道拓扑 | DAG（有向无环图） | 渐进复杂度，无环约束 |
| 事件模型 | 通用 Message（`[]byte` + metadata） | 协议无关，CloudEvents 作为协议插件一等支持 |
| 数据转换 | eql = CEL 表达式层 + 赋值扩展 | CEL 有 cel-go 生产级实现、K8s 生态、类型系统、conformance TCK |
| 部署模型 | 双模式（单二进制 + K8s Operator） | 核心引擎统一 |
| 扩展机制 | Go 编译时注册 + WASM（wazero） | Go 为性能主路径，WASM 为灵活性补充；v2 加 gRPC 进程外插件 |
| 多 Pipeline | v1 支持单进程多 Pipeline | 对标 Benthos streams mode，资源效率 |
| Codec 体系 | 统一 Codec（Decode+Encode），Source decoder / Sink encoder | 连接器矩阵 N×M → N+M |
| 写入语义 | **由各 Sink 插件自行定义**（`config` 字段，无引擎级 Planner / `write_mode` 统一约定） | 事件路由器定位；复杂 ETL 语义在专用 sink 或 transform 内实现 |
| 赋值语义 | immutable 语义 + COW 实现 | 与 CEL 一致，fan-out 安全，纯路由零开销 |
| DLQ 格式 | passthrough + `er-*` metadata | 对标 Kafka Connect headers / Knative CE extensions |
| 配置格式 | **YAML + HOCON 双格式**；**结构对齐 Envelope**（`steps` 嵌套块），**字段名保持 edgestream** | 见 §8 |

---

## 3. 整体架构

### 3.1 六层架构

```
┌──────────────────────────────────────────────────────┐
│                 Configuration Layer                   │
│  YAML / HOCON / K8s CRD  →  统一 IR  →  校验 & 默认值  │
├──────────────────────────────────────────────────────┤
│                  Topology Layer                       │
│      DAG 构建  →  拓扑排序  →  Stage 实例化          │
├──────────────────────────────────────────────────────┤
│                   Engine Layer                        │
│    Message 调度  →  背压控制  →  重试/DLQ  →  Ack    │
├──────────────────────────────────────────────────────┤
│                 Component Layer                       │
│  Source / Transform / Sink / Codec 接口              │
│  +  插件实现（Go 编译时 + WASM）                      │
├──────────────────────────────────────────────────────┤
│              Infrastructure Layer                     │
│     Metrics ─ Tracing ─ Logging ─ Notifications      │
└──────────────────────────────────────────────────────┘
```

### 3.2 关键架构概念

| 概念 | 说明 |
|------|------|
| **Step** | **配置层**书写单元：`steps.{name}` 块（可含 `source`/`transform`/`sink` 子块 + `depends_on`）；合体 step 可展开为多个 stage |
| **Stage** | **运行时** DAG 顶点：`StageIR` 实例，对应 Source / Transform / Sink 之一；由 step 规范化/展开得到 |
| **Edge** | **运行时** DAG 有向边：`EdgeIR`，由下游 step 的 `depends_on` 展开；含 condition / buffer / delivery / route / required |
| **Message** | `[]byte` payload + `map[string]any` metadata + 惰性 ParsedData，协议无关 |
| **Codec** | 格式编解码器（Decode+Encode），Source 用 decoder，Sink 用 encoder |
| **ParsedData** | 惰性解析后的 payload 结构（`any`，v1 为 `map[string]any`） |
| **COW** | Copy-on-Write 消息复制——ShallowCopy 共享 ParsedData + atomic readOnly flag，首次写才 clone |
| **refCount** | 消息到达的 Sink 副本数，fanOut 时动态计算，归零触发 Ack |
| **BatchCollector** | 引擎拥有的攒批工具，攒满后调 Sink.Write |
| **Pipeline** | 一个 TopologyIR 的运行时实例，独立 Stage/Edge/refCount/Ack |
| **Engine** | 管理多个 Pipeline 的进程级运行时 |

---

## 4. 核心接口与 Message 模型

### 4.1 Message

```go
type Message struct {
    ID        string           // 唯一标识，引擎自动生成
    Payload   []byte           // 事件负载 — 任意格式的字节序列（原始值，re-serialize 前不动）
    Metadata  map[string]any   // 元数据 — 携带上下文信息
    
    // 引擎内部字段 — 不对外暴露
    parsedData    any          // 惰性解析后的结构（nil = 未解析）
    parsedDirty   bool         // ParsedData 是否被 Transform 修改
    parsedCodec   string       // 声明的 codec 名（用于惰性 parse）
    readOnly      atomic.Bool  // COW flag — true 时 parsedData 共享只读，首次写需 clone
    originalPayload []byte     // re-serialize 前备份的原始 payload（DLQ 用）
    ctx           context.Context
    ackFn         func(error)  // 引擎注入的确认回调
    errCount      int          // 累计错误次数
}
```

**设计要点：**

- `Payload` 是 `[]byte` — 引擎不关心内容，解析由 Codec 按需完成
- `parsedData` 惰性解析 — 首次 `payload.*` 访问或 Transform 写 payload 路径时才调 `codec.Decode(payload)`
- `parsedDirty` 标记 — Transform 修改 ParsedData 后置 true，序列化推迟到 Sink 前
- `readOnly` COW flag — fan-out ShallowCopy 时共享 parsedData + 置 readOnly=true，Transform 首次写时检测 flag → clone-on-first-write
- `originalPayload` — 引擎在 re-serialize 前备份原始 Payload（切片引用，零拷贝），Sink error 时 DLQ 从此取原始 payload
- `ackFn` 引擎私有注入 — 管 refCount，归零时触发；不对外暴露

### 4.2 Stage

```go
type Stage interface {
    ID() string
    Kind() StageKind           // source | transform | sink（角色）
    ComponentType() string     // kafka | map | http…（配置 type 字段，组件实现名）
    Init(ctx context.Context) error
    Stop(ctx context.Context) error
    HealthCheck(ctx context.Context) HealthStatus
}

type StageKind int
const (
    StageKindSource StageKind = iota
    StageKindTransform
    StageKindSink
)

type HealthStatus struct {
    Healthy bool
    Message string
    Since   time.Time
}
```

### 4.3 Source

```go
// Source — 生产消息（DAG 入口，无入边）
type Source interface {
    Stage
    Consume(ctx context.Context, out chan<- *Message) error
}

// AckingSource — 可选接口，支持 at-least-once 的 Source 实现
type AckingSource interface {
    Source
    OnAck(msg *Message, err error)  // 引擎在消息 Ack 时回调
}

// PollingSource — pull 源辅助接口（框架提供定时调用 + 并发控制）
type PollingSource interface {
    Stage
    Poll(ctx context.Context) ([]*Message, error)
    Interval() time.Duration
}
```

**push 源**（Kafka/HTTP）：实现 `Consume`，持续产出消息到 out channel，背压通过 channel 阻塞传导。

**pull 源**（S3/SQS/CDC）：实现 `PollingSource`，引擎 wrapper 按 Interval 定时调用，空轮询退避（×2，上限 max_interval 默认 5min），pending 队列管理 + 背压时跳过本轮 Poll。

**Ack 机制**：
- 引擎注入 `ackFn` 管 refCount（Message 私有字段）；
- ackFn 触发时引擎调 `Source.OnAck(msg, err)`（如果 Source 实现了 AckingSource）；
- Kafka Source 实现 AckingSource，在 OnAck 里管 per-partition low-watermark offset commit；
- HTTP/Cron 不实现 AckingSource，ackFn 触发时 no-op + 日志；
- Source 文档标注 at-least-once / at-most-once 能力。

### 4.4 Transform

```go
// Transform — 转换消息（DAG 中间节点，有入边有出边）
// 批次入、批次出 — 支持窗口聚合、fan-in、去重等场景
type Transform interface {
    Stage
    Process(ctx context.Context, batch []*Message) ([]*Message, error)
}
```

- 接收一个 batch 返回零个或多个消息；
- 返回 nil → 丢弃所有消息；
- 返回多条 → 拆分（fan-out via split）；
- Transform 返回新值不 mutate 入参（immutable 语义 + COW 实现）。

### 4.5 Sink

```go
// Sink — 消费消息（DAG 出口，无出边）
type Sink interface {
    Stage
    Write(ctx context.Context, msgs []*Message) error   // 引擎攒好 batch 后调用
    Flush(ctx context.Context) error                      // 停机时引擎调
    Stop(ctx context.Context) error                       // 资源释放
}
```

- **引擎拥有攒批** — 引擎在 Sink 前内置 BatchCollector（size/timeout/max_bytes），攒满后调 `Sink.Write(msgs)`；
- Sink 作者只实现写入逻辑，不实现攒批；
- timeout flush 由引擎内部 timer 触发；
- 停机时引擎调 `Flush()`（强制写出 BatchCollector 里未满的残余 batch）；
- 部分失败 = 整 batch retry（Write 返回 error），v2 可扩展 per-message error。
- **写入语义** — 引擎不定义统一的 `write_mode` 或 Mutation 模型；append/upsert/SCD2 等由**各 Sink 插件**在 `config` 中自行声明并实现（如 Kafka 的 `idempotent`、`http` 的 `method`）。Sink 通过 `ValidateConfig` 在启动期校验自身配置。

### 4.6 Edge

```go
type Edge struct {
    From      string
    To        string
    Condition string             // CEL 表达式，空 = 始终转发
    Route     string             // route Transform 的命名路由（自动生成 condition）
    Buffer    EdgeBufferConfig
    Delivery  *DeliverySpec      // retry/timeout/DLQ，正交于路由
    Required  *bool              // 默认 true；false = best-effort 边（Flume 启发）
}

type EdgeBufferConfig struct {
    Type      BufferType         // memory | disk | overflow
    Size      int                // memory buffer 大小，默认 64
    Strategy  BufferStrategy     // block | drop_newest | drop_oldest
    Key       []string           // 分区键（Fluentd chunk key 风格，v2）
    DiskPath  string             // type=disk/overflow 时
    DiskMaxSize int64            // 磁盘缓冲上限
    DiskSyncInterval time.Duration  // fsync 间隔，默认 500ms
}

type DeliverySpec struct {
    Retry    *RetryConfig
    Timeout  time.Duration
    DLQ      string              // 引用 DLQ sink stage ID
}

type BufferType int
const (
    BufferMemory BufferType = iota
    BufferDisk
    BufferOverflow  // memory 满 → 溢写 disk
)

type BufferStrategy int
const (
    StrategyBlock BufferStrategy = iota
    StrategyDropNewest
    StrategyDropOldest
)
```

---

## 5. Codec 体系

### 5.1 统一 Codec 接口

```go
type Codec interface {
    Decode(payload []byte) (any, error)           // Source 侧（decoder）
    Encode(data any) ([]byte, error)               // Sink 侧（encoder）
    OutputType() cel.Type                          // CEL TypeChecker 用
    ValidateConfig(config map[string]any) error    // 启动期校验
}
```

- **统一 Codec**：一个 Codec 同时实现 Decode 和 Encode，注册一次；
- **Source 用 decoder**，**Sink 用 encoder**，引用同一个 Codec；
- v1 所有 codec 的 Decode 返回 `map[string]any` 或 `[]any`（Protobuf 内部转 map，v2 演进到原生 struct）；
- `OutputType()` 用于 CEL TypeChecker：json/avro/csv 返回 `cel.MapType(cel.StringType, cel.DynType)`。

### 5.2 Codec 配置共享

顶层 `codecs:` 段声明命名 codec 配置，Source/Sink 用 `ref` 引用：

```yaml
codecs:
  - name: avro-orders
    type: avro
    config:
      schema: { "type":"record","name":"Order",... }
      registry: confluent
      registry_url: http://schema-registry:8081

steps:
  kafka-in:
    source:
      type: kafka
      decoder: { ref: avro-orders }
      config: { brokers: [...], topics: [orders] }
  kafka-out:
    depends_on: [kafka-in]
    sink:
      type: kafka
      encoder: { ref: avro-orders }
      config: { topic: orders-out }
```

> 平坦 `pipeline[]` 等价写法见 §8.8；codec `ref` 规则对两种形态相同。

**同时支持 inline 声明**（简单 codec 无需 ref）：

```yaml
decoder: json               # 字符串简写（无配置 codec）
decoder: { type: json }     # 对象形式（有配置时）
decoder: { ref: avro-base, config: { schema: "..." } }  # ref + Stage 级 config shallow merge 覆盖
```

### 5.3 校验规则

- `ref` 引用不存在的 codec name → 启动期报错；
- codec name 重复 → 启动期报错；
- codec `type` 不是已注册的 codec 插件 → 启动期报错；
- `ValidateConfig` 失败 → 启动期报错；
- ref + Stage 级 config = shallow merge（顶层 key 覆盖，不递归合并）。

### 5.4 ParsedData 惰性双轨

```
Source 产出 Message（Payload=[]byte, parsedData=nil, parsedCodec="json")
    │
    ▼ 首次 payload.* 访问 或 Transform 写 payload 路径
引擎调 codec.Decode(Payload) → parsedData = map[string]any
    │
    ▼ Transform 修改 parsedData（COW: readOnly→clone→改→parsedDirty=true）
    │
    ▼ 交付 Sink 前
引擎检测 parsedDirty:
    true  → 调 codec.Encode(parsedData) → 覆盖 Payload（re-serialize）
            备份原始 Payload 到 originalPayload（零拷贝切片引用）
    false → Payload 保持原始 []byte（零开销，纯路由场景）
    │
    ▼ Sink.Write(msgs)
```

**关键规则：**
- 未配 decoder 时引用 `payload.*` → 启动期静态分析报错（显式报错，不自动探测，不静默 nil）；
- fan-out 时 ShallowCopy Message（共享 parsedData 指针 + readOnly=true）；
- Transform 首次写 parsedData 时检测 readOnly → clone-on-first-write → 后续同 Transform 内原地改；
- 纯路由/filter 场景永不 clone，零开销；
- 无需回滚机制（输入从未被修改）；
- 中间路径自动创建（`payload.a.b.c = 1` 且 `payload.a.b` 不存在 → 自动建空 map）。

### 5.5 Metadata 保留字段同步

引擎自动同步的保留字段（用户写无效，被覆盖）：

| 保留字段 | 谁填 | 何时刷新 |
|---|---|---|
| `content-type` | Sink encoder 推断 | Sink 序列化时 |
| `content-length` | 引擎 | Sink 序列化后 |
| `ce-specversion` / `ce-id` / `ce-type` / `ce-source` / `ce-subject` | CE Source 插件 | Source 入口 |
| `ce-time` | CE Source 或引擎 | Source 入口（可选 Transform 后刷新） |

业务字段（`kafka.key` / `trace_id` / hash 等）用户负责。所有非保留字段 = 用户字段，引擎不触碰。

---

## 6. 引擎运行时

### 6.1 核心概念

```
                        ┌──────────────────────────────────┐
                        │            Engine                 │
                        │                                  │
  Source.Consume() ──▶  │  fanOut  ──[Edge Buf]── fanIn   │──▶ Transform.Process()
                        │    │                            │         │
                        │    │  条件路由                    │    fanOut│
                        │    ├──[Buf]──▶ Sink.Write()     │         │
                        │    │                            │    ┌────┴────┐
                        │    └──[Buf]──▶ Sink.Write()     │    │ 条件路由 │
                        └──────────────────────────────────┘    └────┬────┘
                                                              ┌────┴────┐
                                                           [Buf]     [Buf]
                                                              │        │
                                                           Sink A   Sink B
```

| 概念 | 说明 |
|------|------|
| **fanOut** | 拿到一条 Message，对每条出边评估 Condition（CEL 表达式）；匹配则克隆消息（ShallowCopy + readOnly）送入边缓冲 |
| **fanIn** | goroutine，select merge 多条入边的消息到一个 channel，供下游 Stage 消费 |
| **Edge Buffer** | 有界 Channel，配置 `type`（memory/disk/overflow）+ `strategy`（block/drop_newest/drop_oldest） |
| **refCount** | fanOut 时动态计算 = 本次实际匹配的出边数（非拓扑可达 sink 数），每条匹配边克隆的消息持有同一个原子计数器 |
| **BatchCollector** | 引擎在 Sink 前内置的攒批工具（size/timeout/max_bytes），攒满后调 Sink.Write |
| **COW** | ShallowCopy 共享 ParsedData + atomic readOnly flag，Transform 首次写时 clone-on-first-write |

### 6.2 背压传播

```
Sink(http) 写入慢
    │
    ▼
write dispatch channel 满（所有 writer goroutine 在 Write 中）
    │
    ▼
batch collector 阻塞（无法投递新 batch）
    │
    ▼
Edge Buffer 满 (strategy=block)
    │
    ▼
fanOut 发送阻塞
    │
    ▼
Transform output channel 满 → worker 阻塞
    │
    ▼
Transform 的 fanIn channel 堆积
    │
    ▼
上游 Edge Buffer 满
    │
    ▼
Source.Consume() 的 out channel 阻塞
    │
    ▼
Source 内部拉取变慢（Kafka consumer pause / HTTP 429）
```

背压是**自动传播**的 — 只要边缓冲设对策略，不需要引擎显式控制。

### 6.3 Ack 链路

```
Source(Kafka) 产出 msg
    │
    ▼ 引擎注入 ackFn（管 refCount）
    │
fanOut 评估出边 condition → 匹配 2 条边 → refCount=2
    ├──▶ Sink A  ✅  refCount → 1
    └──▶ Sink B  ✅  refCount → 0
               │
               ▼
          ackFn(nil) → 引擎调 Source.OnAck(msg, nil) → Kafka Source commit offset
```

**失败处理：**

```
fanOut 匹配 2 条边 → refCount=2
    ├──▶ Sink A  ✅  refCount → 1
    └──▶ Transform ❌ err（重试耗尽）
              │
              ▼
          refCount → 0, ackFn(error)
              │
              ▼
          引擎调 Source.OnAck(msg, err) → Kafka 不 commit offset → 重启重投
```

**关键规则：**
1. refCount = fanOut 时**实际匹配的出边数**（动态计算，非静态可达 sink 数）
2. 任何路径失败（重试耗尽后），立即触发 Ack 并带上错误
3. Source 收到 OnAck 后自行决定：确认（commit offset）/ 不确认（重投）/ 投 DLQ
4. Sink 级 DLQ 可在 Sink 插件内实现 — 引擎不强制

**Transform split 的 Ack 聚合：**

Transform 返回 N 条子消息时，引擎自动建立"子消息 refCount → 父消息 ackFn"的聚合链——子消息全部完成（成功或失败）后父消息才 Ack（Benthos split 模型）。Transform 作者不处理 Ack。

### 6.4 goroutine 模型

```
Pipeline（per Pipeline supervisor goroutine — panic recover + graceful stop）
│
├── Source-1: Consume goroutine (1)
│     └── out channel
│           └── [edge buffer channel]
│                 └── Transform-A: fanIn goroutine (1) → in channel
│                       ├── worker goroutine (N)  ← 从 in channel 竞争消费
│                       │     └── output channel
│                       └── fanOut goroutine (1)  ← 从 output channel 读
│                             ├── [edge buffer channel] → Transform-B fanIn
│                             └── [edge buffer channel] → Transform-C fanIn
│
├── Transform-B: fanIn (1) → workers (N) → fanOut (1)
│     └── [edge buffer channel] → Sink-1
│
├── Sink-1:
│     batch collector goroutine (1)     ← 从 edge buffer 拉 → 攒批 → 投递 write dispatch channel
│     writer goroutine (max_in_flight)  ← 默认 1，从 write dispatch channel 竞争取 batch → Sink.Write
│
└── Engine supervisor goroutine (1 per 进程)
      信号处理 + Pipeline 生命周期 + metrics/health endpoint
```

**per-Stage goroutine 数量：**

| Stage 类型 | goroutine 数 | 公式 |
|---|---|---|
| Source（push） | 1 | Consume |
| Source（pull） | 1 | PollingSource wrapper |
| Transform | 2 + workers | fanIn(1) + workers(N) + fanOut(1) |
| Sink | 1 + max_in_flight | batch collector(1) + writer(max_in_flight) |

**Sink max_in_flight + ordering：**

```yaml
sink:
  ordering: ordered      # 默认 — max_in_flight 强制为 1（启动期校验冲突报错）
  # 或
  ordering: unordered    # 允许 max_in_flight > 1，不保证顺序
  max_in_flight: 3       # 默认 1
```

- `ordered`：max_in_flight 必须为 1，保证消息顺序；
- `unordered`：允许 max_in_flight > 1，1 batch collector + N writer goroutine 并发 Write（对标 Benthos AsyncWriter），不保证顺序；
- per-partition ordering（Option C）v2 引入（与 buffer.key 一起）；
- offset/ack 下沉 Source 插件（low-watermark 连续 commit，引擎不感知 offset）。

### 6.5 重试与 DLQ

**错误分类与处理：**

| 错误类型 | 位置 | retry | DLQ |
|---|---|---|---|
| DSL error / Codec decode error / edge condition error | Transform / 边 condition | 默认**不重试**（确定性错误） | 边 `delivery.dlq` → pipeline `dlq` |
| Sink 写入失败 | Sink | 入边 `delivery.retry`（默认重试，指数退避） | 边 `delivery.dlq` → pipeline `dlq` |
| Source 读取失败 | Source | Source 内部处理 | N/A |
| DSL 编译错误 | 启动期 | N/A | 启动失败 |

**DLQ fallback 链：** 入边 `delivery.dlq`（引用 sink stage id）→ pipeline 级 `dlq.sink` → 丢弃 + `edgestream_dlq_enqueued_total` error metric。

> 重试与 DLQ **不在 `StageIR` 上配置**；全部由 `depends_on` 展开后的 `EdgeIR.delivery` 与 pipeline 顶层 `dlq` 表达。Transform 的条件应用使用 `transform.predicate`（Kafka Connect 风格），**无**边级 `predicate` 字段。

**DLQ 是 Sink stage 引用**（非独立概念）：

```yaml
dlq:
  sink: dlq-kafka          # 引用 pipeline 内 sink stage id
  include_current_payload: false

steps:
  dlq-kafka:
    sink:
      type: kafka
      config: { topic: orders-dlq, ... }
```

**DLQ 消息格式：** passthrough 模式——DLQ 消息 payload = **原始 payload**（进入 pipeline 时的字节）；error 信息写入 metadata 用 `er-` 前缀。

| 字段 | 说明 |
|---|---|
| `er-error-reason` | 人类可读错误描述 |
| `er-error-type` | `dsl_function_error` / `codec_decode_error` / `sink_write_error` / `edge_condition_error` / `wasm_error` |
| `er-error-stage` | 错误发生的 stage ID |
| `er-error-timestamp` | error 发生时间 RFC 3339 |
| `er-original-pipeline` | pipeline 名称 |
| `er-original-source` | Source stage ID |
| `er-retry-count` | 重试次数（确定性错误为 0） |
| `er-original-topic` | Kafka topic（如适用） |
| `er-original-partition` | Kafka partition（如适用） |
| `er-original-offset` | Kafka offset（如适用） |
| `er-current-payload` | error 发生时的当前 payload base64（仅 include_current_payload=true） |

### 6.6 优雅停机与热加载

**停机顺序：**

```
Stop 信号 (SIGTERM/SIGINT)
    │
    ▼
1. 通知所有 Source.Stop() — 停止产出新消息
    │
    ▼
2. 等待所有 Pipeline 中所有 in-flight Message refCount → 0（drain_timeout 默认 30s）
    │
    ▼
3. drain 超时 → drop 在途消息 + Source 不 Ack（offset 不 commit，下次重投）
    │  （在途消息不进 DLQ — 非 error 消息；at-least-once 保证不丢）
    │
    ▼
4. 关闭所有 Transform 的 in channel → worker 自然退出
    │
    ▼
5. 调用所有 Sink.Flush() → 触发 BatchCollector 写出残余 batch
    │  Flush 失败 → 不 Ack（下次重投）
    │
    ▼
6. Sink.Stop() → Transform.Stop() → Source 资源释放
```

**热加载（单 Pipeline 级）：**

```
POST /admin/reload/{pipeline_name}  或  SIGHUP（全进程）
    │
    ▼
1. 读取新配置（不停止旧 Pipeline）
    │
    ▼
2. 解析 + 校验新 TopologyIR（全部启动期校验规则）
    │
    ├─ 校验失败 → 保留旧 Pipeline + HTTP 400 + 错误详情
    │
    ▼ 校验通过
3. graceful stop 旧 Pipeline（drain → stop）
    │
    ▼
4. 用新 TopologyIR 构建全新 DAG → 启动新 Pipeline
    │
    ▼ 30-60s gap（drain + 新启动，接受，对标 Benthos）
5. HTTP 202 + task ID（异步，轮询 status）
```

**热加载 API：**

| API | 行为 |
|---|---|
| `POST /admin/reload` | 全进程所有 Pipeline 重载 |
| `POST /admin/reload/{name}` | 单 Pipeline 重载 |
| `GET /admin/reload/status/{task_id}` | 轮询重载状态 |
| `GET /admin/pipelines` | 列出所有 Pipeline 及状态 |
| `GET /admin/pipelines/{name}/status` | 单 Pipeline 状态 |
| 409 | 该 Pipeline 正在重载中（拒绝并发重载） |

v1 不做 fsnotify 文件监听（v2 评估）。K8s Operator 通过调 `POST /admin/reload/{name}` 触发重载。

### 6.7 多 Pipeline 隔离

单进程多 Pipeline，每 Pipeline 独立 Stage/Edge/refCount/Ack。

**隔离级别（档②中等隔离）：**

| 资源 | 隔离方式 |
|---|---|
| goroutine / worker | per-Pipeline `max_workers` 上限 |
| 内存 | per-Pipeline `max_inflight_messages` 上限（超限反压 Source） |
| Edge buffer | 每 Pipeline 独立（天然隔离） |
| refCount/Ack | 每 Pipeline 独立（天然隔离） |
| Source 连接 | 每 Pipeline 独立（不共享 Stage 实例） |
| panic | goroutine recover 防单 Pipeline panic 杀进程 |
| metrics/tracing | 共享 endpoint + `pipeline` label 区分 |
| 热加载 | 单 Pipeline 级 |

```yaml
engine:
  max_workers_per_pipeline: 16       # 全局默认
  max_inflight_per_pipeline: 10000   # 全局默认

# pipeline 级可覆盖
spec:
  engine:
    max_workers: 32
    max_inflight: 50000
```

### 6.8 Edge Disk Buffer

**WAL 格式：** `[msg_len(4B)][payload(msg_len)][meta_len(4B)][metadata(meta_len)]` 二进制追加。

**分段文件：** 每文件默认 64MB（`disk_segment_size` 可配）。

**fsync：** 周期 fsync，默认 500ms（`disk_sync_interval` 可配）。

**恢复：** 启动时扫描所有 segment 文件，按文件名排序重建消息队列。独立 offset 文件记录每 segment 消费位置（周期 fsync）。segment 全消费后删除。

**overflow：** `type: overflow` = memory 满 → 溢写 disk，disk 满 → `when_full: block|drop_newest|drop_oldest`。从 disk 读时先排空 disk 再回 memory channel。

**崩溃恢复：** WAL 恢复的消息重新走 pipeline，接受 at-least-once 重复（用户幂等兜底）。

**disk buffer 的核心价值：** 解耦 Source 消费速度与 Sink 写入速度——Sink 慢时消息存磁盘而非阻塞 Source。

---

## 7. DSL 语言设计 (eql)

### 7.1 设计目标

eql = **CEL 表达式层**（谓词 + 值计算）+ **eql 赋值扩展**（`.path = expr`、`del()`）+ **edgestream 注册的 CEL 自定义函数**。

| 目标 | 说明 |
|------|------|
| CEL 原生表达式 | if/else macro、函数调用、算术、比较、逻辑 — cel-go 内置 |
| 赋值扩展 | `path = expr`（eql 扩展，CEL 无赋值）、`del(path)`（eql 扩展语句） |
| 无管道、无模板 | 不引入 `\|` 和 `${}`——多步变换用函数嵌套，字符串拼接用 `format()` 或 `+` |
| 无 statement-level if | v1 用 if 表达式 + else 分支引用原值；v2 视需求加 |
| 安全运行 | 纯 Go 实现（cel-go），编译期类型检查 |

### 7.2 路径语法

统一 CEL 语法（**无点号前缀**，推翻 jq 风格）：

```
# CEL 变量
payload         — ParsedData（惰性解析后的 payload 结构）
metadata        — Metadata map
input           — 原始输入快照（只读，赋值前的原始值）

# 路径导航
payload.total                — 字段访问
payload.orders[0].id         — 数组索引 + 链式导航
metadata["content-type"]     — 含特殊字符的 key 用 []
metadata.trace_id            — 标识符安全的 key 用 .
```

### 7.3 赋值（Mapping）

```eql
# 字段赋值
payload.total = payload.price * payload.quantity

# 新建嵌套对象（中间路径自动创建）
payload.enriched.source = "order-service"
payload.enriched.timestamp = now()

# 从 metadata 复制到 payload
payload.trace_id = metadata.trace_id

# 条件赋值（CEL 原生 if 表达式）
payload.priority = if payload.total > 1000 { "high" } else { "normal" }

# 删除字段
del(payload.internal)
del(metadata["x-debug"])

# 整体替换
payload = json(payload)

# 跨命名空间赋值
metadata["x-trace"] = payload.trace_id
```

**赋值语义（immutable + COW）：**
- 每条赋值在 COW 副本上 set 字段；
- 后续赋值的右边读**当前副本**（含之前所有赋值结果，累积可见）；
- `input.x` 访问原始输入快照（只读，不受赋值影响）；
- CEL 表达式层无状态，赋值层有状态顺序执行，两层职责分离。

### 7.4 过滤（Filter / Edge Condition）

纯 CEL 表达式，求值为 Boolean：

```eql
# 比较
payload.total > 100
payload.status == "paid"

# 集合
metadata.region in ["us", "eu", "apac"]
payload.tags.contains("vip")           # CEL 原生 contains
payload.email.matches(r'^[a-z]+@.*\.com$')  # CEL 原生 matches

# 存在性
has(payload.email)                      # CEL 原生 has()
!has(metadata.debug)

# 逻辑
payload.total > 100 && metadata.region == "us"
payload.status == "paid" || payload.status == "confirmed"

# 时间比较
now() - payload.created > duration("1h")
```

### 7.5 函数库

**CEL 原生函数**（camelCase，cel-go 内置）：

| 函数 | 说明 |
|---|---|
| `int(v)` / `double(v)` / `string(v)` / `bool(v)` | 类型转换 |
| `type(v)` | 返回类型 |
| `size(s)` / `size(arr)` | 长度 |
| `contains(s, sub)` | 包含检查 |
| `matches(s, regex)` | 正则匹配 |
| `has(field)` | 存在性检查 |
| `all()` / `exists()` / `exists_one()` / `map()` / `filter()` | 宏 |
| `format(fmt, args)` | 格式化字符串 |
| `timestamp(s)` | RFC 3339 字符串转 timestamp |
| `duration(s)` | 字符串转 duration |
| `a ?? b` | coalesce（第一个非 nil 值） |

**edgestream 注册函数**（snake_case，通过 cel-go `cel.Function()` 注册）：

| 类别 | 函数 | 说明 |
|------|------|------|
| **时间** | `now()` | 当前 UTC 时间（返回 cel.TimestampType） |
| | `parse_time(s, layout)` | strftime layout 解析 |
| | `format_time(t, layout)` | strftime layout 格式化 |
| **字符串** | `uppercase(s)` / `lowercase(s)` | 大小写转换 |
| | `trim(s)` / `trim_prefix(s, p)` | 修剪 |
| | `split(s, sep)` | 分割为数组 |
| | `join(arr, sep)` | 拼接为字符串 |
| | `replace(s, old, new)` | 替换 |
| **数学** | `min(a, b)` / `max(a, b)` | 最小/最大值 |
| | `abs(n)` / `ceil(n)` / `floor(n)` | 数学运算 |
| **JSON** | `json(v)` | 解析 JSON 字符串为 map |
| | `to_json(v)` | 序列化为 JSON 字符串 |
| **类型** | `array(v)` | 类型转换为数组 |
| | `typeof(v)` | 返回类型名（edgestream 扩展，与 CEL `type()` 不同） |
| **UUID/ID** | `uuid()` | 生成 UUID v4 |
| | `hash(s)` | SHA256 哈希 |
| **编码** | `base64_encode(s)` / `base64_decode(s)` | Base64 编解码 |
| | `coalesce(a, b, ...)` | 返回第一个非 nil 值 |

### 7.6 完整 mapping 示例

```eql
# 解析 JSON 字符串为对象
payload = json(payload)

# 删除内部字段
del(payload._internal)

# 计算
payload.total = payload.price * payload.quantity
payload.discount_amount = payload.total * payload.discount

# 丰富
payload.ingested_at = format_time(now(), "%Y-%m-%dT%H:%M:%SZ")
payload.source = "order-pipeline"
payload.trace_id = metadata.trace_id

# 条件分类（CEL 原生 if 表达式）
payload.tier = if payload.total > 10000 {
    "enterprise"
} else if payload.total > 1000 {
    "business"
} else {
    "standard"
}
```

### 7.7 编译与执行

```
DSL 字符串 → eql Parser（赋值语句 + CEL 表达式）
    │
    ├─ CEL 表达式 → cel-go TypeChecker（类型检查）
    │
    ├─ 静态字段引用检查（有 schema 时，未声明字段 → warning）
    │
    ├─ 路径引用检查（引用 payload.* 但未配 decoder → 启动期报错）
    │
    ▼ 编译为可执行程序
执行阶段（每个 Message）：
    - 赋值语句顺序执行，COW 副本上 set/del
    - CEL 表达式从当前副本读取求值
    - 字段不存在返回 nil（不 panic）
    - 类型不匹配 → false（filter 不通过）
    - 函数调用失败 / 除零 / 非法正则 → error → 按入边 `delivery` / pipeline `dlq` 处理
```

### 7.8 错误处理

| 错误类型 | filter Transform | edge condition |
|---|---|---|
| 字段不存在 | false（不通过）+ debug metric | false（不匹配此边） |
| 类型不匹配 | false（不通过）+ debug metric | false（不匹配此边） |
| 函数调用失败 | error → 入边 delivery / pipeline dlq | error → 入边 delivery / pipeline dlq |
| 除零 / 索引越界 | error → 入边 delivery / pipeline dlq | error → 入边 delivery / pipeline dlq |
| 非法正则 | error → 入边 delivery / pipeline dlq | error → 入边 delivery / pipeline dlq |
| 编译错误 | 启动期失败 | 启动期失败 |

**`error_mode` 配置作用域**（优先级：transform > pipeline > engine 默认）：

| 挂载点 | 字段路径 | 说明 |
|--------|----------|------|
| 全局默认 | `engine.error_mode` | 所有 transform 与边 condition 的默认值 |
| Pipeline 覆盖 | `engine.error_mode`（pipeline `spec.engine` 内） | 单 pipeline 覆盖 |
| Transform 覆盖 | `steps.*.transform.error_mode` 或 `pipeline[].error_mode` | 仅该 transform 的 DSL 求值（含 filter/map） |

| 值 | 行为 |
|---|---|
| `propagate`（默认） | 按上表规则 |
| `ignore` | 所有 error 当 false，不进 DLQ，只记 metric |
| `silent` | 同 ignore 但不记 metric/log |

> 边 `condition` 求值错误遵循同一 `error_mode` 链；`ignore`/`silent` 时 condition error 视为不匹配该边。

### 7.9 编译期检查强度

**v1 档②（CEL TypeChecker + 静态字段引用检查）：**
- CEL 表达式层走 cel-go TypeChecker（`data.total + "foo"` 编译失败）；
- 有 schema 时（Codec 声明 schema / Source 配 `expected_fields`），未声明字段报 **warning**（非 error）；
- 无 schema 时退化为档①（仅 CEL TypeChecker）；
- 路径引用检查（引用 `payload.*` 但未配 decoder → 启动期报错）。

**v2 演进档③（VRL 风格 fallibility 类型系统）：**
- 每个函数标注 fallible / infallible；
- `int(data.total)!`（断言不失败）/ `int(data.total) ?? 0`（兜底）/ `n, err = int(data.total)`（显式处理）；
- 未处理的 fallible 调用编译失败；
- v1 函数内部预留 fallibility 标注，v2 编译器升级后自动激活。

### 7.10 route Transform

route Transform 是 map 的语法糖——内部生成 `if/else if/else` DSL 写 `metadata['er-route']`，引擎不特殊处理。

```yaml
steps:
  classify:
    depends_on: [kafka-in]
    transform:
      type: route
      config:
        action: move          # v1 只做 move（first-match switch-case）；copy（fan-out 多 route）v2
        routes:
          us: "payload.region == 'us'"
          eu: "payload.region == 'eu'"
          _default: "true"    # fallback

  us-sink:
    depends_on:
      classify:
        route: us              # 自动生成 condition: "metadata['er-route'] == 'us'"
    sink:
      type: http
      config: { url: https://us-api.example.com/orders }

  eu-sink:
    depends_on:
      classify: { route: eu }
    sink:
      type: http
      config: { url: https://eu-api.example.com/orders }
```

下游 step 的 `depends_on.{upstream}.route` 等价于原顶层 `edges[].route`；**不再使用独立 `edges:` 段**（见 §8.1.3）。

---

## 8. 配置模型

### 8.1 设计原则

1. **双格式、一 schema** — YAML 与 HOCON 书写等价，解析为同一份 Canonical Schema，再展开为 `TopologyIR`
2. **结构对齐 Envelope、字段名保持 edgestream** — `steps.{name}` + `source` / `transform` / `sink` 子块；字段用 `depends_on`、`decoder`/`encoder`、`type`/`config`、`engine` 等
3. **边即依赖** — 拓扑边**仅**通过各 step 的 `depends_on` 声明（含 per-edge buffer/delivery）；**无**独立顶层 `edges:` 段
4. **渐进复杂度** — 线性：`depends_on: [a, b]`；分支/背压：`depends_on: { upstream: { route, buffer, delivery } }`
5. **统一 IR** — 配置层 **step**（`steps.{name}`）→ 运行时 **stage**（`StageIR`）+ **edge**（`EdgeIR`，由 `depends_on` 展开）；术语见 §3.2
6. **substitution 对等** — 两种格式均支持 `${ENV_VAR}` 与 `${config.path}` 引用（见 §8.2）

#### 8.1.1 与 Envelope 的结构对照（仅布局，字段名各用各的）

| Envelope 结构块 | edgestream 结构块 | edgestream 字段（不变） |
|----------------|---------------|---------------------|
| `application.*` | `engine.*` | `max_workers`、`max_inflight` 等 |
| `steps.{name}` | `steps.{name}` | step 名默认即展开后的 stage id（合体 sink 为 `{name}-sink`） |
| `steps.{name}.dependencies` | `steps.{name}.depends_on` | 序列或映射（见 §8.1.3） |
| `steps.{name}.input` | `steps.{name}.source` | `type`、`decoder`、`config` |
| `steps.{name}.deriver` | `steps.{name}.transform` | `type`、`config`、`predicate`、`workers` |
| `steps.{name}.output` | `steps.{name}.sink` | `type`、`encoder`、`batch`、`config` |
| `udfs` | `udfs` / `cel.functions` | 可选，v2 评估 |

> Envelope 的 `planner` 块**不映射**为 edgestream 配置项；写入语义由 **sink 的 `config`** 自行表达。
>
> Envelope 用户迁移：`input`→`source`，`deriver`→`transform`，`output`→`sink`；边写在下游 `depends_on`，不要另建 `edges` 列表。

#### 8.1.2 字段 `kind` 与 `type`

| 字段 | 含义 | 出现位置 |
|------|------|----------|
| **`type`** | 组件实现名（`kafka`、`map`、`http`…） | `steps.*.source/transform/sink`；平坦 `pipeline[]` |
| **`kind`** | Stage 角色（`source` / `transform` / `sink`） | 仅平坦 `pipeline[]`（`steps` 子块由块名隐含角色） |

加载器校验：`type` 须在注册表中存在，且与 `kind`（或子块角色）匹配。Go `Stage` 接口用 `Kind()` / `ComponentType()` 对应配置 `kind` / `type`，避免与 `Type()` 混淆。

#### 8.1.3 `depends_on`：唯一边配置语法

`depends_on` 是 **steps 模式与平坦 pipeline 模式共用的边声明**，加载后展开为 `[]EdgeIR`。

**值形态（二选一，不可混用顶层形态）：**

| 形态 | YAML 示例 | 说明 |
|------|-----------|------|
| **序列** | `depends_on: [kafka-in, enrich]` | 元素为字符串（上游 id）或单键对象（见下） |
| **映射** | `depends_on: { kafka-in: {}, splitter: { route: us } }` | 键 = 上游 id；值 = 边属性（`{}` = 仅默认） |

**序列元素规则：**

| 元素 | 示例 | 展开 |
|------|------|------|
| 字符串 | `- kafka-in` | `EdgeIR{from: kafka-in, to: self}` + `edgeDefaults` |
| 单键对象 | `- splitter: { route: eu }` | `from` = 键名；`to` = 当前 step/stage id；值合并为边属性 |

```yaml
# 序列形态
depends_on:
  - kafka-in
  - splitter: { route: eu }

# 映射形态（与上等价，HOCON 更常用）
depends_on:
  kafka-in: {}
  splitter:
    route: us
    buffer: { type: memory, size: 128 }
    delivery:
      retry: { max: 3, backoff: exponential }
      dlq: dlq-sink
    required: false
```

**边属性字段**（对象 value，均可选；缺省取 `edgeDefaults`）：

| 字段 | 说明 |
|------|------|
| `condition` | CEL 表达式；空 = 始终匹配 |
| `route` | route transform 命名路由；展开为 `condition: "metadata['er-route'] == '{route}'"` |
| `buffer` | `type` / `size` / `strategy` / `key` / disk 相关 |
| `delivery` | `retry` / `timeout` / `dlq` |
| `required` | 默认 `true`；`false` = best-effort 边 |

**规则：**

- `route` 与 `condition` **二选一**；同时出现 → 启动期报错
- fan-out：每个**下游** step 在自己的 `depends_on` 里声明入边（与 Vector `inputs` 同思路）
- fan-in：序列 `[a, b]` 或映射 `a: {}`, `b: {}`
- 条件应用 Transform：在 `transform.predicate` 配置 CEL，**不**在边上重复
- 顶层 **`edges:` 已废弃** — 解析器 v1 仍接受并合并（同 `(from,to)` 时 `edges` 字段覆盖 `depends_on`），`validate` 输出 deprecation warning；v2 移除

**Pipeline 顶层保留**（与 `steps` 并列，非边列表）：

```yaml
edgeDefaults: { buffer: { type: memory, size: 64, strategy: block } }
dlq: { sink: dlq-sink }
codecs: [...]
engine: { ... }
steps: { ... }
```

### 8.2 双格式支持（YAML + HOCON）

#### 8.2.1 格式分工

| 格式 | 典型场景 | 说明 |
|------|---------|------|
| **YAML** | K8s CRD、`edgestream run --config-dir`、CI `validate` | 默认格式；`apiVersion`/`kind`/`metadata` 仅 YAML/CRD 使用 |
| **HOCON** | Envelope 风格手写、本地 `.conf` | 扩展名 `.conf` / `.hocon`；**结构与 Envelope 对齐，字段名仍用 edgestream** |

两种格式**能力完全对等**：同一 pipeline 可用任一种写出，展开后 IR 字节级一致（除 `metadata` 等 K8s 包装字段）。

#### 8.2.2 格式识别、CLI 与 HOCON 库选型

**已锁定：** [`github.com/gurkankaymak/hocon`](https://github.com/gurkankaymak/hocon)（MIT，Lightbend Config 官方列出的 Go port）。

| 候选库 | 结论 |
|--------|------|
| **`gurkankaymak/hocon`** | **采用** — 维护活跃（v1.2.23+）、功能覆盖 edgestream 所需 substitution / 嵌套对象 / duration |
| `o3co/go.hocon` | 备选 — spec 合规清单更全，但过新、生产验证少；Envelope 迁移遇解析差异时再评估 |
| `sopranoworks/gekka-config` | 不采用 — Pekko 生态向，社区体量小 |
| `en-vee/aconf` | 不采用 — 2019 年后停更 |

**集成路径（与 YAML 汇合）：**

```text
.conf/.hocon → hocon.ParseFile → Config 树遍历 → map[string]any
       → PipelineConfig 规范化（与 YAML strict unmarshal 之后相同）
       → depends_on 展开 → TopologyIR
```

> **不要**将 HOCON 直接 unmarshal 进 `PipelineConfig` struct — `steps.{name}` 为动态 map key，须先落成通用树再规范化。
>
> **Substitution：** `${VAR}` / `${?OPT}` 由 `gurkankaymak/hocon` 在 `Get*` / `GetStringSlice` 等读取时解析（含环境变量回退）。生产加载器在树规范化阶段须走已解析值，勿对未解析的 `Substitution` 节点直接序列化。

**edgestream 依赖的 HOCON 特性（须在 spike 中验收）：**

| 特性 | 配置示例 | 用途 |
|------|----------|------|
| 嵌套块 | `steps { kafka-in { source { ... } } }` | Envelope 布局 |
| 序列 `depends_on` | `depends_on = [kafka-in, enrich]` | 线性 / fan-in |
| 映射 `depends_on` | `depends_on { splitter { route = us } }` | 分支 + per-edge 属性 |
| `${KAFKA_BROKERS}` | `brokers = [${KAFKA_BROKERS}]`（勿写 `["${VAR}"]`，否则可能不解析） | 环境变量 |
| 可选替换 | `${?OPTIONAL}` | 未设置则省略 |
| duration | `batch { timeout = 1s }` | Sink batch |
| 无引号 key | `group_id = order-consumer` | 手写友好 |

**已知局限（可接受）：** 不使用 HOCON `include`；`classpath` 语义不适用。若用户配置触发 [#47](https://github.com/gurkankaymak/hocon/issues/47) 等边界 case，在 `validate` 中报清晰解析错误。

**Spike 验收（实现 v2.0-alpha 前必须通过）：** 见仓库 `spike/hocon/`，`go test ./...` 覆盖：

1. §8.3 线性 HOCON 样例可解析，`steps` 下 4 个 step 名可枚举  
2. `depends_on` 映射形态（`route`）可读取  
3. `${VAR}` 环境变量替换、`${?OPT}` 可选替换行为符合 §8.2.4  
4. 解析树可无损转为 `map[string]any`（供后续 `PipelineConfig` 加载器消费）

```bash
edgestream run --config pipeline.yaml          # YAML
edgestream run --config pipeline.conf        # HOCON（按扩展名）
edgestream run --config pipeline.conf --format hocon   # 显式指定（扩展名非标准时）
edgestream validate --config-dir ./pipelines/  # 目录内 .yaml/.yml/.conf/.hocon 均可混放
```

| 扩展名 | 解析器 |
|--------|--------|
| `.yaml` / `.yml` | `gopkg.in/yaml.v3` strict unmarshal |
| `.conf` / `.hocon` | `github.com/gurkankaymak/hocon`（`ParseResource` / `ParseString`） |

K8s Operator **仅接受 YAML CRD**；HOCON 用于集群外本地/侧车配置，不经由 CRD 下发。

#### 8.2.3 Canonical Schema（格式无关中间表示）

解析后的第一层结构（YAML/HOCON 同构）：

```go
// PipelineConfig — YAML 与 HOCON 解析后的统一中间表示（展开前）
type PipelineConfig struct {
    APIVersion    string
    Kind          string
    Metadata      map[string]string
    Engine        EngineConfig              // 对标 Envelope application.*（edgestream 字段名不变）
    Steps         map[string]StepConfig     // 主写法
    Stages        []StageConfig             // 平坦写法（与 Steps 二选一）
    Codecs        []CodecIR
    EdgeDefaults  EdgeConfig
    DLQ           *DLQConfig
    Observability ObservabilityConfig
    Edges         []EdgeIR                  // 已废弃：v1 兼容只读，v2 移除；请用 depends_on
}

// DependsOnList — 列表元素为 string（上游 id）或 map[string]EdgeAttrs（单键）
type DependsOnList []DependsOnEntry

type DependsOnEntry struct {
    Upstream string              // 简写：整项为字符串时
    Edge     *EdgeAttrs          // 对象形态：Upstream = map 的键
}

type EdgeAttrs struct {
    Condition string
    Route     string
    Buffer    *EdgeBufferConfig
    Delivery  *DeliverySpec
    Required  *bool
}

// StepConfig — 单 step：source / transform / sink 子块 + depends_on（边）
type StepConfig struct {
    StepType  string         // data | loop | decision | task；默认 data（v2.1+）
    DependsOn DependsOnList
    Source    *SourceBlock
    Transform *TransformBlock
    Sink      *SinkBlock
}

type SourceBlock struct {
    Type    string
    Decoder *CodecRef
    Config  map[string]any
}

type TransformBlock struct {
    Type      string
    Predicate string
    Workers   int
    Config    map[string]any   // 如 dsl、routes
}

type SinkBlock struct {
    Type        string
    Encoder     *CodecRef
    Batch       *BatchConfig
    Ordering    string
    MaxInFlight int
    Config      map[string]any // 插件自有字段（含写入语义）；支持 parameter.* 透传
}
```

**`Steps` 与 `Stages` 互斥** — 同一文件只能用一种；`steps` 块展开为 `[]StageIR` + `[]EdgeIR`（见 §8.6）。

#### 8.2.4 substitution 与 env 覆盖

| 能力 | YAML | HOCON |
|------|------|-------|
| 环境变量 `${KAFKA_BROKERS}` | 解析后字符串替换 | 原生 substitution（解析器支持时）或同一后处理 |
| 配置引用 `${steps.kafka-in.input.brokers}` | 支持（展开前 resolve） | 原生 `${}` 引用 |
| 默认值 `${?OPTIONAL}` | 未设置则空/省略 | HOCON optional substitution |
| `parameter.*` 透传 | `sink.config` 内或 `sink` 下 `parameter:` 块 | `sink` 下 `parameter { }` 或 `parameter.*` 点号键 |

#### 8.2.5 推荐写法：`steps` 块（YAML，edgestream 字段名）

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: order-processing

engine:
  max_workers: 16
  max_inflight: 10000

steps:
  kafka-in:
    source:
      type: kafka
      decoder: json
      config:
        brokers: ["${KAFKA_BROKERS}"]
        topics: [orders]
        group_id: order-consumer

  enrich:
    depends_on: [kafka-in]
    transform:
      type: map
      workers: 8
      config:
        dsl: |
          payload.total = payload.price * payload.quantity
    sink:
      type: kafka
      encoder: json
      batch: { size: 100, timeout: 1s }
      config:
        topic: orders-enriched
        parameter:
          compression.type: lz4
```

#### 8.2.6 等价 HOCON 示例（同一结构、同一字段名）

```hocon
engine {
  max_workers = 16
  max_inflight = 10000
}

steps {
  kafka-in {
    source {
      type = kafka
      decoder = json
      config {
        brokers = ["${KAFKA_BROKERS}"]
        topics = [orders]
        group_id = order-consumer
      }
    }
  }
  enrich {
    depends_on = [kafka-in]
    transform {
      type = map
      workers = 8
      config {
        dsl = """
          payload.total = payload.price * payload.quantity
        """
      }
    }
    sink {
      type = kafka
      encoder = json
      batch { size = 100, timeout = 1s }
      config {
        topic = orders-enriched
        parameter.compression.type = lz4
      }
    }
  }
}
```

#### 8.2.7 两种 step 书写形态

| 形态 | 适用 | 说明 |
|------|------|------|
| **`steps.{name}` 嵌套块**（上例） | 推荐；对齐 Envelope 布局 | 一个 step 可合体 `transform` + `sink` |
| **`pipeline[]` 平坦列表**（§8.8） | 简单管道、与早期草案兼容 | 每行一个 stage，`kind` + `type` + `depends_on` |

两种形态经加载器展开后生成相同的 `TopologyIR`。

### 8.3 场景一：简单线性（`steps` 写法）

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: order-processing

engine:
  max_workers: 16
  max_inflight: 10000

steps:
  kafka-source:
    source:
      type: kafka
      decoder: json
      config:
        brokers: [localhost:9092]
        topics: [orders]
        group_id: order-consumer

  enrich:
    depends_on: [kafka-source]
    transform:
      type: map
      workers: 8
      config:
        dsl: |
          payload = json(payload)
          payload.total = payload.price * payload.quantity

  filter-high:
    depends_on: [enrich]
    transform:
      type: filter
      config:
        dsl: "payload.total > 100"

  kafka-sink:
    depends_on: [filter-high]
    sink:
      type: kafka
      encoder: json
      ordering: ordered
      batch: { size: 100, timeout: 1s }
      config:
        brokers: [localhost:9092]
        topic: orders-enriched
```

等价 HOCON 片段：

```hocon
steps {
  kafka-source {
    source {
      type = kafka
      decoder = json
      config { brokers = ["localhost:9092"], topics = [orders], group_id = order-consumer }
    }
  }
  enrich {
    depends_on = [kafka-source]
    transform {
      type = map
      workers = 8
      config { dsl = "payload = json(payload)\n..." }
    }
  }
  filter-high {
    depends_on = [enrich]
    transform { type = filter, config { dsl = "payload.total > 100" } }
  }
  kafka-sink {
    depends_on = [filter-high]
    sink {
      type = kafka
      encoder = json
      ordering = ordered
      batch { size = 100, timeout = 1s }
      config { brokers = ["localhost:9092"], topic = orders-enriched }
    }
  }
}
```

> 分支 DAG、per-edge buffer/delivery 见 §8.4。平坦 `pipeline[]` 兼容写法见 §8.8。

### 8.4 场景二：分支 DAG（扩展 `depends_on`）

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: order-processing

codecs:
  - name: avro-orders
    type: avro
    config: { schema: "...", registry: confluent, registry_url: "..." }

edgeDefaults:
  buffer: { type: memory, size: 128, strategy: block }

dlq:
  sink: dlq-sink
  include_current_payload: false

engine:
  max_workers: 16

steps:
  kafka-source:
    source:
      type: kafka
      decoder: { ref: avro-orders }
      config: { brokers: [...], topics: [orders], group_id: order-consumer }

  enrich:
    depends_on: [kafka-source]
    transform:
      type: map
      workers: 8
      config:
        dsl: |
          payload.total = payload.price * payload.quantity
          metadata.region = payload.region

  splitter:
    depends_on: [enrich]
    transform:
      type: route
      config:
        routes:
          us: "metadata.region == 'us'"
          eu: "metadata.region == 'eu'"
          _default: "true"

  us-sink:
    depends_on:
      splitter:
        route: us
    sink:
      type: http
      encoder: json
      batch: { size: 100, timeout: 1s }
      config: { url: https://us-api.example.com/orders }

  eu-sink:
    depends_on:
      splitter: { route: eu }
    sink:
      type: http
      config: { url: https://eu-api.example.com/orders }

  dlq-sink:
    depends_on:
      splitter:
        route: _default
        delivery:
          retry: { max: 0 }
    sink:
      type: kafka
      config: { topic: orders-dlq }
```

等价 HOCON 片段：

```hocon
steps {
  us-sink {
    depends_on {
      splitter { route = us }
    }
    sink { type = http config { url = "..." } }
  }
}
```

> 对比旧写法：原 `edges: [{from: splitter, to: us-sink, route: us}]` 改为在 **us-sink** 上写 `depends_on.splitter.route`。

### 8.5 统一 IR

```go
type TopologyIR struct {
    Name          string
    Stages        []StageIR
    Edges         []EdgeIR
    Codecs        []CodecIR
    EdgeDefaults  EdgeConfig
    DLQ           *DLQConfig
    Engine        EngineConfig
    Observability ObservabilityConfig
}

type StageIR struct {
    ID          string
    Kind        StageKind          // source / transform / sink；配置键 kind（steps 子块由块名隐含）
    Type        string             // 组件实现名；配置键 type
    Workers     int
    Decoder     *CodecRef
    Encoder     *CodecRef
    Predicate   string             // Transform 条件应用（Kafka Connect 风格）；仅 kind=transform
    Batch       *BatchConfig
    Ordering    string
    MaxInFlight int
    Config      map[string]any
    // 边、重试、DLQ 不在 StageIR；由 depends_on → EdgeIR.delivery + pipeline dlq
}

type EdgeIR struct {
    From      string
    To        string
    Condition string             // CEL 表达式，空 = 全部
    Route     string             // route Transform 命名路由（展开为 condition）
    Buffer    *EdgeBufferConfig  // nil = 使用 edgeDefaults
    Delivery  *DeliverySpec      // retry / timeout / dlq
    Required  *bool              // 默认 true
}

type CodecIR struct {
    Name   string
    Type   string
    Config map[string]any
}

type CodecRef struct {
    Ref    string             // 引用 codecs 顶层声明的 name
    Type   string             // inline 声明（与 Ref 互斥）
    Config map[string]any     // Stage 级配置，shallow merge 覆盖 ref 的 config
}

type EngineConfig struct {
    MaxWorkersPerPipeline    int
    MaxInflightPerPipeline   int
    DrainTimeout             time.Duration
}

type DLQConfig struct {
    Sink                  string    // 引用 sink stage ID
    IncludeCurrentPayload bool
}
```

### 8.6 展开规则

配置加载时将 `depends_on` 展开为 `[]EdgeIR`（**YAML 与 HOCON 相同**）：

| 配置 | 展开 |
|------|------|
| `depends_on: [a, b]`（序列） | `EdgeIR{from:a,to:self}`、`EdgeIR{from:b,to:self}` + `edgeDefaults` |
| `depends_on: { a: {}, b: { route: us } }`（映射） | 同上，`b` 边合并对象内字段 |
| 序列元素 `- upstream: { ... }` | 单键对象，等价于映射形态的一项 |
| `depends_on.{upstream}.route` | 生成 `condition: "metadata['er-route'] == '{route}'"` |
| `depends_on.{upstream}.condition` | 直接写入 `EdgeIR.condition` |
| `steps.{name}.source` only | `StageIR{id=name, kind=source, ...}` |
| `steps.{name}.transform`（无 sink） | `StageIR{id=name, kind=transform, ...}` |
| `steps.{name}.transform` + `sink` | transform stage + `{name}-sink` sink stage + 内边 `{name}→{name}-sink` |
| 平坦 `pipeline[]` | 映射 `StageIR`；`depends_on` 同上展开 |
| 遗留 `edges[]`（deprecated） | 合并入 `EdgeIR`；同 `(from,to)` 时 `edges` 覆盖 `depends_on` |
| `sink.config.parameter.*` | 剥前缀写入 sink `config` |

`steps` 合体 step 示例：

```
steps.enrich.depends_on = [kafka-in]
  → EdgeIR kafka-in → enrich

steps.enrich.transform  → StageIR id=enrich, kind=transform
steps.enrich.sink       → StageIR id=enrich-sink, kind=sink
  → EdgeIR enrich → enrich-sink（合体 step 内边，自动）
```

**校验：**

- 每个非 source stage 至少一条入边（来自 `depends_on` 或遗留 `edges`）
- `from`/`to` 引用已定义的 stage id
- 无环；至少一条 source→sink 通路
- `route` 与 `condition` 不可同现于同一边

### 8.7 配置加载流程

```
.yaml/.yml/.conf/.hocon 文件 / K8s CRD(YAML) / 目录
        │
        ▼
  按扩展名选择解析器 → PipelineConfig（Canonical Schema）
        │
        ▼
  substitution 解析（${ENV}、${path}、${?optional}）
        │
        ▼
  steps / pipeline 规范化
        │
        ▼
  depends_on 展开 → []EdgeIR（应用 edgeDefaults；route→condition）
        │
        ▼
  合并遗留 edges[]（若有，输出 deprecation warning）
        │
        ▼
  Validate TopologyIR：
    - stage ID 唯一
    - edge 引用有效
    - 无孤立节点
    - 无环
    - 至少一个 source → sink 通路
    - codec ref 存在 + ValidateConfig
    - CEL 表达式编译 + TypeChecker
    - payload.* 引用但未配 decoder → 报错
    - ordering: ordered + max_in_flight > 1 → 报错
        │
        ▼
  TopologyIR → 传给 Topology Layer 构建执行图
```

### 8.8 平坦 `pipeline[]` 写法（兼容）

与 `steps` **二选一**；展开后与 §8.3 线性示例生成相同 `TopologyIR`。适合极简管道或迁移早期草案。

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: order-processing

pipeline:
  - id: kafka-source
    kind: source
    type: kafka
    decoder: json
    config:
      brokers: [localhost:9092]
      topics: [orders]
      group_id: order-consumer

  - id: enrich
    kind: transform
    type: map
    depends_on: [kafka-source]
    workers: 8
    config:
      dsl: |
        payload = json(payload)
        payload.total = payload.price * payload.quantity

  - id: filter-high
    kind: transform
    type: filter
    depends_on: [enrich]
    config:
      dsl: "payload.total > 100"

  - id: kafka-sink
    kind: sink
    type: kafka
    depends_on: [filter-high]
    encoder: json
    ordering: ordered
    batch: { size: 100, timeout: 1s }
    config:
      brokers: [localhost:9092]
      topic: orders-enriched
```

**与 `steps` 的字段对应：**

| `pipeline[]` | `steps` |
|------------|---------|
| `id` | step 名（map key） |
| `kind: source` | `source:` 子块 |
| `kind: transform` | `transform:` 子块 |
| `kind: sink` | `sink:` 子块 |
| `depends_on` | 同名字段，语法见 §8.1.3 |

---

## 9. 组件生态规划

### 9.1 Source

| 优先级 | 组件 | 说明 | at-least-once |
|--------|------|------|:---:|
| **P0** | `kafka` | Consumer group、多 partition、SASL | ✅ |
| **P0** | `http_server` | 路径/方法路由、TLS、限流 | ❌ |
| **P0** | `cron` | Cron 表达式、时区支持 | ❌ |
| **P0** | `grpc_server` | proto 反射或通用事件接收 | ❌ |
| **P1** | `redis` | Streams / List / PubSub | ✅（Streams） |
| **P1** | `nats` | JetStream 持久订阅 | ✅ |
| **P1** | `file` | tail 模式、通配符匹配 | ⚠️ |
| **P2** | `aws_s3` | S3 bucket poll + SQS 通知 | ⚠️ |
| **P2** | `aws_sqs` | SQS 长轮询 | ✅ |
| **P2** | `gcp_pubsub` | Pub/Sub push/pull | ✅ |
| **P2** | `mysql_cdc` | binlog CDC | ✅ |

pull 源（S3/SQS/CDC）实现 `PollingSource` 接口，引擎 wrapper 管定时 poll + 空轮询退避 + pending 队列 + 背压跳过。Source 配置 `poll_retry: { max_attempts, initial_interval, max_interval }`。

### 9.2 Transform

| 优先级 | 组件 | 说明 |
|--------|------|------|
| **P0** | `map` | eql DSL 驱动，字段映射/丰富/计算 |
| **P0** | `filter` | eql DSL 驱动，CEL 谓词过滤 |
| **P0** | `route` | 条件分支（map 语法糖），`action: move`（v1） |
| **P0** | `wasm` | 运行 WASM 模块（wazero） |
| **P1** | `validate` | JSON Schema / Proto 校验 |
| **P1** | `enrich` | HTTP / gRPC 外部查表丰富 |
| **P1** | `deduplicate` | 按 key 去重，可配窗口 |
| **P1** | `split` | 数组展开为多条消息 |
| **P1** | `throttle` | 速率限制 |
| **P1** | `promote` | 把 payload 字段提升到 metadata（显式投影） |
| **P2** | `aggregate` | 时间窗口聚合（count/sum/avg） |
| **P2** | `redact` | PII 脱敏 |
| **P2** | `compress` / `decompress` | gzip/snappy/lz4 |
| **P2** | `log` | 调试输出 |
| **P1** | `llm` | 单轮 LLM 调用（chat / JSON mode）；OpenAI 兼容 + Ollama；eql 模板 prompt — 详见 [docs/ai-agent.md](docs/ai-agent.md) |
| **P1** | `embed` | 文本 embedding；RAG 摄取管道 |
| **P2** | `agent` | 多轮 Agent + tool calling + session；v2.2 |

每个 Transform 可配 `predicate`（CEL 表达式），引擎在调 `Process` 前求值，false 则透传该消息（Kafka Connect 风格条件应用）。

### 9.3 Sink

写入语义（append、upsert、幂等等）**不由引擎抽象**；各 Sink 插件在自有 `config` schema 中定义并实现。插件注册时提供 `ValidateConfig` + 文档说明支持的写入行为。

| 优先级 | 组件 | 说明 |
|--------|------|------|
| **P0** | `kafka` | 分区策略、header 透传、压缩；写入语义见插件 config（如幂等生产者） |
| **P0** | `http` | 重试、TLS、mTLS、连接池、`method`/`path` 等 |
| **P0** | `grpc` | 负载均衡、TLS |
| **P0** | `drop` | /dev/null，测试用 |
| **P0** | `log` | 结构化日志输出 |
| **P1** | `nats` | JetStream publish |
| **P1** | `redis` | Streams / List / PubSub |
| **P1** | `file` | 文件写入 + 轮转 |
| **P1** | `elasticsearch` | 批量索引；`index`/`op_type` 等由插件 config 定义 |
| **P2** | `aws_s3` | 对象存储写入 |
| **P2** | `bigquery` | 流式插入 |
| **P2** | `websocket_client` | 实时推送 |
| **P2** | `pgvector` / `qdrant` | RAG 向量写入（v2.2，与 embed transform 配合） |
| **P2** | `langfuse` | LLM trace 导出（可选，与 OTLP 互补） |

### 9.4 Codec

| 优先级 | 组件 | Decode 产出 | 说明 |
|--------|------|------------|------|
| **P0** | `json` | `map[string]any` | JSON 解码/编码 |
| **P0** | `raw` | `[]byte` 透传 | 无解析 |
| **P1** | `avro` | `map[string]any` | Avro + Schema Registry |
| **P1** | `protobuf` | `map[string]any`（v1 转换，v2 原生 struct） | Protobuf 解码 |
| **P1** | `csv` | `map[string]any`（带 header）或 `[]string` | CSV 解码 |

### 9.5 WASM 插件

```yaml
steps:
  custom-decrypt:
    depends_on: [kafka-source]
    transform:
      type: wasm
      config:
        module: /opt/plugins/decrypt.wasm
        function: process          # 必须导出
        timeout: 100ms
        memory_limit: 64MB
        fuel: 0                    # 0 = unlimited（靠 timeout 兜底）
        plugin_config:             # 可选，传入 WASM init()
          key: ${DECRYPT_KEY}
          algorithm: aes-256-gcm
```

> 平坦 `pipeline[]` 写法见 §8.8。

**接口契约：**
- payload `[]byte` 直传 WASM 线性内存；
- metadata JSON 序列化整体传入传出；
- 返回 JSON 数组 `[{payload: "base64", metadata: {...}}, ...]`；
- 可选导出 `init(config_ptr, config_len)` 接收 plugin_config（无 init 导出则忽略 + warning）；
- 无 WASI 文件/网络/环境变量；
- 仅提供 `now()` / `uuid()` / `log()` 三个安全 host function；
- timeout 默认 100ms，memory_limit 默认 64MB，并发 = workers。

### 9.6 Go 插件加载

**编译时注册**（v1 主路径）：

```go
// plugins/source/kafka/kafka.go
package kafka
import "code.dandanvoice.com/edgestream/plugin"
func init() { plugin.RegisterSource("kafka", &KafkaSourceFactory{}) }

// 用户自定义：fork + import + 重新编译
// cmd/my-edgestream/main.go
import (
    _ "code.dandanvoice.com/edgestream/plugins/source/all"
    _ "mycompany.com/my-source"
)
```

**gRPC 进程外插件**（v2 可选扩展，对标 HashiCorp go-plugin）。

**不做 Go `plugin` .so 动态加载**。

### 9.7 插件目录约定

```
plugins/
├── source/
│   ├── kafka/
│   ├── http_server/
│   └── ...
├── transform/
│   ├── map/
│   ├── filter/
│   ├── route/
│   └── wasm/
├── sink/
│   ├── kafka/
│   ├── http/
│   └── ...
├── codec/
│   ├── json/
│   ├── avro/
│   └── ...
```

### 9.8 扩展优先级

| 阶段 | 内容 |
|------|------|
| **v2.0** | P0 全量 + WASM + Codec（json/raw） |
| **v2.1** | P1 全量 + Codec（avro/protobuf/csv）+ 插件 SDK 文档 + **AI Phase 1–2**（Agent MCP/JSON CLI → 管道内 `llm`/`embed`） |
| **v2.2** | P2 按需 + gRPC 插件 + per-partition ordering + 社区贡献指南 + **AI Phase 3**（管道内 `agent` transform、向量 Sink） |
| **v2.3+** | **AI Phase 4**（MCP 双向、streaming LLM、`task` step） |

> AI/Agent 完整设计见 [docs/ai-agent.md](docs/ai-agent.md)；Phase 0 Skill 见 [skills/edgestream/](skills/edgestream/)；Phase 1 计划见 [docs/superpowers/plans/2026-07-01-agent-skill.md](docs/superpowers/plans/2026-07-01-agent-skill.md)。

---

## 10. 可观测性设计

### 10.1 指标前缀与标签

前缀：`edgestream_`。

**固定低基数标签**（总是启用）：`pipeline`、`stage_id`、`stage_kind`、`from`、`to`、`status`、`error_type`、`type`、`codec`、`route_name`。

**可选中基数标签**（`extra_labels` 配置启用）：`kafka_topic`、`kafka_partition`、`http_status`。

**禁止高基数标签**（只在 tracing/logging 出现）：`message_id`、`trace_id`。

**安全阀**：`max_label_cardinality: 1000`——单标签超限归 `__other__`。

### 10.2 完整指标清单

| 层级 | 指标名 | 类型 | 标签 |
|------|--------|------|------|
| **Pipeline** | `edgestream_events_total` | Counter | pipeline, status |
| | `edgestream_event_latency_seconds` | Histogram | pipeline |
| | `edgestream_event_size_bytes` | Histogram | pipeline |
| | `edgestream_inflight_events` | Gauge | pipeline |
| **Stage** | `edgestream_stage_duration_seconds` | Histogram | pipeline, stage_id, stage_kind |
| | `edgestream_stage_processed_total` | Counter | pipeline, stage_id, status |
| | `edgestream_stage_errors_total` | Counter | pipeline, stage_id, error_type |
| | `edgestream_stage_retries_total` | Counter | pipeline, stage_id |
| **Source** | `edgestream_source_read_total` | Counter | pipeline, stage_id |
| | `edgestream_source_read_errors_total` | Counter | pipeline, stage_id |
| | `edgestream_source_lag` | Gauge | pipeline, stage_id |
| | `edgestream_source_poll_interval_seconds` | Gauge | pipeline, stage_id |
| **Transform** | `edgestream_transform_batch_size` | Histogram | pipeline, stage_id |
| | `edgestream_transform_fanout_total` | Counter | pipeline, stage_id |
| **Sink** | `edgestream_sink_write_total` | Counter | pipeline, stage_id, status |
| | `edgestream_sink_write_events` | Counter | pipeline, stage_id |
| | `edgestream_sink_batch_size` | Histogram | pipeline, stage_id |
| | `edgestream_sink_in_flight` | Gauge | pipeline, stage_id |
| | `edgestream_sink_flush_total` | Counter | pipeline, stage_id |
| **Edge** | `edgestream_edge_buffer_size` | Gauge | pipeline, from, to |
| | `edgestream_edge_buffer_type` | Gauge | pipeline, from, to, type |
| | `edgestream_edge_dropped_total` | Counter | pipeline, from, to, reason |
| | `edgestream_edge_disk_wal_size_bytes` | Gauge | pipeline, from, to |
| | `edgestream_edge_disk_wal_segments` | Gauge | pipeline, from, to |
| **Codec** | `edgestream_codec_decode_total` | Counter | pipeline, stage_id, codec, status |
| | `edgestream_codec_decode_duration_seconds` | Histogram | pipeline, stage_id, codec |
| | `edgestream_codec_encode_total` | Counter | pipeline, stage_id, codec, status |
| | `edgestream_codec_encode_duration_seconds` | Histogram | pipeline, stage_id, codec |
| **DLQ** | `edgestream_dlq_messages` | Gauge | pipeline, dlq_stage_id |
| | `edgestream_dlq_enqueued_total` | Counter | pipeline, dlq_stage_id, error_type |
| | `edgestream_dlq_replayed_total` | Counter | pipeline, dlq_stage_id |
| **Route** | `edgestream_route_matched_total` | Counter | pipeline, stage_id, route_name |
| | `edgestream_route_default_total` | Counter | pipeline, stage_id |
| **DSL** | `edgestream_dsl_eval_total` | Counter | pipeline, stage_id, status |
| | `edgestream_dsl_eval_duration_seconds` | Histogram | pipeline, stage_id |
| | `edgestream_dsl_type_mismatch_total` | Counter | pipeline, stage_id |
| **Engine** | `edgestream_engine_goroutines` | Gauge | pipeline |
| | `edgestream_engine_pipelines` | Gauge | (无 label) |
| | `edgestream_pipeline_uptime_seconds` | Gauge | pipeline |
| | `edgestream_pipeline_status` | Gauge | pipeline, status |
| | `edgestream_hot_reload_total` | Counter | pipeline, status |

### 10.3 暴露方式

```yaml
observability:
  metrics:
    enabled: true
    port: 9090
    path: /metrics
    format: prometheus       # prometheus | otlp | json
    extra_labels: [kafka_topic, route_name]
    max_label_cardinality: 1000
    otlp:
      endpoint: otel-collector:4317
      interval: 15s
```

### 10.4 追踪（Tracing）

每层创建 span — 完整捕获事件经过的每个 stage 和 edge，包括 condition 匹配结果：

```
Pipeline Span (edgestream.pipeline.order-processing)
├── Source Span (edgestream.source.kafka-source)
│   ├── Transform Span (edgestream.transform.enrich)
│   │   ├── Edge Span (edgestream.edge.enrich→us-sink)
│   │   │   └── Sink Span (edgestream.sink.us-sink)
│   │   └── Edge Span (edgestream.edge.enrich→eu-sink)
```

trace_id / message_id / topic / partition 进 span attributes（高基数信息只在 tracing 出现）。

### 10.5 日志（Logging）

```json
{
  "level": "info",
  "msg": "message processed",
  "pipeline": "order-processing",
  "stage_id": "enrich",
  "stage_kind": "transform",
  "message_id": "msg_01jv4x...",
  "trace_id": "abc123",
  "duration_ms": 1.2,
  "metadata": { "region": "us" }
}
```

| 功能 | 支持 |
|------|:---:|
| 日志格式 | json / text / loki |
| 日志级别动态调整 | ✅ `PUT /debug/loglevel` |
| stage 独立级别 | ✅ stage 级覆盖 |
| 敏感字段脱敏 | ✅ `redact_fields` 配置 |

### 10.6 健康检查

```yaml
observability:
  health:
    port: 8080
    endpoints:
      liveness: /live
      readiness: /ready
```

- **`/live`**：查进程活着（HTTP server + engine supervisor）→ 200/503。不查 Stage。
- **`/ready`**：调所有 Pipeline 所有 Stage 的 `HealthCheck`，全 healthy→200，任一 unhealthy→503 + body 列出 `pipeline/stage_id/message`。支持 `?pipeline={name}`。
- **正在重试/drain 中算 unhealthy**。
- **Transform 默认 healthy**（无外部依赖）。
- **admin/metrics 端点独立 mux**，不受 readiness 影响。

### 10.7 通知（Notifications）

```yaml
observability:
  notifications:
    enabled: true
    rules:
      - name: critical-alerts
        events: [backpressure_paused, sink_fatal, dlq_growing]
        cooldown: 5m
        channels: [pagerduty, slack-critical]
    channels:
      - name: pagerduty
        type: pagerduty
        config: { routing_key: ${PAGERDUTY_KEY} }
    template_engine: go_template
```

**v2 新增事件类型：**

| 事件 | 触发条件 |
|------|---------|
| `backpressure_paused` | 背压状态进入 PAUSED |
| `sink_fatal` | Sink 连续失败不可恢复 |
| `dlq_growing` | DLQ 消息数超过阈值 |
| `edge_dropped` | Edge buffer 满导致丢弃 |
| `source_lag_high` | Source 消费延迟超过阈值 |
| `pipeline_halted` | 管道因配置错误或致命异常停止 |

---

## 11. 部署架构

### 11.1 双形态总览

```
┌─────────────────────────────────────────────────────────┐
│                     Core Engine                          │
│         (Source → Transform → Sink DAG)                 │
│         同样的代码，同一种行为                             │
└──────────────┬────────────────────┬─────────────────────┘
               │                    │
      ┌─────────▼──────┐    ┌───────▼──────────────┐
      │  Standalone     │    │  K8s Operator         │
      │  • 单二进制      │    │  • 同二进制 + CRD     │
      │  • YAML 配置     │    │  • Pipeline CR 驱动   │
      │  • 多 Pipeline   │    │  • 自动重启/扩缩      │
      │  • 信号管理      │    │  • 调 HTTP API 重载   │
      └────────────────┴────┴───────────────────────┘
```

### 11.2 单二进制模式

```bash
# 基础启动（单 Pipeline）
edgestream run --config pipeline.yaml

# 目录模式（多 Pipeline，每个文件一个 pipeline）
edgestream run --config-dir /etc/edgestream/pipelines/

# YAML 或 HOCON（按扩展名自动识别）
edgestream run --config pipeline.yaml
edgestream run --config pipeline.conf

# 配置文件内 ${KAFKA_BROKERS} 自动替换（YAML/HOCON 均支持）
```

#### 文件结构

```
/opt/edgestream/
├── bin/edgestream             # 主二进制（含所有内置 Go 插件）
├── etc/
│   ├── config.yaml        # 引擎全局配置
│   └── pipelines/         # 管道定义（.yaml / .yml / .conf / .hocon 可混放）
├── plugins/
│   └── *.wasm             # WASM 插件（动态加载）
└── data/
    ├── dlq/               # DLQ 持久化
    └── buffers/           # edge disk buffer WAL
```

### 11.3 K8s Operator

#### CRD

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: order-processing
spec:
  codecs: [...]
  edgeDefaults: ...
  dlq: ...
  engine:
    max_workers: 16
    max_inflight: 10000
  steps:                    # 推荐；边在各 step 的 depends_on 内
    kafka-source: { ... }
    enrich: { depends_on: [kafka-source], ... }
status:
  phase: Running
  conditions:
    - type: Ready
      status: "True"
  stageStatus:
    kafka-source: { phase: Running }
    us-sink: { phase: Degraded, message: "HTTP 503" }
```

#### Operator 架构

```
Operator Pod
┌──────────┐     ┌──────────────────┐
│Controller│────▶│  Reconciler      │
│(watch    │     │  → ConfigMap     │
│ CRDs)    │     │  → Deployment    │
└──────────┘     │  → Service       │
                 │  → Status Update │
                 │  → POST /admin/  │
                 │     reload/{name}│
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │  Pipeline Pod     │
                 │  (ConfigMap mount │
                 │   → 引擎启动)     │
                 └──────────────────┘
```

Operator 通过调 Pod 的 `POST /admin/reload/{name}` HTTP API 触发热加载（无 ConfigMap 传播延迟）。

v1 默认**多 Pipeline 聚合模式**（单 Pod 多 Pipeline，资源高效）。独立 Deployment 隔离模式需显式开启。

#### 自动扩缩

```yaml
spec:
  scaling:
    minReplicas: 1
    maxReplicas: 10
    metrics:
      - type: cpu
        targetUtilization: 70
      - type: event_lag
        targetValue: 1000
```

### 11.4 CLI 工具

```
edgestream run        # 运行引擎
edgestream validate   # 仅校验配置（CI 用）
edgestream test       # 用 fixture 数据测试管道（CI 用）
edgestream doc        # 生成管道可视化（DOT 格式 → Graphviz）
```

v2 增加 `edgestream eql`（CEL/eql REPL）、`edgestream lint`（配置 lint）。

---

## 12. 开发路线图

> **当前版本：** v2.0-alpha（核心引擎可运行）→ 进入 **v2.0-beta** 冲刺  
> **详细执行计划：** `docs/superpowers/plans/2025-06-25-sprint1-observability.md`

### 阶段 1：核心引擎（v2.0-alpha）

- [x] 项目骨架（module、目录结构、Makefile）
- [x] Message 模型 + Stage（`Kind`/`ComponentType`）/ Edge / Codec 接口定义
- [x] Topology IR + YAML/HOCON 解析 + steps 规范化 + depends_on（含对象形态）展开 + 校验
- [x] Engine 核心：fanOut/fanIn、Edge Buffer（memory）、refCount Ack、COW、背压传播
- [x] eql DSL：CEL 集成（cel-go）+ 赋值扩展 + 函数注册 + 编译期检查（函数库待扩展）
- [x] Codec 体系：json/raw + 顶层 codecs 配置 + 惰性 ParsedData
- [x] 3 个 Source（kafka/http_server/cron）+ 3 个 Sink（kafka/http/drop）+ 3 个 Transform（map/filter/route）
- [ ] WASM Transform（占位，推迟至 v2.0）
- [x] 多 Pipeline 运行时（进程内隔离；`engine.max_workers`/`max_inflight` 已接线）
- [x] Metrics（`edgestream_` 前缀）+ Tracing（OTLP 骨架）+ Health endpoints
- [x] CLI：run + validate

### 阶段 1.5：v2.0-beta（当前冲刺，约 3–4 周）

按 Sprint 顺序推进，每 Sprint 结束应产出可独立验证的增量。

#### Sprint 1：可观测性基础（闭合 alpha 最后一项）— **已完成**

- [x] Prometheus `edgestream_*` 指标端点（`:9090/metrics`）
  - Pipeline：`edgestream_events_total`、`edgestream_event_latency_seconds`、`edgestream_inflight_events`
  - Stage：`edgestream_stage_duration_seconds`、`edgestream_stage_errors_total`
  - Edge：`edgestream_edge_buffer_size`、`edgestream_edge_dropped_total`
  - DLQ：`edgestream_dlq_enqueued_total`
- [x] Health 端点：`/live`（存活）+ `/ready`（聚合 Stage `HealthCheck`）
- [x] 结构化 JSON 日志（`slog`）+ 动态 level 调整（`PUT /debug/loglevel`）
- [x] OTLP Tracing 接口骨架（pipeline → stage 粒度，noop 实现）

#### Sprint 2：引擎级约束 + error_mode — **已完成**

- [x] `engine.max_workers` 全局 worker 池限制
- [x] `engine.max_inflight` 全 pipeline 在途消息背压
- [x] `error_mode` 传播链（`propagate` / `ignore` / `silent`）

#### Sprint 3：Edge Disk Buffer + 优雅停机加固 — **已完成**

- [x] WAL segment 格式 + 崩溃恢复
- [x] memory → disk overflow
- [x] 周期性 fsync；停机顺序复核（Source → drain → Flush → Stop）

#### Sprint 4：热加载 + 代码质量 + 测试补强

- [ ] Admin API：`POST /admin/reload/{pipeline}`、`SIGHUP`、409 并发保护
- [ ] `msgAdapter` 去重；engine/topology 集成测试
- [ ] CLI：`edgestream test`（fixture 驱动）

### 阶段 2：生产就绪（v2.0）

- [x] Edge disk buffer + overflow（Sprint 3）
- [ ] 其余 P0 组件（grpc_server source、grpc/log sink）
- [x] Sink max_in_flight + ordering（基础实现已完成）
- [x] retry + DLQ（边 `delivery` + pipeline `dlq` fallback 链；确定性/瞬时错误分类待细化）
- [ ] 配置热加载（Sprint 4）
- [ ] Notifications
- [ ] WASM Transform（wazero）
- [ ] PollingSource wrapper
- [ ] 性能基准测试 + 压力测试

### 阶段 3：生态扩展（v2.1）

- [ ] K8s Operator（Pipeline CRD + Controller + Reconciler + HTTP API 重载）
- [ ] P1 组件全量（nats/redis/file source/sink；validate/enrich/dedup/split/throttle/promote transform）
- [ ] Codec P1（avro/protobuf/csv）
- [ ] gRPC 进程外插件
- [ ] 插件 SDK 文档 + 示例
- [ ] **AI Phase 0** — Agent 调用 edgestream：skills.sh Skill（`skills/edgestream/`）、文档对齐（详见 [docs/ai-agent.md](docs/ai-agent.md)）
- [ ] **AI Phase 1** — `plugins list`、`validate --format json`、MCP Server

### 阶段 3.5：AI/Agent（v2.0-beta → v2.2）

- [ ] **AI Phase 0** — skills.sh Skill + Agent 工作流文档（[skills/edgestream/](skills/edgestream/)）
- [ ] **AI Phase 1** — 机器可读 CLI（JSON validate/plugins）+ MCP Server
- [ ] **AI Phase 2** — 管道内 `llm` / `embed` transform（v2.1）
- [ ] **AI Phase 3** — 管道内 `agent` transform、向量 Sink（v2.2）

### 阶段 4：高级特性（v2.2+）

- [ ] P2 组件按需（aws_s3/sqs、gcp_pubsub、mysql_cdc、elasticsearch、bigquery）
- [ ] per-partition ordering（buffer.key 分区缓冲）
- [ ] VRL 风格 fallibility 类型系统（`!`/`??`/`, err`）
- [ ] route `action: copy`（fan-out 多 route）
- [ ] Decision + Task 步骤（Envelope 启发）
- [ ] 跨 Pipeline 事件路由（经由中间总线）
- [ ] 社区贡献指南 + 管道可视化 UI
- [ ] Schema Registry 集成
- [ ] **AI Phase 4** — MCP 双向、streaming LLM、`task` step（详见 [docs/ai-agent.md](docs/ai-agent.md)）

---

## 13. v2.0 定稿检查清单

设计文档从 **draft** 升级为 **v2.0-final** 前，逐项确认。状态：`[x]` 已完成 / `[ ]` 待办 / `[-]` 明确推迟。

### 13.1 架构与接口（冻结项）

| # | 检查项 | 状态 | 备注 |
|---|--------|------|------|
| A1 | `Stage.Kind()` + `ComponentType()` 与配置 `kind`/`type` 一一对应 | [x] | §4.2、§8.1.2 |
| A2 | `Transform.Process(batch []*Message)` 批次语义 | [x] | §4.4 |
| A3 | `Sink.Write(msgs)` + 引擎 BatchCollector 攒批 | [x] | §4.5、§6.1 |
| A4 | refCount = fanOut **实际匹配**出边数（非静态可达） | [x] | §6.3 |
| A5 | 无引擎级 Planner / 统一 `write_mode`；写入语义在 Sink `config` | [x] | §2.2、§4.5 |
| A6 | 重试/DLQ 仅在 `EdgeIR.delivery` + pipeline `dlq`；无 `StageIR.Retry/DLQ` | [x] | §6.5、§8.5 |
| A7 | 条件应用仅 `transform.predicate`；无 `Edge.Predicate` | [x] | §4.6、§8.5 |
| A8 | step（配置）→ stage（运行时）→ edge（`depends_on` 展开）术语一致 | [x] | §3.2、§8.1 |

### 13.2 配置模型

| # | 检查项 | 状态 | 备注 |
|---|--------|------|------|
| C1 | `depends_on` 为唯一边语法；`edges:` 标记 deprecated | [x] | §8.1.3 |
| C2 | `depends_on` 序列/映射二形态 normative 定义 | [x] | §8.1.3、§8.6 |
| C3 | 主示例全部以 `steps` 为推荐写法 | [x] | §5.2、§8.2.5、§8.3、§8.4、§9.5 |
| C4 | `pipeline[]` 仅作兼容附录（§8.8），与 `steps` 互斥 | [x] | §8.2.7、§8.8 |
| C5 | YAML + HOCON 双格式、Canonical Schema 字段对齐 | [x] | §8.2 |
| C6 | 合体 step（transform+sink）展开规则与内边命名 `{name}-sink` | [x] | §8.6 |
| C7 | substitution（`${ENV}`、`${path}`、`${?opt}`）行为写清 | [x] | §8.2.4 |
| C8 | CRD `spec.steps` + `depends_on` 与 standalone 配置一致 | [x] | §11.3 |
| C9 | HOCON 解析库锁定为 `gurkankaymak/hocon`；spike 已通过 | [x] | §8.2.2、`spike/hocon/` |

### 13.3 运行时语义（待实现前复核）

| # | 检查项 | 状态 | 备注 |
|---|--------|------|------|
| R1 | eql 四类错误语义（filter/condition/mapping/编译）完整且可测 | [x] | §7.8 |
| R2 | `error_mode`（propagate/ignore/silent）作用域：engine 默认 → transform 覆盖 | [x] | §7.8 |
| R3 | Transform split 子消息 Ack 聚合链 | [x] | §6.3 |
| R4 | 优雅停机顺序（Source 先停 → drain → Flush → Stop） | [x] | §6.6 |
| R5 | Edge disk buffer WAL 格式与崩溃恢复语义 | [x] | §6.8 |
| R6 | pull 源 `PollingSource` wrapper 行为（退避/背压跳过） | [x] | §4.3、§9.1 |
| R7 | 热加载 gap（30–60s）与 409 并发重载约束 | [x] | §6.6 |

### 13.4 文档与附件一致性

| # | 检查项 | 状态 | 备注 |
|---|--------|------|------|
| D1 | `design-review.md` 过时建议已标注 | [x] | 文首注意 |
| D2 | `competitor-research.md` 中 `edges:` / Planner 表述与主文档对齐 | [x] | §9.2–9.4 及总结项已标注决策 |
| D3 | 全文无 `Stage.Type()` / `Edge.Predicate` / Stage 级 DLQ 残留 | [x] | 定稿前修订已清理 |
| D4 | 指标前缀统一 `edgestream_`（非 `er2_`） | [x] | §10.1 |
| D5 | 路线图阶段与 §13 检查项可追溯 | [x] | §12 |

### 13.5 明确推迟（不阻塞 v2.0-final）

| 项 | 目标版本 | 说明 |
|----|----------|------|
| Decision / Task / Loop step（`StepType`） | v2.1+ | §8.2.3 `StepType` 字段预留 |
| route `action: copy`（多路 fan-out） | v2.2 | §7.10 |
| VRL 风格 fallibility（`!` / `??`） | v2.2 | §7.9 |
| gRPC 进程外插件 | v2.1 | §9.6 |
| per-partition ordering（`buffer.key`） | v2.2 | §6.4 |
| fsnotify 配置监听 | v2 评估 | §6.6 |
| CESQL TCK 对标 | 可选 | 当前为 CEL + eql 赋值扩展 |

### 13.6 定稿动作

完成 §13.1–§13.4 中所有 `[ ]` 项后：

1. 文档头 `状态` 改为 **v2.0-final**
2. 版本号 `v2.0-draft` → `v2.0`
3. 同步修订 `competitor-research.md` 中与配置模型相关的段落（至少 §Envelope 边声明部分）
4. 冻结接口：§4 核心接口变更需 ADR 或 bump minor

---

> 本文档基于对 v1 代码库的全面分析和 10 个竞品项目（Benthos/Redpanda Connect、Vector、Kafka Connect、Knative Eventing、Argo Events、Fluentd、Flume、OpenTelemetry Collector、CloudEvents CESQL、Cloudera Envelope）的深入调研，经 40 项架构决策澄清后编写。完整调研详见 `competitor-research.md`，设计评审详见 `design-review.md`。
# 配置规范

本文档说明 EdgeStream Pipeline 可用的全部配置项。配置支持 **YAML** 与 **HOCON** 两种格式，解析为统一的 `PipelineConfig`，再展开为运行时 `TopologyIR`。

> **与 Envelope 的关系：** EdgeStream 在布局上对齐 [Cloudera Envelope](https://github.com/cloudera-labs/envelope/blob/master/docs/configurations.adoc) 的 `steps` 块结构，但字段名保持 edgestream 自有命名（如 `depends_on`、`source`/`transform`/`sink`）。Envelope 用户迁移时：`application` → `engine`，`input` → `source`，`deriver` → `transform`，`output` → `sink`，`dependencies` → `depends_on`。

## 目录

- [示例配置](#示例配置)
- [Engine](#engine)
- [Steps](#steps)
- [Source](#source)
- [Transform](#transform)
- [Sink](#sink)
- [Codec](#codec)
- [边（depends_on）](#边depends_on)
- [Pipeline 顶层](#pipeline-顶层)
- [平坦 pipeline 写法](#平坦-pipeline-写法)
- [变量替换](#变量替换)
- [配置校验](#配置校验)
- [格式与 CLI](#格式与-cli)
- [Envelope 字段对照](#envelope-字段对照)

---

## 示例配置

以下示例从 Kafka 读取订单 JSON，经 map/filter 处理后写回 Kafka。YAML 与 HOCON 能力对等，此处展示推荐写法。

**YAML：**

```yaml
apiVersion: edgestream/v1
kind: Pipeline
metadata:
  name: order-processing

engine:
  max_workers: 16
  max_inflight: 10000

edgeDefaults:
  buffer: { type: memory, size: 64, strategy: block }

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

  filter-high:
    depends_on: [enrich]
    transform:
      type: filter
      config:
        dsl: "payload.total > 100"

  kafka-out:
    depends_on: [filter-high]
    sink:
      type: kafka
      encoder: json
      ordering: ordered
      batch: { size: 100, timeout: 1s }
      config:
        brokers: ["localhost:9092"]
        topic: orders-enriched
```

**等价 HOCON：**

```hocon
engine {
  max_workers = 16
  max_inflight = 10000
}

edgeDefaults {
  buffer { type = memory, size = 64, strategy = block }
}

steps {
  kafka-in {
    source {
      type = kafka
      decoder = json
      config {
        brokers = [${KAFKA_BROKERS}]
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
        dsl = "payload.total = payload.price * payload.quantity"
      }
    }
  }
  filter-high {
    depends_on = [enrich]
    transform {
      type = filter
      config { dsl = "payload.total > 100" }
    }
  }
  kafka-out {
    depends_on = [filter-high]
    sink {
      type = kafka
      encoder = json
      ordering = ordered
      batch { size = 100, timeout = 1s }
      config {
        brokers = ["localhost:9092"]
        topic = orders-enriched
      }
    }
  }
}
```

---

## Engine

引擎级配置，前缀为 `engine.`（对标 Envelope 的 `application.*`）。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `max_workers` | int | `16` | Pipeline 内 stage 并发 worker 上限 |
| `max_inflight` | int | `10000` | 全局在途消息上限（背压传播） |
| `error_mode` | string | `propagate` | 全局错误策略：`propagate`（传播并失败）、`ignore`（跳过消息）、`silent`（静默跳过） |
| `drain_timeout` | duration | `30s` | 优雅停机时等待排空的最长时间 |

```yaml
engine:
  max_workers: 16
  max_inflight: 10000
  error_mode: propagate
  drain_timeout: 30s
```

---

## Steps

Step 配置前缀为 `steps.{name}.`。每个 step 可包含 `source`、`transform`、`sink` 子块之一或多个，以及 `depends_on` 声明入边。

### 通用 Step 字段

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `depends_on` | 序列或映射 | — | 上游 step/stage id 列表，可附带 per-edge 属性（见[边](#边depends_on)） |
| `step_type` | string | `data` | Step 类型（`data` / `loop` / `decision` / `task`）；v2.1+ 规划，当前仅 `data` 生效 |

### Step 展开规则

| 配置 | 展开后的 stage id |
|------|-------------------|
| 仅 `source` | `{name}`（kind=source） |
| 仅 `transform` | `{name}`（kind=transform） |
| 仅 `sink` | `{name}`（kind=sink） |
| `transform` + `sink` 合体 | transform=`{name}`，sink=`{name}-sink`，自动添加内边 `{name} → {name}-sink` |

> **术语：** 配置层 **step** → 运行时 **stage**（`StageIR`）+ **edge**（`EdgeIR`，由 `depends_on` 展开）。

---

## Source

Source 配置属于 step，前缀为 `steps.{name}.source.`。

### 通用 Source 字段

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `type` | string | Source 插件类型（必填） |
| `decoder` | string 或 CodecRef | 入站 payload 解码器；简写为 codec 名称，或 `{ ref: my-codec }` |
| `config` | object | 插件自有配置 |

### kafka

从 Kafka 主题消费消息。消费成功后通过 at-least-once Ack 提交 offset。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `brokers` | string[] | — | **必填。** Broker 地址列表，如 `["localhost:9092"]` |
| `topics` | string[] | — | **必填。** 订阅主题列表（也可用单数 `topic`） |
| `group_id` | string | step id | Consumer group ID |
| `min_bytes` | int | `1` | Fetch 最小字节数 |
| `max_bytes` | int | `10000000` | Fetch 最大字节数 |

写入 metadata：`kafka.topic`、`kafka.partition`、`kafka.offset`、`kafka.key`。

```yaml
source:
  type: kafka
  decoder: json
  config:
    brokers: ["${KAFKA_BROKERS}"]
    topics: [orders, orders-retry]
    group_id: order-consumer
```

对标 Envelope `input.type = kafka`：`brokers` ↔ `brokers`，`topics` ↔ `topics`，`group_id` ↔ `group.id`。

### cron

按 cron 表达式定时产生消息，适用于轮询、心跳、批触发等场景。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `schedule` | string | — | **必填。** 6 字段 cron（含秒），如 `"0 */1 * * * *"`（每分钟）；也可用 `cron` 别名 |
| `timezone` | string | `UTC` | IANA 时区，如 `Asia/Shanghai` |
| `payload` | string | `{"tick":true}` | 每次 tick 产生的消息 body（JSON 字符串） |

```yaml
source:
  type: cron
  decoder: json
  config:
    schedule: "0 0 * * * *"
    timezone: Asia/Shanghai
    payload: '{"price":10,"quantity":5}'
```

### http_server

启动 HTTP 服务接收请求 body 作为消息。适用于 Webhook 接入。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `address` | string | `:8080` | 监听地址；也可用 `listen` 别名 |
| `path` | string | `/` | 接收路径 |

写入 metadata：`http.method`、`http.path`。

```yaml
source:
  type: http_server
  config:
    address: ":8080"
    path: /events
```

---

## Transform

Transform 配置前缀为 `steps.{name}.transform.`（对标 Envelope `deriver`）。

### 通用 Transform 字段

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | — | Transform 插件类型（必填） |
| `workers` | int | `1` | 并行 worker 数 |
| `predicate` | string | — | CEL 表达式；非空时仅对匹配消息执行 transform |
| `error_mode` | string | 继承 `engine.error_mode` | 覆盖本 transform 的 DSL 求值错误策略 |
| `config` | object | — | 插件自有配置 |

### map

使用 **eql** DSL 对 payload/metadata 做字段映射与赋值。

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `dsl` | string | **必填。** eql 映射语句，支持 `payload.x = expr`、`del(field)` 等 |

```yaml
transform:
  type: map
  workers: 8
  config:
    dsl: |
      payload.total = payload.price * payload.quantity
      metadata.enriched = true
```

### filter

使用 **eql** CEL 表达式过滤消息；表达式为 `true` 时保留。

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `dsl` | string | **必填。** CEL 过滤表达式 |

```yaml
transform:
  type: filter
  config:
    dsl: "payload.total > 100"
```

### route

按命名路由将消息分流；匹配的路由名写入 `metadata["er-route"]`，供下游 `depends_on` 的 `route` 字段使用。

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `routes` | map[string]string | **必填。** 路由名 → CEL 表达式；支持 `_default` 兜底路由 |
| `route_order` | string[] | 可选。路由求值顺序；未指定时按字母序，`_default` 最后 |

```yaml
transform:
  type: route
  config:
    routes:
      us: "metadata.region == 'us'"
      eu: "metadata.region == 'eu'"
      _default: "true"
    route_order: [us, eu, _default]
```

### wasm

WASM 自定义 transform（v2.0-alpha 尚未实现）。

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `module` | string | WASM 模块路径（规划中） |

---

## Sink

Sink 配置前缀为 `steps.{name}.sink.`（对标 Envelope `output`）。

### 通用 Sink 字段

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | — | Sink 插件类型（必填） |
| `encoder` | string 或 CodecRef | — | 出站 payload 编码器 |
| `batch` | object | — | 批处理配置（见下表） |
| `ordering` | string | `unordered` | `ordered`（严格顺序，强制 `max_in_flight=1`）或 `unordered` |
| `max_in_flight` | int | `1` | 并发写入 goroutine 数；`ordered` 时必须为 1 |
| `config` | object | — | 插件自有配置；支持 `parameter.*` 透传 |

**batch 子字段：**

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `size` | int | `1` | 批大小（条数） |
| `timeout` | duration | — | 批超时；到达超时后刷新未满批次 |
| `max_bytes` | int | — | 批最大字节数（规划中） |

### kafka

向 Kafka 主题写入消息。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `brokers` | string[] | — | **必填。** Broker 地址列表 |
| `topic` | string | — | **必填。** 目标主题 |
| `balancer` | string | `least_bytes` | 分区策略：`least_bytes` 或 `hash` |

若消息 metadata 含 `kafka.key`，将作为消息 key 写入。

```yaml
sink:
  type: kafka
  encoder: json
  ordering: ordered
  batch: { size: 100, timeout: 1s }
  config:
    brokers: ["localhost:9092"]
    topic: orders-enriched
    parameter:
      compression.type: lz4
```

对标 Envelope `output.type = kafka`：`brokers` ↔ `brokers`，`topic` ↔ `topic`；`parameter.*` 透传 Kafka 客户端参数。

### http

向 HTTP 端点 POST（或自定义 method）消息 body。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `url` | string | — | **必填。** 目标 URL |
| `method` | string | `POST` | HTTP 方法 |

```yaml
sink:
  type: http
  encoder: json
  config:
    url: https://api.example.com/orders
    method: POST
```

### drop

丢弃消息（用于测试、调试或显式吞没分支）。

无额外配置项。

```yaml
sink:
  type: drop
```

---

## Codec

编解码器将 `[]byte` payload 与结构化数据互转。Source 使用 `decoder`，Sink 使用 `encoder`。

### 引用方式

**简写（内联类型名）：**

```yaml
decoder: json
encoder: json
```

**引用顶层声明：**

```yaml
codecs:
  - name: avro-orders
    type: avro
    config: { schema: "..." }

source:
  decoder: { ref: avro-orders }
```

**内联带配置：**

```yaml
decoder:
  type: json
  config: { ... }
```

### 内置 Codec

| 类型 | 说明 | 配置项 |
|------|------|--------|
| `json` | JSON ↔ `map[string]any` | 无 |
| `raw` | 透传 `[]byte`，不做解析 | 无 |

> `avro`、`protobuf` 等 codec 在路线图中；当前 alpha 内置 `json` 与 `raw`。

---

## 边（depends_on）

拓扑边**仅**通过各 step/stage 的 `depends_on` 声明（对标 Envelope `dependencies`）。**不要**使用已废弃的顶层 `edges:` 列表（v1 兼容，会输出 deprecation warning）。

### 值形态

| 形态 | YAML 示例 | 说明 |
|------|-----------|------|
| **序列** | `depends_on: [kafka-in, enrich]` | 元素为字符串（上游 id）或单键对象 |
| **映射** | `depends_on: { splitter: { route: us } }` | 键=上游 id；值=边属性 |

**序列元素示例：**

```yaml
depends_on:
  - kafka-in
  - splitter: { route: eu, buffer: { size: 128 } }
```

**映射形态示例（HOCON 更常用）：**

```hocon
depends_on {
  kafka-in {}
  splitter {
    route = us
    buffer { type = memory, size = 128, strategy = block }
    delivery {
      retry { max = 3, backoff = exponential }
      dlq = dlq-sink
    }
    required = false
  }
}
```

### 边属性字段

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `condition` | string | — | CEL 表达式；空=始终匹配。与 `route` **互斥** |
| `route` | string | — | `route` transform 的命名路由；展开为 `metadata["er-route"] == "{route}"` |
| `buffer` | object | 继承 `edgeDefaults.buffer` | 边级缓冲（见下表） |
| `delivery` | object | — | 投递策略：retry / timeout / dlq |
| `required` | bool | `true` | `false` = best-effort 边，下游失败不阻塞上游 Ack |

**buffer 子字段：**

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `type` | string | `memory` | `memory` 或 `disk` |
| `size` | int | `64` | 缓冲容量（条数） |
| `strategy` | string | `block` | 满时策略：`block`（背压）或 `drop` |
| `key` | string[] | — | 分区键字段（per-partition ordering，v2.2 规划） |
| `disk_path` | string | — | disk buffer 路径 |
| `disk_max_size` | int64 | — | disk buffer 最大字节 |
| `disk_sync_interval` | duration | — | disk buffer 刷盘间隔 |

**delivery 子字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `retry.max` | int | 最大重试次数 |
| `retry.backoff` | string | 退避策略，如 `exponential` |
| `timeout` | duration | 单次投递超时 |
| `dlq` | string | DLQ sink stage id；覆盖 pipeline 级 `dlq.sink` |

### 分支路由完整示例

```yaml
steps:
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
      splitter: { route: us }
    sink:
      type: http
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

---

## Pipeline 顶层

与 `steps` 并列的 Pipeline 级配置。

### metadata（YAML / CRD）

| 字段 | 说明 |
|------|------|
| `apiVersion` | 如 `edgestream/v1`（YAML/CRD 专用） |
| `kind` | 如 `Pipeline` |
| `metadata.name` | Pipeline 名称；展开为 `TopologyIR.Name` |

### edgeDefaults

未在 `depends_on` 中显式指定的边属性默认值。

```yaml
edgeDefaults:
  buffer: { type: memory, size: 64, strategy: block }
  required: true
```

### dlq

Pipeline 级死信队列默认目标。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `sink` | string | — | DLQ sink stage id |
| `include_current_payload` | bool | `false` | DLQ 消息是否包含失败时的 payload |

```yaml
dlq:
  sink: dlq-sink
  include_current_payload: false
```

### observability

| 字段 | 说明 |
|------|------|
| `metrics.enabled` | 是否暴露 Prometheus 指标 |
| `metrics.port` | 指标 HTTP 端口 |
| `metrics.path` | 指标路径，默认 `/metrics` |

```yaml
observability:
  metrics:
    enabled: true
    port: 9090
    path: /metrics
```

### codecs

顶层 codec 声明列表，供 `decoder`/`encoder` 的 `ref` 引用。

```yaml
codecs:
  - name: json-strict
    type: json
    config: {}
```

### edges（已废弃）

```yaml
# 不要使用 — 请改用 depends_on
edges:
  - from: splitter
    to: us-sink
    route: us
```

---

## 平坦 pipeline 写法

与 `steps` **二选一**；适合极简管道或早期草案迁移。每行一个 stage，需显式声明 `kind` 与 `type`。

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
        payload.total = payload.price * payload.quantity

  - id: kafka-sink
    kind: sink
    type: kafka
    depends_on: [enrich]
    encoder: json
    ordering: ordered
    batch: { size: 100, timeout: 1s }
    config:
      brokers: [localhost:9092]
      topic: orders-enriched
```

| 字段 | 含义 |
|------|------|
| `kind` | Stage 角色：`source` / `transform` / `sink` |
| `type` | 插件实现名：`kafka`、`map`、`http` 等 |
| `depends_on` | 与 steps 模式语法相同 |

---

## 变量替换

两种格式均支持环境变量替换。

| 语法 | 说明 |
|------|------|
| `${VAR}` | 替换为环境变量值；未设置时保留字面量（YAML）或空（HOCON） |
| `${?VAR}` | 可选替换；未设置时省略 |

```yaml
config:
  brokers: ["${KAFKA_BROKERS}"]
  optional_tag: ${?OPTIONAL_TAG}
```

```hocon
config {
  brokers = [${KAFKA_BROKERS}]
  optional_tag = ${?OPTIONAL_TAG}
}
```

**HOCON duration：** 时间字段可直接写 `1s`、`500ms`、`1m` 等。

**parameter 透传：** Sink `config` 内 `parameter.*` 键会剥除前缀后传给底层客户端（对标 Envelope Kafka `parameter.*`）。

```yaml
config:
  topic: orders
  parameter:
    compression.type: lz4
```

等价 HOCON：`parameter.compression.type = lz4`

---

## 配置校验

`edgestream validate` 在启动前执行以下检查：

| 检查项 | 说明 |
|--------|------|
| stage ID 唯一 | 展开后不得重复 |
| 边引用有效 | `from`/`to` 必须引用已定义 stage |
| 拓扑约束 | 至少一个 source、一个 sink；source 无入边；sink 无出边 |
| 无环 | DAG 不得存在循环依赖 |
| 连通性 | 至少一条 source → sink 通路 |
| route/condition 互斥 | 同一边不可同时设置 |
| ordered + max_in_flight | `ordering: ordered` 时 `max_in_flight` 必须为 1 |
| codec ref | `decoder`/`encoder` 的 `ref` 须在 `codecs` 中声明 |
| 插件类型 | `type` 须在注册表中存在且与 `kind` 匹配 |

```bash
edgestream validate --config pipeline.yaml
edgestream validate --config-dir ./pipelines/
```

---

## 格式与 CLI

| 扩展名 | 解析器 |
|--------|--------|
| `.yaml` / `.yml` | YAML（`gopkg.in/yaml.v3`） |
| `.conf` / `.hocon` | HOCON（`github.com/gurkankaymak/hocon`） |

```bash
# 运行单个 Pipeline
edgestream run --config pipeline.yaml
edgestream run --config pipeline.conf

# 显式指定格式（扩展名非标准时）
edgestream run --config pipeline.txt --format hocon

# 目录内混放 YAML / HOCON
edgestream run --config-dir ./pipelines/

# 仅校验
edgestream validate --config testdata/pipelines/linear.yaml
```

K8s Operator **仅接受 YAML CRD**；HOCON 用于集群外本地配置。

---

## Envelope 字段对照

从 Envelope 迁移时参考下表。EdgeStream **不**映射 Envelope 的 `planner` 块；写入语义由 sink `config` 自行表达。

| Envelope | EdgeStream | 备注 |
|----------|--------|------|
| `application.name` | `metadata.name` | |
| `application.executor.*` | `engine.max_workers` 等 | 语义不同，按 edgestream 引擎调优 |
| `application.batch.milliseconds` | — | 流式微批由 sink `batch` 控制 |
| `steps.{name}.dependencies` | `steps.{name}.depends_on` | 支持序列与映射两种形态 |
| `steps.{name}.input` | `steps.{name}.source` | |
| `steps.{name}.input.format` | `source.decoder` | 编解码独立为 Codec |
| `steps.{name}.deriver` | `steps.{name}.transform` | |
| `steps.{name}.output` | `steps.{name}.sink` | |
| `steps.{name}.planner` | — | 不映射 |
| `input/parameter.*` | `source.config` / `parameter.*` | 透传插件参数 |
| `output/parameter.*` | `sink.config` / `parameter.*` | 透传插件参数 |
| `udfs` | — | v2.1+ 规划 `cel.functions` |

### Source 插件对照

| Envelope input.type | EdgeStream source.type | 主要 config 字段 |
|---------------------|-------------------|------------------|
| `kafka` | `kafka` | `brokers`, `topics`, `group_id` |
| `filesystem` | — | v2.1+ 规划 |
| `jdbc` | — | v2.1+ 规划 |
| — | `cron` | `schedule`, `payload` |
| — | `http_server` | `address`, `path` |

### Sink 插件对照

| Envelope output.type | EdgeStream sink.type | 主要 config 字段 |
|----------------------|-----------------|------------------|
| `kafka` | `kafka` | `brokers`, `topic` |
| `filesystem` | — | v2.1+ 规划 |
| `log` | `drop` | 测试/调试用途 |
| — | `http` | `url`, `method` |

---

## 相关文档

- [设计方案 §8 配置模型](../edgestream-design.md#8-配置模型) — 设计背景与展开规则
- [eql DSL](../edgestream-design.md#7-dsl-语言设计-eql) — map/filter/route 表达式语法
- [示例配置](../testdata/pipelines/) — `linear.yaml`、`linear.conf`

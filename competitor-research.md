# Event Router / Stream Processing Projects — Comparison with `riverpod`

> **注意（2025-06-24）：** 文末部分「建议」已被 `riverpod-design.md` v2.0-draft 吸收或推翻（`depends_on`、Codec 体系、不采纳 Planner 等）。**以 `riverpod-design.md` 为准**；下文保留调研原始脉络。

Research conducted against the riverpod design (`riverpodouter-v2-design.md`) for a
Go-based DAG event router with Source/Transform/Sink, per-edge buffers, a custom
DSL ("eql"), and a Kubernetes operator.

The same analysis is provided inline in this file; references at the end.

- [1. Knative Eventing](#1-knative-eventing)
- [2. Argo Events](#2-argo-events)
- [3. Kafka Connect](#3-kafka-connect)
- [4. Fluentd / Fluent Bit](#4-fluentd--fluent-bit)
- [5. Apache Flume](#5-apache-flume)
- [6. Newer Go/Rust stream routers](#6-newer-gorust-stream-routers--benthos-vector-streamdal)
- [7. CloudEvents Spec & CESQL](#7-cloudevents-spec--cesql)
- [8. OpenTelemetry Collector](#8-opentelemetry-collector)
- [9. Cloudera Envelope](#9-cloudera-envelope)
- [Cross-cutting summary](#cross-cutting-summary)
- [Top concrete things riverpod should steal](#top-concrete-things-riverpod-should-steal-in-priority-order)

---

## 1. Knative Eventing

Knative Eventing is an event-driven platform built entirely on CloudEvents (HTTP POST). Core
abstractions:

- **Sources** (ApiServerSource, KafkaSource, PingSource, IntegrationSource,
  RedisStreamSource, etc.) produce events conforming to CloudEvents v1.0.
- **Broker**: an HTTP endpoint that ingests events and durably stores/routes them.
  Backed by pluggable **Broker types** — Channel-based Broker, Kafka Broker,
  RabbitMQ Broker.
- **Trigger**: subscribes to a Broker with an attribute-based or CESQL **filter**
  and forwards matching events to a **sink** (Knative Service, JobSink,
  KafkaSink, IntegrationSink to S3/SNS/SQS). Example:

  ```yaml
  apiVersion: eventing.knative.dev/v1
  kind: Trigger
  metadata: { name: my-trigger }
  spec:
    broker: default
    filter:
      attributes: { type: dev.knative.foo, source: /my-source }   # or CESQL filter
    subscriber: { ref: { apiVersion: v1, kind: Service, name: my-svc } }
  ```

- **Channel**: a separate abstraction between Broker/Trigger for point-to-point
  event delivery with pluggable implementations (Kafka Channel, InMemoryChannel).
  **Subscriptions** wire a Channel to a Subscriber/Reply.
- **Flows**: `Sequence` (linear pipeline, reply-to-next) and `Parallel`
  (fan-out to multiple branches with optional filter per branch). These are
  higher-level DAG-like compositions.
- **DeliverySpec**: every Trigger/Sink/Channel can carry `retry`, `retryAfterMax`,
  `timeout`, `deadLetterSink` — the canonical pattern of "delivery concerns
  decoupled from the consumer".
- **Event Registry**: an `EventType` CRD where sources declare event types so
  consumers can discover available events (with Backstage plugin).
- **Transforms**: a built-in **JSONata** transform step on Triggers/Broker, so
  filtering and a small transform can live in one CRD.

### Distinctive ideas

- Knative's **Broker/Trigger** cleanly separates durable ingress (Broker) from
  declarative subscription (Trigger). riverpod's Source/Sink mixing could borrow
  this "registry + subscription" split.
- **Event Registry** is a metadata-first design — declaring what event types
  exist *as first-class CRDs*, decoupled from where they're routed.

### What riverpod could borrow / is missing

- Add an **EventType CRD / registry** so that Sinks can discover available
  event sources and DSL filters can reference event types by name, not magic
  strings.
- Adopt **DeliverySpec** uniformly (retry, timeout, `deadLetterSink`) as a
  per-edge concern, not a global sink concern — Knative treats delivery as
  orthogonal to business logic.

---

## 2. Argo Events

Argo Events uses four primitives:

- **EventSource**: 20+ source types (webhook, S3, Kafka, SQS, Calendar,
  GitHub, Minio, etc.) emitting CloudEvents onto the EventBus.
- **EventBus**: NATS/JetStream/Kafka streams used as the transport between
  EventSources and Sensors.
- **Sensor**: a CRD listing **dependencies** (events the sensor waits for) and
  **triggers** (actions to take). Each dependency has filters.
- **Trigger**: an action (Argo Workflow, HTTP, Lambda, K8s object, NATS, Kafka,
  Slack, etc.).

The distinctive feature is **conditional triggering based on event
combinations**:

```yaml
spec:
  dependencies:
    - { name: dep01, eventSourceName: webhook-a, eventName: example01 }
    - { name: dep02, eventSourceName: webhook-a, eventName: example02 }
    - { name: dep03, eventSourceName: webhook-b, eventName: example03 }
  triggers:
    - template: { conditions: "dep02", name: t1, http: { url: http://abc.com/h1, method: GET } }
    - template: { conditions: "dep02 && dep03", name: t2, http: { url: http://abc.com/h2, method: GET } }
    - template: { conditions: "(dep01 || dep02) && dep03", name: t3, http: { url: http://abc.com/h3, method: GET } }
```

`conditions` is a boolean expression over dependency names supporting only
`&&`, `||`. Default when missing is `&&` of all deps. **conditionsReset** can
clear accumulated matching state by `cron` (e.g. reset at 23:59) to prevent
yesterday's `A` matching today's `B`.

Filter types include: **expr filter** (CEL expressions), **data filter** (JSON
payload matchers), **context filter** (CloudEvents context attributes), **time
filter** (event timestamp range), **script filter** (Lua).

### Distinctive ideas

- Sensor models *stateful* event correlation: accumulated matches across
  distinct event sources waiting to resolve a logical condition. This is more
  than DAG routing — it's a **compound-event detector** over an EventBus.
- `conditionsReset` is a real-world answer to the "stale dependency" problem;
  without resetting, a `dep01 && dep02` where one side arrives daily and the
  other stalls can pair across time windows incorrectly.

### What riverpod could borrow / is missing

- riverpod's DAG currently implies **stateless per-event routing**. Consider a
  "Compound Source" or **join node** that waits on a boolean expression of
  upstream sources with window/time-based reset primitives — a feature Argo
  has that pure DAG routers don't.
- Borrow the **interpreter-free boolean expression** style
  (`dep01 && (dep02 || dep03)`) — riverpod's DSL could expose this directly as a
  "Trigger" construct sitting at DAG joins, plus the `byTime` reset knob.

---

## 3. Kafka Connect

Kafka Connect is the worker-based streaming integration framework for Kafka. Key
components:

- **Source/Sink connectors**: produce to Kafka topics / consume from them.
  Developed against the `SourceConnector`/`SinkConnector` +
  `SourceTask`/`SinkTask` interfaces.
- **Single Message Transforms (SMT)**: a chain of stateless per-record
  transforms executed *after* a source produces and *before* a sink consumes.
  Configured as an ordered list:

  ```json
  "transforms": "dropUnwanted,insertTs,flatten",
  "transforms.dropUnwanted.type": "org.apache.kafka.connect.transforms.Drop",
  "transforms.insertTs.type": "org.apache.kafka.connect.transforms.InsertField",
  "transforms.insertTs.timestamp.field": "ingested_at",
  "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
  "transforms.flatten.delimiter": "."
  ```

  Available SMTs include Cast, Drop, DropHeaders, Riverpod, ExtractField,
  ExtractTopic, Filter (Apache Kafka), Filter (Confluent, supports
  `filter.condition`), Flatten, GzipDecompress, HeaderFrom, HoistField,
  InsertField, InsertHeader, MaskField, MessageTimestampRouter, RegexRouter,
  ReplaceField, SetSchemaMetadata, TimestampConverter, TimestampRouter,
  TombstoneHandler, ValueToKey. **Predicates** (e.g. `TopicNameMatches`) can
  negate or conditionally apply an SMT.
- **Schema Registry**: Avro/Protobuf/JSON Schema registry for types — connectors
  negotiate schemas, enabling evolution and compatibility checks.
- **Connect REST API**: full cluster management — `/connectors`,
  `/connector-plugins`, `/tasks`, `/status`, `/restart`. Workers expose a
  unified admin API.
- **Distributed vs Standalone mode**: standalone = single worker + file-backed
  config; distributed = group of workers, config in an internal Kafka topic
  (`config.storage.topic`, `offset.storage.topic`, `status.storage.topic`),
  automatic rebalance of connector tasks across workers on failure.
- **Error handling**: `errors.tolerance: none|all`,
  `errors.deadletterqueue.topic.name`,
  `errors.deadletterqueue.topic.replication.factor`,
  `errors.deadletterqueue.context.headers.enable`, `errors.log.enable`,
  `errors.log.include.messages`. The DLQ pattern is uniformly specified.
- **KIP-618**: Exactly-once source support via source-provided transactional IDs
  and partitioning guarantees, plus `transactional.id.alias` and per-task EOS
  over Kafka producer transactions. Combined with sink EOS (KIP-488/588),
  enables real end-to-end exactly-once across `source → Kafka → sink`.

### Distinctive ideas

- The **SMT chain** is conceptually identical to riverpod's Transform chain —
  but it lives *at the connector level*, not at the broker. It's a serial
  pipeline that runs once per record, with **predicates** making individual
  transforms optional.
- **Schema Registry** + Connect's contract-with-IDL model is the *only* project
  on this list that enforces data shape evolution end-to-end.

### What riverpod could borrow

- The **predicate-on-transform** pattern (`transforms.X.predicate`) so a
  Transform step can be conditionally applied per event without writing it as
  a separate conditional construct in the DSL — much more ergonomic than
  riverpod's current "branch for condition" pattern.
- The DLQ story: standardize `errors.tolerance` +
  `errors.deadletterqueue.*` semantics as a per-edge Sink config block,
  including context headers (`dlq.context.headers.enable`) so downstream can
  see *why* an event was DLQ'd.

---

## 4. Fluentd / Fluent Bit

Fluentd's event model is `(tag, time, record)` — a triple. The `tag` (dotted
string, e.g. `myapp.access`) is the routing dimension, not a column.

### Match pattern routing (Fluentd's distinctive paradigm)

```text
<filter myapp.access>          # filter is matched by tag pattern
  @type record_transformer
  <record> { host_param "#{Socket.gethostname}" } </record>
</filter>

<match myapp.access>             # output is matched by tag pattern
  @type file
  path /var/log/fluent/access
</match>
```

Match patterns support: `*` (single tag segment), `**` (zero or more segments),
`{a,b,c}` (alternation), `/regex/`, `#{...}` (Ruby embedding), and
*whitespace-delimited multi-pattern* (e.g. `<match a.** b.*>`). **Order matters** —
first match wins, so wider patterns must come last.

### Filter pipeline

All `<filter pattern>` blocks matching a tag form an ordered
`Input → filter 1 → … → filter N → Output` pipeline (vs riverpod's DAG).
`record_transformer`, `grep`, `parser` are common filters.

### Label directive

Groups `<filter>` + `<match>` blocks into a sub-router bypassing tag ordering;
`@ERROR` label is the built-in destination for buffer-full / invalid records
(analogous to DLQ but per-router). `@ROOT` re-enters the default route (used by
`concat` filter to release timeout events).

### Buffer chunks

Output plugins declare a `<buffer chunk-key, ...>` block. **Chunk keys**
partition events — e.g. `<buffer tag, time>` creates one chunk per tag per time
slice. Buffer types: `memory` (in-memory queue of chunk objects) or `file`
(each chunk is a file on disk — survives restart). Buffer has `@type`, `path`,
`flush_mode`, `flush_interval`, `chunk_limit_records`, `total_limit_size`,
`retry_max_times`, `retry_wait`, `retry_exponential_backoff`, `overflow_action`
(`throw_exception`, `block`, `drop_oldest_chunk`). This is the full
back-pressure / durability knob set.

### Fluent Bit

C/Rust rewrite — globally-pipelined engine with
`Input → Parser → Filter → Router → Output`, similar match patterns plus TLS,
smaller memory footprint, a `lua` filter.

### Distinctive ideas

- **Tag-based routing** collapses "Destination selection" to a *pattern
  language over a single string field* rather than an explicit graph. It's the
  opposite philosophy from riverpod's DAG: graph-centric vs. tag-pattern-centric.
  Both are valid; matches are cheap to reason about but the topology is implicit.
- **Chunk keys + flush modes** is a much richer buffer model than riverpod's
  "per-edge buffers" — partitioning, spilling to file, multiple overflow
  strategies.
- `out_copy` plugin is the explicit fan-out primitive (`<match **>` with `copy`
  sends to multiple outputs in parallel).

### What riverpod could borrow / is missing

- Adopt a **chunk-key abstraction** over per-edge buffers so a single edge can
  partition events (e.g. `buffer.key = [tenant, type]`) — closer to Kafka
  Connect's partition keys than to current riverpod buffers, and gives ordering
  and bounded back-pressure per partition.
- The **`@ERROR` label** idea: a named "fallback router" reachable by *any*
  edge on error/buffer-overflow — letting users define their own DLQ routing
  sub-DAG declaratively, instead of just a sink URL.

---

## 5. Apache Flume

Flume's architecture is **Source → Channel → Sink** within an **Agent** JVM
process.

- **Source**: Avro, Thrift, Kafka, HTTP, Syslog, Netcat, Exec, JMS, SpoolDir,
  Taildir, Twitter, SequenceGenerator.
- **Channel** (passive store): `MemoryChannel` (in-memory queue, lost on crash),
  `FileChannel` (filesystem-backed, crash-durable), `KafkaChannel` (Kafka
  topic-based, replicated durability).
- **Sink**: HDFS, HBase, Avro, Thrift, Logger, Null, Elasticsearch, Kafka,
  HTTP, MorphlineSolr, RollingFile.
- **Interceptor chains**: every source has `interceptors = i1 i2 i3`, each
  with `.type`, optionally a builder, dropping events or modifying headers.
  Interceptors are stateless transforms that run *inside the source*, before
  the channel.
- **Transactional channel semantics**: source and sink both wrap their channel
  put/take in a **ChannelTransaction** (`begin()` / `commit()` / `rollback()`).
  The sink removes events from a channel only after the *next hop's channel (or
  terminal repository)* has accepted them — **single-hop reliability**
  guarantees end-to-end durability across multi-hop flows.
- **Channel selectors**: `replicating` (every event to all channels) and
  `multiplexing` (route by event header value to subset of channels):

  ```properties
  agent.sources.r1.selector.type = multiplexing
  agent.sources.r1.selector.header = State
  agent.sources.r1.selector.mapping.CA = mem-channel-1
  agent.sources.r1.selector.mapping.NY = mem-channel-1 file-channel-2
  agent.sources.r1.selector.default = mem-channel-1
  agent.sources.r1.selector.optional.CA = file-channel-2
  ```

  The distinction between **required vs optional channels** — required channel
  failure rolls back the whole batch; optional channel failures are silently
  ignored.
- **Sink Groups** + `LoadBalancingSinkProcessor` (round_robin / random / custom
  failover) and `DefaultSinkProcessor`: load balance / failover across multiple
  sinks.
- **Batch semantics**: source/sink batch sizes must be ≤ channel
  `transactionCapacity` (an explicit config check prevents mismatches).

### Distinctive ideas

- **Selector → channel subset** with required/optional split is the cleanest
  primitive for *partial fan-out with mixed durability* — riverpod's per-edge
  buffers treat all sinks uniformly; Flume lets one out-edge be best-effort
  while another is required.
- The **transactional Channel** decouples back-pressure (channel capacity)
  from sink liveness — a sink being down doesn't cause the source to block;
  the channel absorbs until capacity, then transactions rollback.

### What riverpod could borrow

- Per-edge **"required vs optional" sinks** would let users declare an audit
  sink that doesn't roll back the whole batch when it fails, distinct from a
  primary sink that *does* — currently riverpod's per-edge buffers have no
  built-in "optional edge" concept.
- Explicit **transaction-capacity** invariant check on per-edge buffers
  (analog of Flume's `batchSize <= transactionCapacity`) baked into
  validation, prevents runtime deadlocks on misconfigured pipelines.

---

## 6. Newer Go/Rust stream routers — Benthos, Vector, Streamdal

(Research note: web search tools were unavailable at research time; this section
relies on prior knowledge of the projects.)

### Benthos (now Alpine / archived-to-Olive) — Go

Benthos's config is an explicit pipeline:

```yaml
input:
  kafka: { addresses: [...], topics: [foo] }
pipeline:
  processors:
    - mapping: |
        root.id = uuid_v4()
        root.body = this.value.lowercase()
    - switch:
        - check: this.type == "a"
          processors: [ { mapping: root.target = "A" } ]
        - check: this.type == "b"
          processors: [ { mapping: root.target = "B" } ]
output:
  switch:
    cases:
      - check: this.target == "A"
        output: { kafka: { addresses: [...], topic: topicA } }
      - check: this.target == "B"
        output: { http_client: { url: http://x.com } }
  fallback:
    - { s3: { bucket: dlq } }
```

- Pipeline = ordered **processors**; **Bloblang** is the embedded
  imperative/declarative DSL with `root.x = ...`, `this.x`, functions,
  `if`/`switch`/`map`. **Output switches** branch by predicate.
- Built-in retry, back-pressure, batch + batching policies, **WASM plugins**
  for sandboxed transforms, **gRPC plugins**, `schema.registry` processor.

### Vector (Datadog) — Rust

Vector uses `sources → transforms → sinks` with TOML/YAML and the **Remap
Language (VRL)** which compiles to typed programs with a type checker.
Transforms include `remap`, `filter`, `sample`, `dedupe`, `route` (route by
predicate), `lua`, `wasm`. Sinks support backpressure via `healthcheck`,
`request.retry_attempts`, `request.timeout_secs`. The `route` transform emits
events to *component IDs*.

### Streamdal — Go

Streaming-pipeline sampler / inspect / collect framework. More of an
inspection/sampling framework than a router; not really a DAG router.

### Distinctive ideas

- **Bloblang (Benthos)** is the closest parallel to riverpod's custom DSL — but
  it's a deliberately *general-purpose* pure-functional language with
  `root`/`this`, not a config-level predicate syntax. Letting users write
  arbitrary mapping logic in the transform steps (not just key-value routers)
  is what makes Benthos capable of complex branching without `if` ladders of
  components.
- **WASM-based plugins** for transforms in Benthos and Vector — sandboxed,
  language-agnostic, hot-loadable; much safer than native Go plugin binaries.
- Vector's **typed VRL with compile-time checks** is a much more defensible DSL
  experience than ad-hoc parser-DSLs.

### What riverpod could borrow / is missing

- Strongly consider a **Bloblang/VRL-style** mapping sublanguage inside
  Transform steps — predicates alone (CESQL-style) cover filtering, but
  transforms need real expression power (assignment, branching, function
  calls). riverpod's DSL being config-style likely forces reach-out to Go
  plugins for non-trivial transforms; that's a usability gap.
- Adopt **WASM transforms** as a first-class plugin mechanism (vs. compiled Go
  plugins) — enables user-deployed logic without rebuilding the operator image;
  Vector's and Benthos's adoption of WASM is strong evidence it scales.

---

## 7. CloudEvents Spec & CESQL

CloudEvents v1.0.2 defines the canonical event envelope:

- **Required attributes**: `id`, `source` (URI-reference), `specversion`,
  `type`.
- **Optional attributes**: `time` (RFC 3339), `datacontenttype` (MIME),
  `subject`, plus **extension attributes** (any lower-case alphanumeric).
- **Modes**: **binary** (`Ce-*` HTTP headers + body is the data) and
  **structured** (the whole event serialized as JSON/Avro in the body).
- **Data** is explicitly *opaque* — the spec deliberately says filtering
  languages do not address the `data` field.

### CESQL (CloudEvents SQL) — `cesql/spec.md`

CESQL is "**the**" standardized SQL-like filtering language for CloudEvents.
Grammar (EBNF):

```ebnf
expression ::= value-identifier | literal | unary-operation | binary-operation
            | function-invocation | like-operation | exists-operation
            | in-operation | "(" expression ")"

unary-logic-operator  ::= "NOT"
binary-logic-operator ::= "AND" | "OR" | "XOR"
binary-comparison-operator ::= "=" | "!=" | "<>" | ">=" | "<=" | "<" | ">"
binary-numeric-arithmetic-operator ::= "+" | "-" | "*" | "/" | "%"
like-operator  ::= "LIKE"
exists-operator ::= "EXISTS"
in-operator    ::= "IN"
```

- Type system: Boolean, Integer (32-bit), String (correlated to CloudEvents
  types). No null handling — functions/operations on missing attributes return
  errors.
- **Built-in functions**: string (`SUBSTRING`, `CONCAT`, ...), integer (`INT`,
  `SUM`, ...), `LIKE` patterns with `%` and `_`, `EXISTS attr`, `IN (set, ...)`,
  predicate-form `LIKE`/`IN` with optional `NOT`.
- **Total pure functional language** — guaranteed termination, referentially
  transparent, expression result is Boolean/Integer/String; error set is a
  secondary output.
- **Filtering usage**: When used as a filter predicate, the output *MUST* be
  Boolean; on error, the event MUST NOT pass the filter; engines SHOULD use
  "fail fast" mode.
- The spec explicitly relates to the **Subscriptions API** — it's a registered
  filter dialect.

Examples:

```text
type = 'com.example.someevent' AND source LIKE 'myorg.storefront.%'
EXISTS subject AND int(hop) < int(ttl) AND int(hop) < 1000
type IN ('a.b.c', 'd.e.f') AND NOT (source = '/internal')
```

The repo even ships **a TCK** (`cesql_tck/`) — YAML test cases for binary
comparison/logic/math operators, casting functions, EXISTS, IN, LIKE, parse
errors, case sensitivity, etc. Knative ships a real CESQL implementation in
Go (`knative.dev/eventing/pkg/eventfilter/attributes/csql`).

### Distinctive ideas

- CESQL is *deliberately* restricted to CloudEvent context attributes (not
  `data`) — keeping the language small + terminating and giving it a free TCK
  that any implementation can run to claim conformance. riverpod's custom DSL
  probably reinvents strings, integers, AND/OR/NOT, LIKE, EXISTS — all already
  in CESQL.
- The spec *defines error semantics contractually*: "filter MUST NOT pass on
  error" is critical — most ad-hoc DSLs leave this undefined.

### What riverpod could borrow / is missing

- **Yes, riverpod's "eql" is reinventing CESQL.** Strongly recommend implementing
  CESQL (or at least a CESQL subset close enough to run the official TCK) as
  the **built-in predicate language**, and reserving "eql" as a CESQL
  extension namespace for riverpod-specific operators (e.g. referencing
  `data.*` paths as ergonomic shortcut).
- Add the **error contract** ("false on parse error, false on evaluation
  error", with optional error reporting channel) explicitly — and *import the
  official `cesql_tck` test fixtures* into riverpod's CI to lock in conformance.
  This is a near-free interoperability win with the CloudEvents ecosystem.

---

## 8. OpenTelemetry Collector

OTel Collector's pipeline is **Receivers → Processors → Exporters**, declared
in a `service.pipelines.*` block:

```yaml
service:
  pipelines:
    traces:
      receivers: [otlp, jaeger]
      processors: [memory_limiter, batch, filter]
      exporters: [otlp_grpc, zipkin]
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

- **Receivers** (push/pull, multi-signal: traces/metrics/logs): otlp,
  jaeger, zipkin, kafka, prometheus, hostmetrics, fluentforward, …
- **Processors**: ordered transform list (`attributes`, `resource`, `filter`,
  `probabilistic_sampler`, `memory_limiter`, `span`, `batch`, `transform`).
  Each pipeline gets its own *instance* of a referenced processor (state is
  per-pipeline, not per-worker).
- **Exporters** (push/pull): `otlp_grpc`, `otlp_http`, `kafka`, `file`,
  `debug`, `prometheus`, `prometheusremotewrite`, `zipkin`.
- **Extensions**: orthogonal concerns (`health_check`, `pprof`, `zpages`,
  `oidc` authenticator) wired in `service.extensions`. **Authenticators are
  extensions** referenced by receivers/exporters via
  `auth: { authenticator: oidc }` — cleanly separates transport auth from
  pipeline logic.
- Multiple `--config` sources with `file:`, `env:`, `yaml:`, `http(s):`
  providers; configs are merged in-memory (similar to Kustomize overlays);
  `--set key=value` overrides at runtime.

### Connector concept (key for riverpod's DAG)

A **Connector** is *both* exporter and receiver — it consumes as an exporter
at the tail of one pipeline and emits as a receiver at the head of another:

```yaml
connectors:
  count:
    spanevents:
      my.prod.event.count:
        description: "number of prod events"
        conditions:
          - 'attributes["env"] == "prod"'
          - 'name == "prodevent"'
service:
  pipelines:
    traces:
      receivers: [foo]
      exporters: [count]
    metrics:
      receivers: [count]
      exporters: [bar]
```

This allows: **fan-out** (one upstream pipeline → multiple downstream
pipelines), **fan-in** (multiple upstream → one downstream), **signal-type
conversion** (traces → metrics via `count`/`spanmetrics`), and **routing** via
the **routing connector**:

```yaml
connectors:
  routing:
    default_pipelines: [logs/other]
    table:
      - context: resource
        condition: 'attributes["env"] == "prod"'
        action: copy            # match the route, but keep data for subsequent routes
        pipelines: [logs/prod]
      - context: log
        condition: 'severity_number >= SEVERITY_NUMBER_ERROR'
        pipelines: [logs/errors]   # default action: move
service:
  pipelines:
    logs/in:      { receivers: [otlp],       exporters: [routing] }
    logs/prod:    { receivers: [routing],    exporters: [file/prod] }
    logs/errors:  { receivers: [routing],    exporters: [file/errors] }
    logs/other:   { receivers: [routing],    exporters: [file/other] }
```

Key model points of `routingconnector`:

- `table.context` (`resource` | `span` | `metric` | `datapoint` | `log` |
  **request**) chooses OTTL evaluation scope.
- `action: move` (default) removes matched data from subsequent route
  evaluation — like a switch/case break.
- `action: copy` keeps matched data available for later routes — like
  switch/case fallthrough.
- `default_pipelines`: catch-all for unmatched records.
- `error_mode`: `propagate | ignore | silent` — when `ignore`/`silent` and a
  route's OTTL errors out, the record falls through to default pipelines.

### OTTL (OpenTelemetry Transformation Language)

OTTL is Collector's CESQL-analogue. The `filter` processor uses OTTL:

```yaml
processors:
  filter:
    error_mode: ignore
    logs:
      log_record:
        - 'IsMatch(body, ".*password.*")'
        - 'severity_number < SEVERITY_NUMBER_WARN'
    metrics:
      metric:
        - 'type == METRIC_DATA_TYPE_HISTOGRAM'
```

OTTL supports `attributes[...]`, `resource.attributes[...]`, `IsMatch`,
`IsMatch(scope.name, ...)`, severity comparators, type enums, and function
libraries — much richer than CESQL (CESQL only addresses headers; OTTL
addresses body/context).

The deprecated `match_once` removal (v0.120) forced users to express routing
either as explicit enumerations of condition combinations or as a **layered
approach**: one router separates "matched any" from "matched none", second
layer applies nondeterministic combinations. This is a practical frame for
thinking about riverpod's multi-fan-out edge semantics.

### Distinctive ideas

- The **Connector construct** is the cleanest formulation of "an edge in the DAG
  is also a routable node" — riverpod could model per-edge "router sinks" as
  native first-class Nodes that are simultaneously a Sink of one edge and a
  Source of many.
- `action: move | copy` is a tiny primitive that solves what would otherwise
  require complex DAG patterns — "route to high-priority and to archive"
  needs `copy` for archive; default `move` for first match + remaining
  fallthrough. riverpod would otherwise encode this with router node +
  fan-out node + branch — three nodes for one concept.

### What riverpod could borrow / is missing

- Treat the **"routing edge" as a first-class Node type** that is both Sink
  (for upstream pipeline) and Source (for multiple downstream pipelines),
  exactly mirroring OTel's Connector — this collapses common DAG patterns
  from 3 nodes to 1 and matches users' mental model.
- Borrow the **`action: move | copy`** primitive at the routing edge: `copy`
  = fan-out with downstream of also matching; `move` = switch-case routing.
  With a `default_pipelines` analog as the unmatched catch-all — concise
  enough to express complex fan-out + branch in a single edge config.
- Borrow the **Extension model** (`service.extensions` +
  `auth: { authenticator: ... }`) for cross-cutting concerns like auth, mTLS,
  health — keeping them out of the per-Source/per-Sink config.

---

## 9. Cloudera Envelope

[cloudera-labs/envelope](https://github.com/cloudera-labs/envelope) — configuration-driven ETL pipeline framework on Apache Spark (Java, HOCON config, batch + Spark Streaming micro-batch). Last release v0.7.2 (Dec 2019), ~160 stars, effectively archived but the **configuration design is distinctive and directly relevant to riverpod**.

### 9.1 Configuration format: HOCON

Envelope uses [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) (Human-Optimized Config Object Notation) — Typesafe Config's format. Properties of HOCON that matter for riverpod:

- **Superset of JSON** — any valid JSON is valid HOCON.
- **Less indentation-sensitive than YAML** — uses `=` for assignment, `{}` for blocks, supports flat dotted paths (`application.executor.memory = 4G` is equivalent to nested `application { executor { memory = 4G } }`).
- **Native substitutions** — `foo = ${bar}` references another config key; useful for DRY configs.
- **Environment variable overrides built-in** — Typesafe Config automatically layers `ENV_VAR` overrides on top of the file config (e.g. `KAFKA_BROKERS` overrides `kafka.brokers`). No need for riverpod's `--set-env` CLI flag.
- **Config layering** — system env > primary file > CLI args, composited and resolved in one pass.
- **Comments** — `#` and `//` both supported (YAML only `#`).
- **No significant whitespace** — blocks delimited by `{}`, not indentation. Avoids YAML's tab-vs-space and "indentation error that looks valid" class of bugs.

Tradeoff: HOCON is Java-ecosystem; Go HOCON parsers exist (e.g. `github.com/gurkankaymak/hocon`) but are less mature than Typesafe Config. riverpod could adopt HOCON syntax or cherry-pick its ergonomic features (substitutions, env overlay, flat dotted paths) into a YAML-based scheme.

### 9.2 Step-centric DAG with `dependencies` list

The core config structure is:

```hocon
steps {
  fix {
    input { type = kafka  brokers = "..."  topics = [fix] }
  }
  messagetypes {
    input { type = kudu  connection = "..."  table.name = "..."  hint.small = true }
  }
  newordersingle {
    dependencies = [fix, messagetypes]
    deriver  { type = sql  query.literal = "SELECT ... FROM fix JOIN messagetypes ..." }
    planner  { type = upsert }
    output   { type = kudu  connection = "..."  table.name = "..." }
  }
  largeorderalert {
    dependencies = [newordersingle]
    deriver  { type = sql  query.literal = "SELECT ... WHERE orderqty = 9999" }
    planner  { type = append }
    output   { type = kafka  brokers = "..."  topic = largeorders }
  }
}
```

Each step is a **named block** containing optional `input` / `deriver` / `planner` / `partitioner` / `output` sub-blocks. Edges are expressed as `dependencies = [a, b]` **inside the child step**, not as a separate `edges:` section — the same pattern Vector uses (`inputs = [...]`).

> **riverpod v2.0（2025-06）：** 已采纳 Envelope/Vector 的内联边思路，用 `depends_on`（序列或映射）替代独立 `edges:` 段；per-edge `buffer`/`delivery`/`route` 写在 `depends_on` 对象值内。详见 `riverpod-design.md` §8.1.3。**Planner 不采纳**；写入语义由各 Sink `config` 自管。

### 9.3 The Planner concept — decoupling write semantics from output transport

This is Envelope's most distinctive idea and riverpod's biggest structural omission for ETL use cases. A **Planner** sits between the deriver (transform) and the output (sink) and decides *how* to apply arriving records to the target:

| Planner | Semantics |
|---|---|
| `append` | Insert-only (Kafka, file) |
| `upsert` | Insert-or-update by key (Kudu, HBase, JDBC) |
| `overwrite` | Full overwrite |
| `delete` | Delete by key |
| `history` | **Type 2 SCD** — track effective-from/to ranges + current flag, event-time ordered |
| `bitemporal` | **Bi-temporal** — both event-time and system-time validity ranges |
| `eventtimeupsert` | Upsert keyed on event time + natural key |

Example (Type 2 SCD, no code):

```hocon
planner {
  type = history
  fields.key = [clordid]
  fields.timestamp = [transacttime]
  fields.values = [symbol, orderqty, leavesqty, cumqty]
  fields.effective.from = [startdate]
  fields.effective.to = [enddate]
  field.current.flag = currentflag
  carry.forward.when.null = true
  time.model.event.type = longmillis
  time.model.last.updated.type = stringdatetime
}
```

riverpod's current design has **no equivalent** — Sink handles both transport (Kafka/HTTP/Kudu) *and* write semantics (append vs upsert) in one plugin. This means:
- Implementing SCD Type 2 / bi-temporal in riverpod requires a custom Transform that does lookup-against-target + mutation planning inline — heavy, error-prone, and duplicated per sink type.
- The same Kafka sink can't transparently become "upsert to Kudu" vs "append to Kafka" by swapping a planner.

**Recommendation:** riverpod should introduce a **Planner layer** between Transform and Sink (or as a Sink-decorator). P0 planners: `append`, `upsert`, `drop`. P1: `history` (SCD2), `merge`. P2: `bitemporal`. This is the single highest-value architectural idea to steal from Envelope for any ETL-oriented user.

> **riverpod v2.0 决策：不采纳 Planner。** 写入语义由各 Sink 插件 `config` 自行定义；复杂 ETL（SCD2 等）在专用 Sink 或 Transform 内实现。见 `riverpod-design.md` §2.2、§4.5。

### 9.4 The Translator concept — decoupling byte parsing from input transport

Symmetric to Planner on the input side. A **Translator** sits inside an input step and converts raw bytes → structured rows:

| Translator | Purpose |
|---|---|
| `avro` | Avro decode with schema |
| `delimited` | CSV / pipe-delimited |
| `kvp` | Key-value pair (e.g. FIX `tag=value\u0001tag=value`) |
| `protobuf` | Protobuf decode |
| `morphline` | Morphline pipeline (Cloudera's Kite SDK) |
| `raw` | No-op passthrough |

```hocon
input {
  type = kafka
  brokers = "..."  topics = [fix]
  translator {
    type = kvp
    delimiter.kvp = "\u0001"
    delimiter.field = "="
    schema { type = flat  field.names = [...]  field.types = [...] }
  }
}
```

riverpod's current Source conflates transport + parsing — a Kafka source must "know" whether payloads are JSON/Avro/CSV. The Translator split lets the **same `kafka` source plugin** produce parsed records by pairing it with any translator. This reduces the connector matrix: instead of `kafka_json`, `kafka_avro`, `http_json`, `http_csv` (combinatorial explosion), you have `kafka` + `http` transports × `avro` + `json` + `csv` + `protobuf` translators.

**Recommendation:** riverpod should add an optional `translator` sub-block on Source (or a `decode` Transform that's sugar for the same). P0 translators: `json`, `raw`. P1: `avro`, `protobuf`, `csv`.

> **riverpod v2.0 决策：已采纳为 Codec 体系** — Source `decoder` / Sink `encoder` + 顶层 `codecs:` 共享配置。见 `riverpod-design.md` §5。

### 9.5 Control-flow steps: Loop, Decision, Task, Repetition

Envelope has four step *types* beyond `data`:

- **`loop`** — iterate a sub-graph over `range` / `list` / `step`-derived values, with `${param}` substitution into dependent steps' config. Parallel or serial. Example: run the same SQL+export for each region in a list.
- **`decision`** — runtime conditional sub-graph selection. `method: literal|step_by_key|step_by_value`, `if-true-steps: [a, b]`. The complement set of dependent steps runs if false. This is **dynamic topology pruning**, not per-message routing — the DAG shape itself changes at runtime based on data.
- **`task`** — side-effect-only step (send notification, update metastore, trigger external job), no DataFrame. Decouples "do something off-pipeline" from data flow.
- **`repetition`** (streaming) — re-run cached steps on a schedule/criteria within a long-running streaming job, refreshing reference data periodically.

riverpod has none of these. The most transferable are **decision** (runtime sub-graph selection — useful for "only run enrichment on prod events") and **task** (lifecycle side-effects — useful for "on pipeline start, warm a cache; on shutdown, flush a manifest").

### 9.6 Other notable config features

- **`udfs`** top-level array — register custom Spark SQL functions by class name/alias, extending the deriver language without writing a plugin jar. riverpod could let users register eql functions via config.
- **DQ deriver** (`type = dq`) — declarative data quality rules as config: `checknulls`, `enum`, `range`, `regex`, `count`, `checkschema`. Scope = `dataset|row`. This is a config-level alternative to writing filter DSL for common validation.
- **Per-step tuning** — `cache.enabled`, `cache.storage.level`, `hint.small` (broadcast join), `repartition.partitions`, `coalesce.partitions`, `print.schema.enabled` (debug). Spark-specific but the pattern of per-step operational knobs is good.
- **`parameter.*` passthrough** — `kafka.parameter.*` and `hbase.conf.*` strip the prefix and forward to the underlying client config. Avoids Envelope having to mirror every Kafka/HBase knob. riverpod should do the same rather than enumerating every Kafka consumer option.
- **`spark.conf.*`** — same pattern for engine-level config.

### 9.7 Envelope weaknesses (don't over-adopt)

- **Spark-only, JVM-only** — heavyweight; not suitable for riverpod's lightweight Go single-binary goal.
- **Micro-batch, not true streaming** — Spark Streaming's micro-batch model is fundamentally different from riverpod's per-event channel model; Planner/Translator ideas transfer but the execution model doesn't.
- **No per-edge conditions** — `dependencies` are unconditional; routing is done in SQL `WHERE` clauses, not on edges. riverpod's per-edge condition is more expressive.
- **No per-edge buffers / backpressure config** — Spark handles internally; riverpod's per-edge buffer is a real advantage.
- **No custom DSL** — relies on Spark SQL (powerful but JVM-coupled). riverpod's eql/CESQL is more portable.
- **No at-least-once ack chain** — relies on Kafka offset management + Spark checkpointing. riverpod's refCount ack is more general.
- **Project effectively dead** (last release 2019) — ideas are evergreen but the codebase is not a live reference.
- **HOCON Go ecosystem immature** — if riverpod adopts HOCON syntax, the Go parser is a risk.

### 9.8 What riverpod should steal from Envelope (priority order)

1. **Planner layer** (§9.3) — decouple write semantics (append/upsert/SCD2/bitemporal) from Sink transport. **Highest-value idea for ETL users.**
2. **Translator layer** (§9.4) — decouple byte parsing (json/avro/csv/protobuf) from Source transport. Reduces connector matrix.
3. **`dependencies = [...]` as sugar for unconditional edges** (§9.2) — keep explicit `edges:` for per-edge condition/buffer, but allow `depends_on: [a, b]` on a stage as shorthand. Best of both Envelope's friendliness and riverpod's expressiveness.
4. **HOCON ergonomics or HOCON syntax** (§9.1) — at minimum, support native env-var overlay + `${substitution}` in YAML; ideally offer HOCON as an alternative config format.
5. **Decision steps** (§9.5) — runtime sub-graph pruning based on data (`if-true-steps`). Complements per-edge conditions (which are per-message).
6. **Task steps** (§9.5) — lifecycle side-effects decoupled from data flow.
7. **DQ rules as config** (§9.6) — declarative `null/enum/range/regex/schema` validation without DSL.
8. **`parameter.*` passthrough** (§9.6) — forward unknown config keys to underlying clients (Kafka/HTTP) instead of enumerating every option in riverpod's schema.
9. **UDF registration via config** (§9.6) — extend eql with user-registered functions without writing a Go plugin.
10. **Per-step operational knobs** (§9.6) — `cache`, `hint`, `repartition` analogs (e.g. per-stage `prefetch`, `parallelism hint`).

---



| Concern | Knative | Argo | KC | Fluentd | Flume | Benthos | Vector | OTel | CESQL | Envelope |
|---|---|---|---|---|---|---|---|---|---|---|
| Predicate DSL | CESQL / attrs | CEL / expr / script | (no DSL) | match pattern | selectors | Bloblang | VRL | OTTL | **CESQL** | Spark SQL + UDFs |
| Transform chain | JSONata | (?) | **SMT + predicates** | filter pipeline | interceptors | mapping/processors | transforms | processors | n/a | derivers (sql/dq/nest/…) |
| Buffer / backpressure | Channel | EventBus stream | DLQ + `errors.*` | **chunk keys** | Channel txns | retry policies | retry/batch | sending_queue | — | (Spark internal) |
| Delivery / DLQ | **DeliverySpec + DLS** | (?) | `errors.deadletterqueue.*` | `@ERROR` label | retry + sink processors | `fallback` output | `retry_attempts` | retry queue | — | (planner + output) |
| Routing topology | Broker→Trigger | Sensor deps | topics | tag patterns | replicating/multiplex | `output.switch` | `route` transform | **connector** | — | **`dependencies` list** |
| Write semantics | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **Planner (SCD2/bitemporal)** |
| Input decoding | n/a | n/a | SMT | n/a | interceptors | n/a | codecs | n/a | n/a | **Translator (avro/csv/kvp)** |
| Event type registry | **EventType CRD** | dependency names | schema registry | n/a | n/a | (?) | n/a | n/a | — | (udfs registry) |
| Compound events | (?) | **dep conditions + conditionsReset** | n/a | n/a | n/a | (windowing) | (windowing) | n/a | — | **decision steps** |
| Exactly-once | (?) | n/a | **KIP-618** (?) | n/a | channel transaction | (?) | (?) | (?) | — | (?) |
| Plugin sandbox | n/a | (?) | jar classpath | (?) | plugins.d | **WASM** | **WASM** | components | — | jar classpath |
| Config format | YAML/CRD | YAML/CRD | JSON/props | Ruby DSL | props | YAML | TOML/YAML | YAML | n/a | **HOCON** |
| Control flow | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **loop/decision/task/repetition** |

(?) in the table = known feature but not central / not researched deeply here.

## Top concrete things riverpod should steal, in priority order

1. **CESQL as the standard predicate language** (project 7) — adopt the
   official spec + run `cesql_tck`; reserve riverpod-specific extensions as a
   CESQL-namespace. riverpod's `eql` should *be* CESQL-compatible, not a
   competitor.
2. **Envelope's Planner layer** (project 9) — decouple write semantics
   (append/upsert/SCD2/bitemporal) from Sink transport. Highest-value idea
   for ETL users; riverpod currently conflates the two in Sink plugins.
3. **Envelope's Translator layer** (project 9) — decouple byte parsing
   (json/avro/csv/protobuf) from Source transport. Reduces the connector
   matrix from N×M to N+M.
4. **OTel-Collector Connector** as a first-class DAG node (project 8) —
   collapse "router + fan-out" into one Node with `action: move|copy` and
   `default_pipelines`.
5. **`dependencies = [...]` as sugar for unconditional edges** (projects 9 + Vector) —
   **已采纳：** riverpod 用 `depends_on`（序列/映射）为唯一边语法，per-edge 属性内联在对象值内；独立 `edges:` 已 deprecated。见 `riverpod-design.md` §8.1.3。
6. **HOCON ergonomics** (project 9) — native env-var overlay + `${substitution}`
   in config; ideally offer HOCON as an alternative format to YAML.
   **已采纳：** YAML + HOCON 双格式。见 `riverpod-design.md` §8.2。
7. **Argo's `conditions` + `conditionsReset` over dependencies** (project 2)
   — a "join gate" at DAG joins with `&&` / `||` over upstream Sources and
   time-based reset so stale half-matches don't fire.
8. **Kafka Connect's predicate-on-transform** (project 3) — every Transform
   step carries an optional predicate that decides whether the step applies to
   a given record. Lets users write "mask SSN if env=prod" without branching
   the DAG.
9. **Kafka Connect DLQ semantics** (project 3) — `errors.tolerance: all|none` +
   `errors.deadletterqueue.topic` + `errors.deadletterqueue.context.headers.enable`
   as a per-Sink block; convention over ad-hoc config.
10. **Knative DeliverySpec** (project 1) — `retry`, `retryAfterMax`, `timeout`,
    `deadLetterSink` as a separate per-edge block, orthogonal to business routing.
11. **Envelope Decision + Task steps** (project 9) — runtime sub-graph pruning
    based on data (decision), and lifecycle side-effects decoupled from data
    flow (task).
12. **Envelope DQ rules as config** (project 9) — declarative `null/enum/range/
    regex/schema` validation without writing DSL.
13. **Knative EventType registry** (project 1) — a CRD catalog of event types
    that Sinks reference by name; enables discovery and prevents typos in DSL
    predicates.
14. **Fluentd chunk keys** (project 4) — per-edge buffers should accept a
    `key: [kind, tenant]` partition key for ordering + bounded back-pressure
    per partition, not just a single queue.
15. **Flume required vs optional sinks** (project 5) — per-edge declaration
    allowing some fan-out edges to be best-effort (failure ignored) while
    others are required (rolls the batch back).
16. **WASM transforms** (project 6, Benthos + Vector) — sandboxed, hot-loadable,
    language-agnostic, deployed without rebuilding operator image — the strongest
    evidence-backed extension point for user logic beyond CESQL/DSL.
17. **Envelope `parameter.*` passthrough** (project 9) — forward unknown config
    keys to underlying clients (Kafka/HTTP) instead of enumerating every option
    in riverpod's own schema.
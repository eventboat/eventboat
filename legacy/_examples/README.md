# Riverpod 示例配置

本目录收录常见 Pipeline 模式的演示配置，可直接用于学习、`riverpod validate` 校验或作为项目模板。

> 带 `cron` + `drop` 的示例无需外部依赖即可本地校验与试运行；涉及 Kafka / HTTP 的示例需配置对应环境变量或服务。

## 快速开始

```bash
# 校验单个示例
go run ./cmd/riverpod validate --config _examples/01-linear-etl.yaml

# 校验全部 YAML 示例
go run ./cmd/riverpod validate --config-dir _examples

# 试运行（cron 定时产生消息，drop 吞没输出）
go run ./cmd/riverpod run --config _examples/01-linear-etl.yaml
```

## 示例索引

| 文件 | 模式 | 说明 |
|------|------|------|
| [01-linear-etl.yaml](01-linear-etl.yaml) | 线性 ETL | Source → map → filter → sink，最基础的 DAG 链 |
| [02-route-branching.yaml](02-route-branching.yaml) | 条件分支 | `route` transform + 下游 `depends_on.route` 多路分流 |
| [03-fan-in.yaml](03-fan-in.yaml) | Fan-in 合并 | 多个 Source 汇入同一 Transform |
| [04-http-webhook.yaml](04-http-webhook.yaml) | HTTP Webhook | `http_server` 接收请求并转发至 HTTP 端点 |
| [05-transform-sink-combined.yaml](05-transform-sink-combined.yaml) | Transform+Sink 合体 | 单 step 内嵌 transform 与 sink，自动展开内边 |
| [06-edge-delivery.yaml](06-edge-delivery.yaml) | 边级投递策略 | per-edge buffer、retry、DLQ、`required: false` |
| [07-flat-pipeline.yaml](07-flat-pipeline.yaml) | 平坦 pipeline 写法 | 与 `steps` 等价的 `pipeline[]` 声明风格 |
| [08-hocon-linear.conf](08-hocon-linear.conf) | HOCON 格式 | 与 01 等价的 Envelope 风格本地配置 |
| [multi-pipeline/](multi-pipeline/) | 多 Pipeline | 同一进程加载多个独立 Pipeline |

## 模式说明

### 线性 ETL

```
cron-source → enrich (map) → filter-high (filter) → drop-sink
```

适用于单一路径的数据清洗、字段映射与过滤。

### 条件分支（Route）

```
kafka-in → tag-region (map) → splitter (route) ─┬─ us-sink
                                                 ├─ eu-sink
                                                 └─ default-sink
```

`route` transform 将匹配的路由名写入 `metadata["er-route"]`；下游 step 在 `depends_on` 中通过 `route: <name>` 声明入边条件。

### Fan-in

```
orders-source ──┐
                ├─ merge (map) → drop-sink
events-source ──┘
```

多个上游 step 的 `depends_on` 序列 `[a, b]` 表示 fan-in：Transform 会等待所有入边消息（引擎 fan-in 语义）。

### Transform + Sink 合体

```yaml
publish:
  depends_on: [enrich]
  transform: { type: map, ... }
  sink: { type: drop }
```

展开为 `publish`（transform）→ `publish-sink`（sink）两条 stage，无需手动声明内边。

## 相关文档

- [配置规范](../docs/configurations.md)
- [README 配置示例](../README_ZH.md#配置示例)
- [testdata/pipelines](../testdata/pipelines/) — CI 与单元测试用最小配置

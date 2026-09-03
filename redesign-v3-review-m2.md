# redesign-v3.md M2 范围独立设计审查报告（实现前关卡）

日期：2026-09-03
评审对象：`redesign-v3.md`（v1.10）的 M2 范围——§5.8 作业管道、§3.3 explain/replay、§3.4 MCP 操作面、§6.6 OTel，对照 M1 实现（879adfc）与 `redesign-v3-review.md`（M1 审查）的已记档决策。
评审方法：全文逐节细读 M2 相关章节 + 依赖假设逐项对照 pkg.go.dev / 官方仓库 / Go module proxy 实测 + 与 M1 代码逐文件交叉推演（engine/settle/store/registry/config/loader/testrun）。

## 总体结论

**通过，无阻塞项。** 可以进入实现。

- 依赖选型三项（MCP SDK / OTel / sql 驱动）全部有稳定结论，无需用户裁决即可开工（证据见第一节）；
- 开放问题 #10（部署级配置形态）给出明确建议并**按建议直接实施**（最小 Runtime 配置文件 + CLI 覆盖）——这是代码层可逆决策，非不可逆动作，按"文档未覆盖处按原则自行决策并记录"处理，记录于 R13；
- **2 项中等**（R1/R2：M1 的 Source 接口没有错误通道与取尽信号；作业取消会永久阻塞 checkpoint 前缀——两者都是 M2 必须补的引擎机制，不改设计结论）；
- **14 项低**（缺口类：逐条给出裁决，实现按裁决执行并记入 README）。

规范三处承重墙在 M2 范围内依然自洽：§5.8 作业语义与 §6.2 七条不变量无冲突（不变量 7 的作业版 = 水位跟随 settle，M1 已有机制直接复用）；§3.3 replay 三模式都能映射到 M1 store 已有查询 + InjectAt（一处引擎缺口 R4）；§3.4 工具表与 CLI §3.6 一致（一处命名分歧 R11，按任务书执行）。

---

## 一、依赖假设核查（对照 pkg.go.dev / 官方仓库 / Go proxy，2026-09-03）

| 假设 | 核查结果 | 判定 |
|---|---|---|
| 官方 Go SDK（`github.com/modelcontextprotocol/go-sdk`）成熟度 | **稳定**：v1.0.0 起为稳定线，当前 v1.7.0（支持 MCP spec 2026-07-28，Apache-2.0，与 Google 协作维护）。工具定义用类型化 struct + `jsonschema:` tag（`mcp.AddTool(server, &mcp.Tool{...}, handler)`），schema 自动生成；传输层 stdio（`mcp.StdioTransport`）与 Streamable HTTP（`mcp.NewStreamableHTTPHandler` 返回 `http.Handler`）齐备；客户端 `CommandTransport`/`StreamableClientTransport` 可用于自测。roots/sampling/logging 按新 spec 弃用中（≥12 个月窗口），与我们无关 | ✅ **选型：官方 SDK v1.7.0** |
| mark3labs/mcp-go 备选 | 仍为 v0.x（未达 v1 稳定线）；官方 SDK 已稳定，社区事实标准向官方迁移 | ❌ 不用 |
| OTel Go：OTLP + Prometheus 双出口 | 标准模式：一个 `MeterProvider` 挂两个 reader——`otlpmetrichttp`（push）+ `exporters/prometheus`（pull，`promhttp.Handler` 服务 `/metrics`，基于 client_golang）。实测版本：`go.opentelemetry.io/otel` v1.46.0、`otel/sdk` v1.46.0、`exporters/prometheus` v0.68.0、`otlpmetrichttp`/`otlptracehttp` v1.46.0 | ✅ |
| go-sql-driver/mysql | v1.10.1，纯 Go 无 CGO，事实标准 | ✅ |
| pgx vs lib/pq（postgres） | `lib/pq` README 自述 maintenance mode 并推荐 pgx；`jackc/pgx/v5` v5.10.0，纯 Go，`pgx/v5/stdlib` 提供 `database/sql` 驱动（二进制协议，快 10–20%） | ✅ **选型：pgx/v5 stdlib** |
| modernc.org/sqlite（已在依赖树） | M1 已用（v1.58.0）；sql 源增加 `sqlite` 方言服务于 examples/测试（见 R8），零新增依赖 | ✅ |

**结论：无依赖阻塞。** 所有候选均可从 Go proxy 下载（`go list -m -versions` 实测）。

---

## 二、问题清单

严重度：🔴 阻塞 / 🟡 中（实现必须处理，不改设计结论）/ 🔵 低（缺口补决策）。

### R1 🟡 M1 的 Source 接口没有错误通道与"取尽"信号，作业生命周期无法表达

- **证据**：`registry.Source.Run(ctx, emit)` 无返回值、无终止语义（`internal/registry/registry.go`）；引擎把源的 Close 后退出视为常态。而 §5.8 作业生命周期需要：源拉尽 → settling → 终态；源查询失败（DB 不可达）→ run failed（区别于 partial）。
- **裁决**：新增可选能力接口（类型断言，向后兼容，不改既有插件）：

  ```go
  type PullSource interface {
      Source
      // Pull emits rows until exhausted (nil return) or source failure.
      Pull(ctx context.Context, emit func(Message)) error
  }
  ```

  引擎发现源实现 PullSource 时走 `Pull`：返回 nil = 取尽（该源 goroutine 结束，引擎暴露 sources-done 状态）；返回 error = 经引擎 Options 回调上抛，作业 runner 判 run failed。连续源继续走 Run（语义不变）。`capabilities: [pull]` 的 verify 校验以注册表声明为准，与接口能力同源维护。

### R2 🟡 作业取消/失败遗留的未决消息会永久阻塞 checkpoint 连续前缀

- **证据**：settle tracker 的 checkpoint 只按 spool seq 连续前缀推进（`internal/engine/settle.go`）。`overlap: latest` 要取消在跑的 run；被取消 run 的未决消息若不终态化，seq 前缀永远卡住，后续 run 的 checkpoint 无法推进，spool 重放窗口无限增长。
- **裁决**：取消 = 有界等待（`limits.drain_timeout`）在途消息 settle；超时仍未决的消息**强制死信**（reason=`job canceled`，含完整原消息，可用 `replay --job` 回灌）。终态化而非丢弃——与不变量 4 精神一致（不可丢，只可降级可审计）。run failed（源错误）不取消在途消息：引擎继续运行至在途 settle 完毕再收尾（失败发生在翻页边界，天然安全）。

### R3 🟡 每次作业运行需要独立 IR 与引擎实例（参数注入 + 并发运行）

- **证据**：`${parameters.x}`（如 sql 源 `args`）在**触发时**才定值（§5.9 生命周期表），而 M1 的 IR/引擎在加载时构建一次。`overlap: all` 还要求两个 run 并存。
- **裁决**：作业管道保留原始配置；**每次 run：解析实参 → 替换 `${parameters.x}` → 重建该 run 的 IR → 独立 engine 实例**（同一 SQLite store，spool append 由 SQLite 串行化；各实例独立 admission 信号量——背压粒度从 per-pipeline 变 per-run，M2 记档可接受，单源作业无感）。run 归属通过 accept 时盖章 `meta.job_run_id` 实现（死信计数、replay --job 均按此过滤）。水位与 checkpoint 是 pipeline 级共享的（不变量 7 不受影响）。

### R4 🟡 `InjectAt` 对 sink 节点不成立，replay"任意 node 重注入"缺一角

- **证据**：`Engine.InjectAt` 在非 source 节点走 `fanOut`（`internal/engine/engine.go`）；sink 无出边 → 零匹配 → 按过滤 settle。死信重注入到 sink 是合法运维动作（§3.3 "重新注入任意 node"）。
- **裁决**：InjectAt 分支：目标是 sink → 直接入该 sink 的 channel（via=nil，按 sink 自身 delivery 走）；目标是 transform/source → 现行为。回归测试覆盖。

### R5 🔵 `Metrics.Settled` 语义过载（M1 挂账，OTel 迁移时清偿）

- **证据**：`persistCheckpoint` 把 checkpoint 指针存进名为 Settled 的计数器（`internal/engine/engine.go`）——它不是"已 settle 条数"。作业 run 的 delivered 计数不能用现有字段。
- **裁决**：M2 拆分：`CheckpointPtr`（指针）+ `SettledCount`（条数，新增计数器在 onSettled 回调累加）。OTel 指标用后者。

### R6 🔵 dead_letter 表缺 `job_run_id` 列；schema 迁移机制缺位

- **证据**：`replay --job <run-id>` 需按 run 过滤死信；M1 表无此列。且 M1 用 `CREATE TABLE IF NOT EXISTS`——对已存在的库不会加列。
- **裁决**：dead_letter 加 `job_run_id TEXT NOT NULL DEFAULT ''` + 索引；新增 `job_run` 表（见第三节）。实现 guarded 迁移（`PRAGMA table_info` 检列，缺则 `ALTER TABLE ADD COLUMN`），存量库兼容。

### R7 🔵 store `ReplayFrom` 全窗物化内存

- **证据**：SQLite 实现把所有行收进 slice 再回调（避免单连接死锁，`internal/store/sqlite.go` 注释自认）。大窗口回放/恢复会内存膨胀。
- **裁决**：新增分页接口 `ReplayPage(pipeline, afterSeq, limit, fn) (lastSeq, more, err)`（LIMIT 窗口迭代，回调前释放连接）；引擎崩溃恢复与 `replay --spool` 都改走它。M1 旧接口保留（mem store 语义不变），SQLite 旧接口按页组合实现。

### R8 🟡（sql 源）命名参数改写与 keyset 包装 SQL 的方言细节

- **证据**：§5.8 示例查询用 `:from`/`:to` 命名绑定；MySQL/PG 原生占位符分别是 `?` 与 `$n`；PG 的 `::` 类型转换与字符串字面量里的冒号不能误判。keyset 分页需要在用户查询外包一层 `(k1,k2) > (...)` 行比较 + ORDER BY + LIMIT（MySQL/PG 均支持行构造器比较；派生表内含 ORDER BY 两方言均合法）。
- **裁决**：
  1. 实现小型扫描器：逐字符跳过 `'…'`/`"…'`/反引号字面量与 PG `::`，把 `:name` 改写为方言占位符（mysql/sqlite→`?`，postgres→`$n`），按 args 声明顺序绑定；改写器独立单测（含冒号陷阱用例）。
  2. 分页包装：`SELECT * FROM (<query>) AS _eb_page WHERE (k1,...) > (?,...) ORDER BY k1,... LIMIT ?`；要求 key 列出现在用户查询的选择列表（verify 提示，运行期查不到列报 source error）。`cursor.column` 是水位列（供 `from: cursor` 绑定），`pagination.key` 是 keyset 键，二者可不同。
  3. `driver: mysql | postgres | sqlite`——sqlite 方言（modernc 驱动已在依赖树）服务于 examples 与 CI 可跑的作业样例（任务书提供的两条路线中选"sqlite 对拍"路线，另配 testkit fake pull 源做引擎级确定性测试）。规范写 mysql/postgres（P1）；sqlite 是加法扩展，记档。

### R9 🔵 引擎绑定值 `cursor`/`now` 的解析边界

- **证据**：§5.8 `from: {default: cursor}`；无水位时（首次运行）cursor 无值；多 pull 源时水位是 per-source 的，而 parameters 是 per-run 的。
- **裁决**：`cursor` 在**每个源各自**解析：该源 args 里的 `${parameters.from}` 值为 cursor 时，用该源自身的持久水位替换（R3 的 per-run 替换阶段按源进行）；无水位时解析为空串 `""`（配合 `>= :from` 的文本比较即"从头开始"，文档写明；更严格的首跑边界由用户显式传参）。`now` 解析为 run 起跑时刻 UTC RFC3339。多源作业合法但文档建议单源（水位归因清晰）。

### R10 🔵 explain 脚本处理裁决：**带样例消息时 dry-run，无样例时仅符号摘要**

- **证据**：任务书要求裁决。反方（只摘要）：静态推演不执行代码。正方（dry-run）：Starlark 沙箱无时钟/无熵/无 I/O、步数预算硬上限（§4.3），对同一条输入求值是纯函数——这正是 §4.3 确定性小节"test/simulate/replay/生产逐字节一致"的承诺；explain 是 simulate 的产品名（§3.3）。不 dry-run 的 explain 在"transform 后消息走哪条边"这一核心问题上只能给假答案（下游 CEL 要对 transform **后**的 payload 求值）。
- **裁决**：`--message` 给定时：脚本在样例上真实执行（预算内）；失败 = 输出真实 backtrace 并停在该节点（这正是生产会发生的，explain 的价值）。`--message` 省略：不执行，输出符号化摘要（各边条件文本 + 字段依赖、脚本语句数/预算）。消息盖章用固定占位（`meta.message_id="explain"`、固定 ingest_time），保证输出可 diff。

### R11 🔵 MCP 工具命名分歧：spec `dlq_query`/`dlq_replay` vs 任务书 `dead_letter_query`/`dead_letter_replay`

- **证据**：§3.4 表用 `dlq_query`/`dlq_replay`（v1.6 全称原则回退保留 dlq）；M2 任务书用全称。
- **裁决**：**按任务书 `dead_letter_query`/`dead_letter_replay`**（任务书是最新操作指令，且对 Agent 语义更透明），CLI 保持 `replay --dlq`（spec §3.6）。分歧记入 README 决策。

### R12 🔵 `tail` 的"脱敏规则适用"无规则语言可依

- **证据**：§3.4 tail 一句"脱敏规则适用"；全文没有脱敏规则的配置形态。
- **裁决**：M2 tail 抽样最近 N 条（per-node 有界 ring buffer），不做脱敏（截断超长 payload + 去重）；脱敏规则列 P2。记档。

### R13 🔵（裁决建议，随实现落地）开放问题 #10：部署级配置形态

- **建议**：**最小 Runtime 配置文件 + CLI 覆盖**，而非继续堆 CLI 标志。理由：M2 新增 knobs（OTLP endpoint、采样率、admin 监听、MCP 开关）已超过 3 个心智单位；spec §5.10 明文"全局端点与导出器在部署级配置文件"（文件形态是规范本意，不是新发明）；CI/测试仍可用全标志形态（标志覆盖文件）。形态（kind: Runtime，~10 个字段，严格白名单校验，未知字段=错误）：

  ```yaml
  apiVersion: eventboat/v3
  kind: Runtime
  storage:   { data_dir: data, ephemeral: false }
  admin:     { listen: "127.0.0.1:7788", enable: true }   # REST+SSE+UI+/metrics
  mcp:       { enable: true }                              # /mcp Streamable HTTP + `eventboat mcp` stdio
  telemetry:
    otlp_endpoint: ""      # 空 = 不导出
    sample_ratio: 0.1
    prometheus: true       # admin /metrics exposition
  ```

  查找顺序：`--runtime <file>` 显式 > `./eventboat.yaml` > 全默认值；M1 的 `--data-dir/--ephemeral` 保留为覆盖标志。

### R14 🔵 作业终态判定矩阵与 hooks 触发

- **裁决**：
  - `success`：源取尽 + 在途 settle 完毕 + 该 run 死信数 0；
  - `partial`：同上但死信数 > 0（run 完成了，有死信）；
  - `failed`：源错误（Pull 返回 error）或引擎致命错误（error 文本入历史）；
  - `canceled`：被 overlap:latest 或运维取消（R2 语义）。
  - hooks：`success` → 仅 success；`failure` → failed **与 partial**（partial 需要人关注死信）。hook = 内联 sink（插件名即键，复用 sink 工厂 + schema 校验），投递 run 摘要 JSON（run_id/status/计数/error），尽力投递（3 次重试），不进 DAG/settle（通知不是数据）。

### R15 🔵 顶层段裁剪面更新

- M2 实现：`run`/`parameters`/`hooks`/`limits`（§5.10）；`telemetry`/`dlq`/`codecs` 仍裁剪（loader 的 hint 文案相应更新：telemetry 全局端点走 Runtime 配置，管道级 telemetry 段列 P2；dlq retention 列 P2；codecs 列 P2）。记档。

### R16 🔵 OTel span 策略：不逐消息开 span

- **证据**：§6.6 span 结构 `pipeline → source → node → edge → sink`；逐消息 span 在万级 msg/s 下不成立（trace 导出开销 ≫ 路由开销），且 M2 源不带 traceparent 上文。
- **裁决**：span = pipeline 生命周期、job run 生命周期（含 catchup/overlap 决策事件）、sink 批次写（含重试/失败事件）、deploy/replay/trigger 等操作面动作；**错误路径始终进 span 事件**：Starlark backtrace（行号）、CEL 求值错误（表达式文本+位置）挂在所属 node/job span 上（同时进死信记录，spec 双写要求）。逐消息采样（管道 telemetry 段 span 采样）列 P2。§6.6 的 span 链描述按"批粒度近似"实现，记档。

### R17 🔵 背压粒度注记

- `overlap: all` 下两个 run 并存时 spool 高水位信号量是 per-engine（R3），不是全局 pipeline 级——单 run 场景无差异；M2 记档，全局配额是 P2（§6.4 本来就列部署级）。

---

## 三、§5.8 作业管道语义核查（含作业历史表自洽性）

逐项对照结论（问题已并入 R1–R3、R9、R14）：

1. **调度**：复用 `robfig/cron/v3`（M1 cron 源已在用，标准 5 字段）。`schedule` 缺省 = 仅手动/触发器。
2. **catchup_window**（开放问题 #9 落地）：重启时扫描历史，对每个错过的 tick：`now - tick <= catchup_window` 且该 tick 无成功 run → **补跑一次**（以该 tick 为 `scheduled_for`，参数取声明默认值）；窗口外的错过 tick 计 `eventboat_jobs_catchup_skipped_total` 并跳过（不告警轰炸：一次启动最多一条补跑 + N 条 skip 计数）。多个错过窗口 → 只补最近一个（spec §9.9 建议"最多一次"）。窗口内作业仍 running 时的行为 = overlap 策略管（skip 情形跳过该 tick，不算 catchup 违约）。
3. **skip_if_successful**：以 `scheduled_for`（tick 身份）为准——同 tick 已有 success run 则跳过。这同时给了 catchup 幂等性（重启不会重复补跑）。
4. **overlap 三态**：skip（默认，计指标）/ all（并发 run，R3）/ latest（取消旧 run（R2 语义）再起新）。
5. **水位跟随 settle（不变量 7 作业版）**：M1 的 srcTracker 连续前缀 + `Settled(frontier)` 回调 + 源持久水位（source_state）机制**原样复用**——sql 源的 Settled 返回 `{watermark: max settled cursor}`。kill -9 后：spool 重放（不变量 3）覆盖未决集，源从持久水位续拉（不变量 7），两者不重叠（水位 = 已 settle 前缀）。专项测试：作业中断 → 重启 → 续传（新增测试，七条既有不变量测试不动）。
6. **背压互动**：pull 源 emit 走 `accept` 的 admission 信号量——spool 高水位自动暂停翻页，**零新机制**。
7. **作业历史表**（schema 自洽性核查通过——计数与引擎 per-run Metrics 同源、死信按 meta.job_run_id 对得上、retention 按 ended_at 清理不碰在跑 run）：

  ```sql
  CREATE TABLE IF NOT EXISTS job_run (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT    NOT NULL UNIQUE,
    pipeline      TEXT    NOT NULL,
    status        TEXT    NOT NULL,             -- pending|running|settling|success|partial|failed|canceled
    trigger_type  TEXT    NOT NULL,             -- schedule|manual|catchup
    parameters    TEXT    NOT NULL DEFAULT '{}',-- 本次运行实参（JSON，回补可审计）
    scheduled_for TEXT,                         -- tick 时刻（手动为 NULL）
    started_at    TEXT, ended_at TEXT,
    rows_read     INTEGER NOT NULL DEFAULT 0,
    delivered     INTEGER NOT NULL DEFAULT 0,
    dead_lettered INTEGER NOT NULL DEFAULT 0,
    error         TEXT    NOT NULL DEFAULT '',
    updated_at    TEXT    NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_job_run_pipeline ON job_run(pipeline, id);
  CREATE INDEX IF NOT EXISTS idx_job_run_sched ON job_run(pipeline, scheduled_for);
  ```

8. **parameters 校验**（§3.1 第 4 项）：type ∈ {string,integer,number,boolean}；default/required/enum/pattern/min/max 自洽（default 违反 enum/pattern/min/max = verify 错误）；`${parameters.x}` 引用未声明名 = verify 错误；连续管道（无 run 块）出现 `parameters` 段、`${parameters.x}`、脚本/谓词引用 `parameters.` = verify 错误（code: `job_parameters_in_continuous`）。`eval` 类运行时求默认值不吸收（§7.5）。
9. **hooks 校验**：键 ∈ {failure, success}；值 = 单插件键内联 sink，按 sink schema 严格校验。
10. **limits 接线**（M1 挂账）：`max_in_flight` → engine HighWatermark；`drain_timeout` → drain 超时（替换硬编码 10s）；`workers` 总额裁剪记档（R15 同类）。
11. **kill -9 专项测试形态**：沿用 M1 不变量 3 的"abandon engine + reopen store"模式（store 视角忠实：取消后引擎静默、无进一步写）；另加 sql/sqlite 子进程级 kill 测试列为 CI 可选 stretch（不阻塞主线）。

## 四、§3.3 explain / replay 核查

- **explain**：静态 IR 推演（节点/边清单、每边条件文本与编译产物）、`--message` 时消息级逐边 MATCH/no-match（CEL 真求值）+ 脚本 dry-run（R10 裁决）+ settle 路径与 delivery 摘要（静态）；`--topology` 输出 mermaid + ASCII（与配置边一一对应，§5.3 承诺）。
- **replay 三模式 ↔ store 能力映射**：

| 模式 | store 查询 | 重注入 | 缺口 |
|---|---|---|---|
| `--dlq [--since] [--where] [--dry-run]` | `DeadLetters`（SQLite 加 created_at 过滤） | `InjectAt(node)`（R4 修 sink 注入），meta 盖 `is_replay=true` | `--where` 用 CEL 对 {payload, meta} 求值（进程内过滤） |
| `--spool --from <spool-seq>` | `ReplayPage`（R7 分页） | 同上，按原 entry node 重放 | 无 |
| `--job <run-id>` | dead_letter WHERE job_run_id=?（R6） | 同 --dlq | 无 |

- replay 运行形态：起真实引擎（真 sink，回灌是真实投递）→ 注入 → WaitSettled → 停止；`--dry-run` 不起引擎，用 explain 的推演器输出每条死信的路径预测。

## 五、§3.4 MCP / Admin / SSE / UI 核查

- SDK 选型定案（第一节）；tools 全集 15 个：`catalog`、`verify`、`test`、`explain`、`deploy`、`status`、`jobs`、`trigger`、`tail`、`dead_letter_query`、`dead_letter_replay`、`drain`、`pause`、`resume`（R11 命名）。
- 架构：`internal/ops` 服务层（纯 Go，无传输耦合）承载全部工具逻辑；MCP server（SDK tools）与 Admin REST（net/http JSON）都是它的薄壳——**REST/MCP/JSON 三方同形状**（同一 ops 方法、同一 JSON struct），OpenAPI 单源生成裁剪到 M4（任务书允许），形状一致性以共享 struct + 一致性测试（同一输入比对的 golden 测试）保证。
- `deploy` 铁律：先 verify（fail 即拒绝，返回结构化诊断）→ 通过则 per-pipeline drain 旧 → 起新 → 变更摘要。无绕过通道（Admin REST 与 MCP 走同一 ops.Deploy）。
- `run` 升级 `--config-dir` 多管道 + Runtime 配置（R13）；`eventboat mcp [--stdio|--http]`（stdio 子进程形态供 Agent host 拉起，HTTP 形态挂 admin /mcp）。
- Agent 闭环验收测试形态：CI 内起 in-process runtime + 经 stdio 子进程拉起 MCP server，用官方 SDK 的 `CommandTransport` 客户端驱动全链路（catalog→verify→test→explain→deploy→status→trigger→修错→再部署）。SDK 客户端成熟（官方测试路径）。
- SSE：`/admin/sse` 推 status 快照与 tail 事件；UI 单静态页读 `/admin/status.json` 画 mermaid + 作业历史表，只读。
- 认证：M2 绑定 127.0.0.1 + 无认证（POC），README 记录安全边界。

## 六、§6.6 OTel 指标清单（M2 实现集，`eventboat_` 前缀；写下的 = 实现的）

计数器（counter，`_total` 后缀）：

| # | 指标 | 标签 |
|---|---|---|
| 1 | `eventboat_messages_in_total` | pipeline, source |
| 2 | `eventboat_messages_settled_total` | pipeline |
| 3 | `eventboat_dead_letter_total` | pipeline, node, reason_class（script/decode/codec/delivery/transform） |
| 4 | `eventboat_dlq_write_failures_total` | pipeline |
| 5 | `eventboat_cel_eval_errors_total` | pipeline, edge |
| 6 | `eventboat_fanout_no_match_total` | pipeline, node |
| 7 | `eventboat_delivery_retries_total` | pipeline, node |
| 8 | `eventboat_optional_drops_total` | pipeline, edge |
| 9 | `eventboat_decode_errors_total` | pipeline, source |
| 10 | `eventboat_spool_failures_total` | pipeline |
| 11 | `eventboat_backpressure_events_total` | pipeline, source |
| 12 | `eventboat_script_step_budget_exhausted_total` | pipeline, node |
| 13 | `eventboat_jobs_started_total` | pipeline, trigger_type |
| 14 | `eventboat_jobs_overlap_skipped_total` | pipeline |
| 15 | `eventboat_jobs_catchup_skipped_total` | pipeline |
| 16 | `eventboat_jobs_completed_total` | pipeline, status（success/partial/failed/canceled） |
| 17 | `eventboat_job_rows_read_total` | pipeline |
| 18 | `eventboat_job_rows_delivered_total` | pipeline |

直方图：

| # | 指标 | 标签 |
|---|---|---|
| 19 | `eventboat_script_duration_seconds` | pipeline, node |
| 20 | `eventboat_sink_write_duration_seconds` | pipeline, node |
| 21 | `eventboat_job_duration_seconds` | pipeline, status |
| 22 | `eventboat_settle_latency_seconds`（accept→settle） | pipeline |

仪表（gauge / observable gauge）：

| # | 指标 | 标签 |
|---|---|---|
| 23 | `eventboat_in_flight_messages` | pipeline（settle outstanding） |
| 24 | `eventboat_spool_depth` | pipeline（arrivedMax − settledThrough） |
| 25 | `eventboat_source_watermark_lag`? → 改为 `eventboat_pipeline_paused`（0/1，背压状态） | pipeline, source |

共 25 个。M1 atomic 计数器迁移策略：引擎保留 atomics（内部逻辑与 status.json 数据源），新增 `internal/obs` 以 OTel 工具二次记录同一事件（R5 拆分后的 SettledCount 供 #2）；无 OTLP endpoint 且 Prometheus 关闭时挂 noop provider（零开销）。

## 七、分流结论

无阻塞项 → 按"第一步（M1 挂账清偿）"开工。R1/R2/R4/R8 在相应步骤实现；R3/R5–R7/R9–R17 按本报告裁决执行并记录于 commit message / README decisions。

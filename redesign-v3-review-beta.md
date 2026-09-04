# redesign-v3.md Beta 硬化轮范围审查报告（实现前关卡）

日期：2026-09-04
评审对象：POC→Beta 硬化轮任务书（清偿台账 / 加固测试 / 定名前置义务 / 发版），对照基线
cfb0325（M1+M2+M3+M4 全闭环）、redesign-v3.md v1.14、三份里程碑审查报告与 README 决策账本。
本轮**无新功能**：还债、加固、定名前置调研、v0.1.0-beta 发版。
评审方法：台账逐条对照源码现状（settle/starhost/jobs/rpcplugin/obs 五个改造点的实现级走查，
含锁序与回调链）+ 依赖假设网络核查（golangci-lint / testcontainers-go，2026-09-04）。

## 总体结论

**通过，无阻塞项。** 按第二步→第五步顺序开工。本轮的两个高风险改造（settle 锁外持久化、
starhost 精确 dirty）均有"语义不变"安全网：七不变量测试零改动 + 嵌套写七例回归零改动，
失败即回滚（单 commit revert）。

---

## 一、Beta 出口定义（可验收口径）

**v0.1.0-beta 退出条件，逐项可机检或对档核查：**

1. **构建与测试**：`go build ./...`、`go vet ./...`、`go test -race ./...` 全绿；
   `go test -count=5 ./...` 全绿（本地跑通入档，CI 保持单轮 race——时长预算所限）。
2. **不变量锚点**：七条 `TestInvariant_*`（internal/engine/invariants_test.go）与全部里程碑
   锚点测试**零改动通过**。例外仅一处且预先记档：TestJobKill9 的等待方式加固（本轮第
   二.3 项）——它修的是测试的负载敏感性，不改任何断言语义；七不变量测试本身零改动。
3. **台账清偿**：本报告第二节"进本轮"清单内项目全部落地或明确裁剪记档；已知 🔴/🟡 级
   台账为零（🔵 级 P2/P3 按定序留 beta+，逐条有理由，见第二节）。
4. **性能**：settle 与 starhost 两项改造有前后基准对比入档（README 性能节扩表）；CI bench
   job 从 informational 升级为宽松阈值门并生效。
5. **长跑**：soak job（workflow_dispatch + 夜间 cron，30 分钟级）存在且手动触发跑通一次；
   断言不变量与无 goroutine 泄漏。
6. **集成纵深**：kafka testcontainers 集成 job（真实 broker：生产/消费/重平衡/死信）在 CI
   绿，时长预算内（目标 < 5 分钟，硬顶 timeout-minutes: 10）。
7. **定名前置**：§8.4 研究性义务执行完毕，docs/naming-checklist.md 交付，人工动作清单明确；
   一切不可逆动作（域名注册、商标申请、GitHub Release 发布）留给用户。
8. **发版卫生**：golangci-lint 基线配置入库且 CI 绿（豁免清单记档如有）；CHANGELOG.md
   （M1→beta 用户视角摘要）；docs/ 索引与交叉链接核对；annotated tag `v0.1.0-beta` 打出，
   tag message 链接 CHANGELOG；tag 上 CI 绿。

**定义里明确不包含**的（避免口径漂移）：无新功能承诺、无 admin 鉴权（beta+ 项，见下）、
无 Pebble/Operator 回锅、无性能绝对数字承诺（只有回归门）。

---

## 二、台账风险定序（进本轮 vs 留 beta+）

对照各里程碑裁剪/P2 记录逐条裁决。**进本轮**（理由）：

| 项 | 来源 | 进本轮的理由 |
|---|---|---|
| settle 锁外持久化 | M1 有意取舍（settle.go:96-100 注释自认） | 当前 settle 回调持 tracker 锁做 SQLite fsync + 源插件回调 + 源状态写，单连接 store（SetMaxOpenConns(1)）下串行化一切存储操作；负载下 settle 延迟直接钉在磁盘上。七不变量是安全网 |
| starhost 精确 dirty | M4 嵌套写修复的副作用（value.go:133-139 容器读即 materialize 即 dirty） | 只读脚本碰一个容器字段就付全量物化 + 全量写回双份转换；§4.3 明言惰性绑定是首要优化点。嵌套写七例回归是安全网 |
| TestJobKill9 负载加固 | M4 观察项（-count/CI 饱和下超时类 flake，同型问题已在 invariants_test.go:303 注记） | beta 出口要求 -count=5 稳定；该测试三处固定 5s/10s 死线 + 双引擎并存的墙钟依赖是最脆一环 |
| 全局背压配额 | M2 R17（P2） | 语义补全非新机制：limits 已是管道级，overlap:all 多 run 并存时聚合共享一个配额池即可；工作量小、记档即裁决 |
| tail 脱敏 | §3.4 明文"脱敏规则适用"+ M2 R12（P2） | 用户可见面（MCP tail / Admin REST）带敏感数据是 beta 硬伤；配置形态裁决见第三节 R-B3 |
| 逐消息 span 采样 | §6.6 + M2 R16（P2） | 默认 0 = 零开销零行为变化；实现是引擎 accept→settle 一条链路 |
| gRPC 插件崩溃重启 | M3 裁剪（快速失败） | 策略可配（默认 fast-fail 保持现语义），带退避与计数指标；restart 是 opt-in，风险受控 |
| kafka testcontainers | M1 裁剪记档"CI stretch, 非关键路径" | kafka source/sink 至今只有合约测试间接覆盖，beta 前补真实 broker 一次；独立 CI job + env 门控，不影响本地 go test |
| soak 长跑 | 任务书第四步 | beta 可信度来源；手动触发跑通一次即出口 |
| 性能回归门 | M3 裁剪（bench job informational） | 宽松阈值门（见第三节 R-B6）；防的是"数量级退化"，不是噪声 |
| golangci-lint 基线 | 任务书第五步 | 发版卫生最低线 |
| 定名前置（研究） | §8.4 唯一悬置义务 | 只调研不动钱；产出人工动作清单 |
| CHANGELOG + tag | 任务书第五步 | 发版本身 |

**留 beta+**（理由）：

| 项 | 来源 | 不进本轮的理由 |
|---|---|---|
| `workers` 总配额 | M2 裁剪（P2） | 背压正确性缺口已由聚合配额（本轮）补上；workers 是吞吐整形旋钮，beta 规模无实测诉求 |
| WASM metadata 通道 | M3 裁剪 | ABI 加字段 = 插件生态震荡，post-beta 与插件版本机制一起做；workaround（script 节点）已文档化 |
| 外部 codec 插件 | M3 裁剪 | 无用户信号；M4 起 codec 注册面（schema+version 强制）已稳定，等具体诉求再开 |
| LSP workspace/多文件 | M4 裁剪 | 一管道一文件是 v3 模型；overlay 组合属 verify CLI 用例 |
| `dlq:` retention 配置段 | §5.10 / M2 裁剪 | 死信已可查可清（replay --delete）；retention 旋钮无使用数据支撑 |
| Pebble profile | M4 R14 裁剪 | 维持裁剪（查询面是 SQL，P2 无吞吐压力） |
| K8s Operator | §6.7 P2 / M4 R14 | 维持"清单 + docs/k8s.md"形态 |
| conformance corpus | §7.4 M1 裁剪 | testkit + 合约套件已覆盖锚点；corpus 是内容生产工程，单列一轮 |
| admin 鉴权 | M2 记档边界（127.0.0.1 + 无鉴权） | 需要独立设计轮（token 形态、MCP/Admin 分层）；beta 以"默认绑回环 + 文档明示"为边界，CHANGELOG 记为已知限制 |
| per-key 有序分片 | §5.x P1 | order_key 已入键；分片是吞吐特性 |
| payload Schema Registry 接入 | 开放问题 #1（P2） | 内联 JSON Schema 起步的既定路线 |
| 跨管道 connectors / `sink: pipeline` | 开放问题 #8（P2） | 三段式已预留加段路径，不动 |
| 插件 loopback TLS | M3 裁剪 | 一次性 token + 127.0.0.1 绑定；与重启策略一起评估 post-beta |
| starlark-rust 兜底 | 开放问题 #7 | 等基准证明瓶颈（本轮基准门正是前置） |
| `load()` 文件白名单 | 开放问题 #2（P1） | 内联脚本模型不变 |

---

## 三、技术预研（两个高风险改造的方案 + 两个基建项的选型）

### R-B1 🟡 settle 锁外持久化：回调移出 tracker 锁 + 单调守卫 + 尝试指针屏障

> **实现期修正（2026-09-04，落地时发现）**：预研的"单 worker 异步合并持久化"方案被
> **不变量 7 测试的零改动约束**否决——inv7 在楔住写中途**不经 waitSettled 直读**
> `st.SourceState`，其确定性依赖"settle 的持久化与 settle 同 goroutine 完成、先于该
> worker 的下一次写"这一旧序（settle.go 旧注释的另一半语义）。完全异步会让该读读到
> 空水位。落地方案（语义严格不变、同样达成"tracker 锁下零 SQLite IO"）：
>
> 1. **记账与回调分离**：`advanceLocked()` 在 `t.mu` 内只做前缀扫描与前沿计算；
>    `invoke()` 在**释放 t.mu 后**于**结算 goroutine 本身**回调 onSettled/onAdvance
>    ——同 goroutine 序保持（inv7 依赖），锁不再横跨 fsync。
> 2. **单调守卫替代全序**：并发 advance 可能乱序 flush，`persistCheckpoint` 以
>    persistMu + 三个单调量（checkpoint 写守卫、per-source 已写 frontier、
>    flushAttempted 尝试指针）保证 checkpoint/水位/指标永不回退；乱序的旧 advance
>    直接跳过。
> 3. **可见性屏障 = 尝试指针**：`durableThrough()`（flushAttempted）进入
>    `WaitSettled` 与 `Quiesced` 的判定——观察者见 settled ⇒ 持久化**已尝试**（成功
>    或失败）。失败也消费尝试指针：持久化永远失败时 WaitSettled 不再楔死（旧行为
>    如此，保持），持久化本身由下一次 advance 重试（不变量 3 兜底）。
> 4. 顺带收益：per-source `Settled` 回调与 `SetSourceState` 由"每次 advance 都写"变
>    为"frontier 前进才写"（幂等语义，写放大下降）。
>
> 基准（BenchmarkSettleThroughput，本机 i5-14600KF，前后各 3s×2）：
> mem **6556 → 6386 ns/op**、fsync_sim(100µs 模拟) **539010 → 543726 ns/op**
> ——吞吐持平（该合成基准的瓶颈是 persistMu 串行化的模拟 fsync 本身，前后皆然）；
> 本改造的收益是结构性的：**tracker 锁的最坏持锁时长从 fsync 级降到 ns 级**
> （snapshot/arrived/add 不再排在 fsync 队列后），语义确定性由同 goroutine 序
> 保持。记档：若未来需要真正的吞吐提升，方向是合并写（batch flush），不是锁。

**现状（实现级走查）**：`settleTracker.maybeAdvanceLocked`（settle.go:96-144）持 `t.mu` 调用
两组回调：`onSettled`（指标原子计数 + admission 释放，微秒级）与
`onAdvance = Engine.persistCheckpoint`（engine.go:319-342：`persistMu` → SQLite
`SetCheckpoint`（fsync）→ 每源 `src.Settled`（插件回调）→ SQLite `SetSourceState`（fsync））。
注释自认这是有意取舍：**"settle 可见 ⇒ 持久化可见"**——观察者轮询 snapshot() 只会在持久写
完成后看到 outstanding==0。SQLite 单连接池（sqlite.go:143 `SetMaxOpenConns(1)`）使这段
临界区进一步串行化全进程所有存储操作。

**方案（语义不变）**：

1. **记账与持久分离**：outstanding 前缀推进（settledPtr 扫描）与前沿计算留在 `t.mu` 内
   （纯内存）；`onAdvance` 的 IO 改为投递给**单 worker 合并持久化器**（per engine）：
   持有 `desired`（最新 through + 各源 frontier，单调合并取 max）与 `flushed`（已落盘），
   worker 循环取 desired 快照 → `SetCheckpoint` → `src.Settled` → `SetSourceState` → 更新
   flushed。合并语义天然正确：checkpoint 是幂等覆盖写，源状态单调，多次 advance 只需落
   最终值；顺序性由单 worker 保证。
2. **可见性屏障**：所有原先隐含"settled ⇒ durable"的观察点显式加 flush barrier——
   `Engine.WaitSettled`（outstanding==0 后等 flushed==desired）、`Engine.Quiesced`（jobs
   终态判定，同样加 flushed==desired）、`Engine.Close/drain`（关 store 前 flush）。
   `Metrics.CheckpointPtr` 移到 worker 成功落盘后更新（其文档语义"mirrors the durable
   checkpoint position"反而更准了）。ops Status 的 settledThrough 读 flushed 指针。
3. **失败语义不变**：持久失败只放大崩溃回放窗口、从不丢消息（不变量 3）。worker 失败
   退避重试（50ms 级）；Close 时最终 flush 一次，失败记日志返回（与现在 persistCheckpoint
   失败即过的行为一致，只是从同步变异步）。
4. **`onSettled` 留在锁内**：纯原子操作 + 信号量释放，无 IO，不动（最小 diff 原则）。
   settle_latency 指标含义不变（accept→settle 记账，不含持久化）——前后对比因此干净。

**安全网与验收**：七不变量测试零改动全绿（关键点：凡测试等待的是**持久可见状态**
（store.Checkpoint / SourceState / 交付计数），flush 屏障使可见时机语义与旧实现一致；
TestInvariant_Kill9Replay 的"崩溃"夹在 settle 与 flush 之间时，效果等价于真崩溃——回放窗
放大、at-least-once 保持，正是该测试的断言域）。新增微基准
`BenchmarkSettleThroughput`（mem store 与 sqlite store 各一，no-op sink，测 accept→settle
全链吞吐），前后数字入档。

**回滚方案**：整改造单 commit 交付，revert 即回到持锁持久化；无 schema/接口变更
（store.Store 面不动，persister 是 engine 内部组件）。

### R-B2 🟡 starhost 精确 dirty：变异拦截替代"物化即脏"

**现状**：`materialize()`（value.go:331-338）无条件置 dirty；`fieldGet` 对容器字段（map/
list）先物化再返回引用（M4 嵌套写修复，value.go:133-139）。结果：`x = payload.nested`、
`for k,v in payload` 这类**只读**访问也付全量 `GoToStarlark` 物化 + 引擎侧全量
`StarlarkToGo` 写回（nodes.go:128-136），语义零效果。

**方案**：物化不再置 dirty；dirty 只由**真实变异入口**置位——我们的包装类型上的写操作
（attrDict 的 SetKey/SetField、deleteKey、remove() 胶水；list 包装的 append/insert/SetIndex/
pop 等）。凡物化树内的容器必须是可拦截包装类型（含 list 包装——starlark 原生 *List 的
builtin 变异无法拦截，`payload.items.append(x)` 会绕过；实现时按 convert.go 现状补 list
包装或变异入口，嵌套写七例回归 + TestNoWriteMeansNoDirty + TestScalarReadsStayLazy 是
安全网）。read-only 脚本碰容器后：物化照付（引用语义要求），**写回与 dirty 不再发生**。

**安全网与验收**：嵌套写七例零改动全绿；新增反向用例（容器只读不 dirty：
`x = payload.nested.k` / `for` 遍历含容器）。基准：BenchmarkSimpleScript（既有）+
新增 BenchmarkContainerReadOnly（只读碰容器的脚本），前后入档。

> **实现期修正（2026-09-04，落地时发现）**：完整"变异拦截"对 **list 不可行**——
> starlark 原生 `*List` 的 append/索引写是原生绑定方法，无法拦截；原生 dict 的
> update/pop 等内置方法同理，但 dict 树内全是我们的 attrDict，可在 Attr 层包装
> （update/pop/popitem/clear/setdefault）。落地方案（分两档）：
>
> 1. **map 树精确**：attrDict 携带 owner 的 mark 闭包；SetKey/SetField/Delete/
>    变异类内置全部 mark；materialize 不再置 dirty；GoValue 对"已物化但干净"的
>    状态返回**原解码值**（写回跳过下沉到 starhost 层，engine 门不变）。
> 2. **含 list 的树保守**：转换时探测树内有无 list（convertGo 返回 hasLists），
>    有则在 materialize 时保守置 dirty（= 今日行为，精确语义零回归）；边界由
>    TestListTreeStaysConservativeDirty 锁死——将来若引入 list 包装类型，翻转
>    该测试即可。
>
> 基准（i5-14600KF，2s×2）：ContainerReadOnly **1901 → 1458 ns/op（-23%，写回
> 跳过）**；SimpleScript **1548 → 1568 ns/op（写路径持平）**。七例嵌套写回归 +
> TestNoWriteMeansNoDirty + TestScalarReadsStayLazy 零改动全绿。

**回滚方案**：单 commit revert；对外行为面（MsgState.Dirty/GoValue 接口）不变。

### R-B3 🔵 tail 脱敏配置形态裁决

**裁决**：管道级 `telemetry:` 段（§5.10 本就预留）落最小两个字段：

```yaml
telemetry:
  redact:                     # glob 列表：命中即掩码（值 → "***"）
    - payload.user.email
    - payload.credit_card*
    - meta.authorization
  span_sample_rate: 0.0       # 逐消息 span 采样率，默认 0（R-B4）
```

- **模式语言**：点分字段路径，每段是 path.Match glob（`*` 通配单段内字符）；前缀
  `payload.` / `meta.` 对齐绑定根。无 `**`（跨段通配）——需求出现再加，避免一次发明完。
- **作用面**：tail 是呈现层数据（ops 层 ring buffer），脱敏在 recordTail 截断**前**做：
  payload 按 JSON 解码 → 命中路径的值替换为 `"***"` → 重新序列化 → 截断 512B。非 JSON
  payload 无法定位字段，跳过脱敏（记入文档，不猜字节）。**spool/死信/交付本体永不脱敏**
  （那是数据面，改了破坏 at-least-once 与 replay 语义）。
- **校验**：verify 时逐 pattern `path.Match` 编译检查，坏模式 = 错误
  （`telemetry_redact_pattern`）；loader 顶层白名单加 `telemetry`，撤掉既有 hint 分支
  （loader.go:137-139 预言了这个键）；LSP topLevelSections 同步。
- 大小写/嵌套数组索引（`payload.items.*.sku`）：`*` 段匹配数组全部元素——实现为一行
  递归，覆盖列表内 dict 的字段。

### R-B4 🔵 逐消息 span 采样

管道级 `telemetry.span_sample_rate`（默认 0=零开销零 span）。实现点：`Engine.accept`
按率起 span（`eventboat.message`，属性 message_id/source/seq），`onSettled` 与
`deadLetterMsg` 收尾（终态属性 + 错误事件）；tracer 来自 obs（OTLP 未配置时是 noop
tracer，率>0 也只有可忽略成本）。与 R16 不冲突：R16 裁决的是**不默认逐消息**，采样率
默认 0 正是该裁决的配置化表述。job run span 不受影响（ParentBased 采样链保持）。

### R-B5 🔵 golangci-lint 基线

golangci-lint **v2.13.2**（2026-08-28 发布，2026-09-04 核查）。配置 `.golangci.yml`
（`version: "2"`）：默认 linter 集（errcheck/govet/staticcheck/unused/ineffassign）+
formatters（gofmt）+ misspell；`pkg/pluginv1`（生成码）与 `legacy/`（独立模块，本就不在
build 面内）排除。CI 独立 step（go test 之后，~1-2 分钟）。清零或豁免清单记档于本文件
附记。本地验证：`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run`。

### R-B6 🔵 testcontainers-kafka CI 可行性与时长预算

**选型**：`github.com/testcontainers/testcontainers-go/modules/kafka`（官方 Kafka 模块，
KRaft 模式免 ZooKeeper，镜像 `confluentinc/confluent-local:7.5.0`，动态端口从
`container.Brokers` 取——2026-09-04 官方文档核查）。依赖树较重但**仅测试导入**（env 门控
的独立包 `internal/inttests/kafka`，本地 `go test ./...` 无 Docker 时整包 skip，
不进生产 build 面）。

**CI 形态**：独立 job `kafka-integration`（ubuntu-latest 自带 Docker），env
`EVENTBOAT_KAFKA_TEST=1`，`timeout-minutes: 10`。时长预算：容器启动 15-40s + 四条路径
（生产/消费 roundtrip、消费组重平衡、死信路径、崩溃后从提交水位续传）各 5-30s →
目标总时长 < 5 分钟。跑不动时的降级预案：镜像预热（actions 缓存不可行则接受 40s 启动）
或 `confluent-local` 换更小镜像（apache/kafka 镜像同模块支持）。

### R-B7 🔵 性能回归门（宽松阈值）

bench job 去掉 `continue-on-error`，跑三项关键基准：CEL 谓词
（BenchmarkPredicateEval）、starhost 简单脚本（BenchmarkSimpleScript + 新增
BenchmarkContainerReadOnly）、settle 吞吐（新增 BenchmarkSettleThroughput）。阈值以
"基线值 × 宽裕倍数"硬编码在 CI 脚本里（ubuntu-latest 同级 runner 上基线的 3-5 倍，
防的是数量级退化与算法回退，不是噪声）；基线数字与倍数记档于本文件附记 + README。
超阈值 job 红。WASM 基准（HeavyTransform）保持 informational（wazero 跨 runner 方差大）。

### R-B8 🔵 soak job 形态

`workflow_dispatch` + 夜间 cron 的独立 workflow：`go test ./internal/inttests/soak/ -timeout 35m`
（env 门控同 kafka）。内容：3 条管道（连续 fan-out、job sql 拉取、含脚本重试）混合负载
跑 ~25 分钟，随机故障注入（testkit FlakySink/StoreWrapper 钩子、中途 Abandon 模拟崩溃再
resume），断言：交付计数 ≥ 注入计数（at-least-once）、checkpoint 单调、终态后
goroutine 数回落到基线（stdlib runtime.NumGoroutine 对账，不引 goleak）。首次以手动触发
跑通为出口，夜间 cron 是常驻收益。

---

## 四、实现顺序与提交切分

1. 第二步（风险先行，每项独立 commit，push 即 CI）：R-B1 settle → R-B2 starhost →
   Kill9 加固 → 背压聚合配额（第二节台账）。**每项落地后本地 -count=5 对应包 +
   全量 race 验证再 push。**
2. 第三步：R-B3 脱敏 → R-B4 span 采样 → 插件重启策略 → P2/P3 定序记档收尾。
3. 第四步：kafka job → soak → 阈值门。
4. 第五步：lint → CHANGELOG/文档 → tag。

一处纪律豁免预先记档：TestJobKill9 的等待方式改造允许改测试文件（它不是不变量测试，
且改造目标是让它在负载下更稳而非更松——断言不动，只改等待条件与余量）。

## 附记（发版时回填）

- [x] settle 前后基准：BenchmarkSettleThroughput mem **6556 → 6386 ns/op**、
  fsync_sim **539010 → 543726 ns/op**（i5-14600KF，3s×2；详见 R-B1 实现期修正）
- [x] starhost 前后基准：ContainerReadOnly **1901 → 1458 ns/op（-23%）**、
  SimpleScript **1548 → 1568 ns/op**（i5-14600KF；详见 R-B2 实现期修正）
- [ ] lint 豁免清单（如有）
- [ ] 阈值门基线数字与倍数
- [ ] soak 首跑链接、kafka job 首绿链接

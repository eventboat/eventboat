# redesign-v3.md M3 范围独立设计审查报告（实现前关卡）

日期：2026-09-04
评审对象：`redesign-v3.md`（v1.11）的 M3 范围——§4.5/§4.6 阶梯第三档（WASM transform）、§4.7 CESQL 方言、§6.5 gRPC 进程外插件协议与插件 ABI 版本化、§7.4 M3 验收（第三方插件 / TCK 100% / WASM 基准收益），对照 M2 实现（63c02bb）与两份前序审查的已记档决策。
评审方法：全文细读 M3 相关章节 + 依赖假设逐项对照 pkg.go.dev / 官方仓库 / Go module proxy 实测下载 + **本机实测**（wazero 运行 Go wasip1 guest 的三种实例化模式、超时击杀、内存上限；下述证据标注"实测"者均为跑过代码的结论，非文档推断）。

## 总体结论

**通过，无阻塞项。** 可以进入实现。

- 依赖选型全部有实证结论（第一节）；任务书要求"给出建议供裁决"的 WASM guest 工具链问题被实测**第三选项消解**：标准 Go 工具链 `-buildmode=c-shared`（reactor）直接产出 wazero 可用的 `_initialize` 模块，TinyGo/Rust 均不再必要（R2）——CI 零新增工具链，guest 与宿主同语言同仓库；
- **无 🔴 阻塞项**；**5 项 🟡 中**（R1/R3/R4/R5/R8：WASM 无燃料计量改用双上限、wasm 档消息 wire 形态裁剪、trap 后模块死亡须重实例化、gRPC 协议握手与 verify 纯度的切分、CESQL 扩展模式经预解析重写实现——均不改设计结论，按裁决实施）；**9 项 🔵 低**；
- 两处与规范措辞的偏差以"扩展而非违背"处理并记档：CESQL 扩展模式不宣称兼容（§7.4 验收第 5 条措辞，规范 §7 开放问题 #5 已预设此纪律）；WASM 消息 wire 只走 payload 字节（§6.5 未规定 wire 细节，属实现自由度）。

---

## 一、依赖假设核查（对照 pkg.go.dev / 官方仓库 / Go proxy / 本机实测，2026-09-04）

| 假设 | 核查结果 | 判定 |
|---|---|---|
| wazero 当前版本与支持面 | **v1.12.0**（2026-05-28，Go proxy 实测）。模块路径仍为 `github.com/tetratelabs/wazero`（GitHub 仓库已迁移至 `wazero/wazero` 组织，proxy Origin 指向新仓库、旧路径持续可用）。零依赖、无 CGO；WASI **仅 preview1**（`imports/wasi_snapshot_preview1`）；宿主函数经 `NewHostModuleBuilder`；内存上限 `WithMemoryLimitPages`；上下文取消击杀 `WithCloseOnContextDone(true)`；时钟/随机源默认确定性假值（沙箱友好）；默认无文件系统、stdio 丢弃 | ✅ **选型：wazero v1.12.0**（规范 §6.5 指定） |
| wazero 维护状态 | 维护节奏放缓（原核心维护者转向其他项目；ncruces/go-sqlite3 等下游公开讨论迁离）。但：API 稳定承诺在案（semver）、零依赖纯 Go、v1.12.0 为 2026-05 发布，且规范点名 wazero。风险对冲：锁版本 + 我们只用最保守面（reactor 实例化 + 内存导出函数调用 + ctx 击杀 + wasi 基础函数） | ✅ 可用，风险记档（README 决策账本） |
| WASM guest 工具链：TinyGo vs Rust(wasm32-wasip1) vs 预编译 | **实测发现第三选项并采信**：Go ≥1.24 对 `GOOS=wasip1 GOARCH=wasm` 支持 `//go:wasmexport`，且 **`go build -buildmode=c-shared` 产出 reactor 模块（导出 `_initialize`，不跑 `main`）**——Go 1.24 官方发布说明明确此用法。本机实测（go1.25.0 + wazero v1.12.0）：`WithStartFunctions("_initialize")` 实例化后导出函数可直接、重复、并发安全地调用，输入输出经 guest 内存往返正确（见 R4 ABI）。对照实测否定项：command 模式（默认 build）`_start` 跑完即 `exit(0)`，模块死亡；`main` 阻塞 + goroutine 启动会触发 Go runtime 死锁检测器 `exit(2)`。TinyGo（wasi 目标仍标注 experimental、需额外安装 ~100MB）与 Rust（需 rustup + target，CI 增 1-2 分钟）均退为 docs 中"任何 wasip1 reactor 语言皆可"的示例性提及 | ✅ **选型：标准 Go 工具链**；TinyGo/Rust 不引入 |
| CESQL 解析器复用 | 官方 `github.com/cloudevents/sdk-go/sql/v2` **v2.16.2**（2025-09-22，proxy 实测可下载），`parser.Parse(string) (Expression, error)`，`Expression.Evaluate(cloudevents.Event) (interface{}, error)`，返回 int32/bool/string。连带依赖：`cloudevents/sdk-go/v2` v2.16.2 + 旧版 ANTLR 运行时 `github.com/antlr/antlr4/runtime/Go/antlr v1.4.10`（已停止演进但发布件不可变；与 cel-go 用的 antlr4-go 是不同模块路径，可共存）。解析器**只支持上下文属性标识符，不支持 `data.*`**（lexer 无 DOT token，实测 grammar 确认）——扩展模式的实现方式见 R8 | ✅ **复用官方解析器**（实现规范 ≠ 造轮子）；依赖重量（zap/json-iterator 等随 sdk-go/v2 进入）记档接受 |
| CESQL TCK | 官方仓库 `cloudevents/spec` `cesql/cesql_tck/`：**18 个 YAML 文件**（Apache-2.0），格式 `{name, tests: [{name, expression, event|eventOverrides, result|error}]}`，错误类型枚举 parse/math/cast/missingAttribute/missingFunction/functionEvaluation/generic。sdk-go 自带同构 runner（`sql/v2/test/tck_test.go`，基准事件 = `test.FullEvent()`，可直接 import `github.com/cloudevents/sdk-go/v2/test` 复用） | ✅ **TCK 文件 vendored 入库 + 自建 runner 镜像官方语义**（R8） |
| gRPC / protobuf | `google.golang.org/grpc` v1.83.1、`protobuf` v1.36.12 已在模块图（MCP SDK 间接依赖），转直接依赖零成本。本机 protoc/buf/protoc-gen-go/protoc-gen-go-grpc 齐备；生成物入库，CI 不需要 protoc | ✅ |
| jsonschema（外部插件 schema 校验） | M1 已用 santhosh-tekuri/jsonschema/v6（registry 编译路径），外部插件 manifest 复用同一编译器与诊断格式 | ✅ 零新增 |

**结论：无依赖阻塞。**

---

## 二、问题清单

严重度：🔴 阻塞 / 🟡 中（实现必须处理，不改设计结论）/ 🔵 低（缺口补决策）。

### R1 🟡 wazero 没有燃料/步数计量——"步数上限"以双上限替代

- **证据**：wazero v1.x 无 fuel/epoch 中断 API（pkg.go.dev 全 API 面核查）；Starlark 档的 `SetMaxExecutionSteps` 精确步数预算在 WASM 档无对应物。实测可用的两个杠杆：`WithCloseOnContextDone(true)` + 每次调用独立 `context.WithTimeout`（实测：死循环 guest 恰在 deadline 被击杀，返回 `module closed with context deadline exceeded`）；`WithMemoryLimitPages`（实测：Go guest 在 512 页上限下正常运行）。
- **裁决**：WASM 档资源模型 = **每次调用 wall-clock 超时（默认 1000ms，`wasm.timeout_ms` 可配）+ 内存页上限（默认 512 页 = 32MiB，`wasm.max_memory_pages` 可配）**。README 明示与 Starlark 步数预算的语义差异（WASM 档是时间界不是操作数界；超时错误与脚本错误同走 delivery→死信）。可观测：超时/内存超限计数进 `eventboat_wasm_timeouts_total`。

### R2 🟡（任务书要求裁决项）guest 工具链 = 标准 Go reactor；示例 .wasm 入库 + CI 重建双轨

- **证据**：第一节实测。CI 影响：`GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared` 是普通 go build——现有 CI job 一行命令可产出 guest，无需 TinyGo/Rust 安装。
- **裁决**：
  - 仓库内 guest（示例 + 基准）用 Go 编写，`internal/wasmhost/testdata/` 存源码；
  - **编译产物 .wasm 入库**（`examples/wasm/*.wasm` + 测试 guest），保证 `go test ./...` 无工具链也能跑集成测试；
  - CI 新增一步"重建 guest + 比对产物字节"防漂移（`go build` 确定性足够：同版本工具链同参数产出可复现；若字节比对偶发不稳则降级为"重建后跑集成测试"，实施时定）；本地开发用仓库根 `build-guests` 脚本重建。
  - docs/wasm.md 记录 ABI 与"任意 wasm32-wasip1 reactor 语言皆可"（Rust `#[no_mangle]`、TinyGo `//export` 的最小示例列为文档片段，不进构建）。
- **产物体积记档**：Go guest 约 1.7MB（含运行时）；`-ldflags "-s -w"` 后约 1.2MB。可接受（不是网络分发，是本地构建产物）。

### R3 🟡 WASM transform 的消息 wire 形态：payload JSON 字节进出，metadata 不进 guest

- **证据**：任务书开放点"payload 原始字节 + metadata JSON？——裁决 wire 形态"。候选：A) payload 字节 + metadata JSON 双通道；B) 仅 payload。规范 §6.5 只说"消息以 bytes 进出"，未规定 metadata。§4.6 对 WASM 档的定位是"重计算/性能/依赖逃生舱"——重计算作用于 payload；meta 操作 Starlark 已覆盖且 COW 绑定已优化。
- **裁决**：**B，仅 payload**。具体语义：
  - 入：`JSON 编码(inst.msg.Decoded)` 写入 guest 内存（Decoded 为解码后值，与 script 档视角一致；上游可为 script 节点，链式正确）；
  - 出：guest 返回字节 → 以 JSON 解码替换 `msg.Decoded`（解码失败 = transform 错误，同 script 错误模型：沿入边 delivery 重试 → 死信）；
  - meta 原样透传（wasm 节点不改 meta；要改 meta 就串一个 script 节点——组合优于配置）。
  - ABI：guest 必须导出 `eb_alloc(len i32) -> i32` 与 `transform(ptr i32, len i32) -> i32`（返回指向"4 字节 LE 长度前缀 + 输出字节"的指针；返回 0 = 错误，可选导出 `eb_last_error() -> i32` 提供错误文本，同样长度前缀格式）；模块须为 reactor（导出 `_initialize`）。宿主负责 `eb_alloc` 写入与结果读回。
  - 错误契约：trap（含超时/内存超限）、返回 0、输出 JSON 解码失败 = transform 失败 → 入边 delivery 策略 → 死信（与 script 档完全同路径，`processTransform` 复用重试/死信骨架）。

### R4 🟡 wazero 模块被 trap/超时击杀后死亡——每 worker 一实例 + 失败重实例化

- **证据**：实测：ctx 击杀后模块进入 closed 状态，后续调用一律失败（`module usable after kill: false`）。且 `api.Module` 非并发安全——多 worker 并发调同一实例不合法。
- **裁决**：wasmhost 结构 = **共享 `CompiledModule`（编译一次，wazero 编译缓存友好）+ 每 transform worker goroutine 一个惰性实例**（编译产物实例化 <1ms 级，可接受）；实例遇 trap/超时/错误后关闭并在下一条消息前重建（错误信息计入死信原因）。`workers > 1` 时各 worker 独立实例独立重建，互不牵连。默认能力面：仅实例化 `wasi_snapshot_preview1`（stdio 丢弃、确定性假时钟、无文件系统、无 env/args）；`wasm.allow: [log]` 显式开通后 guest stdout/stderr 接引擎日志（"能力制沙箱"：默认零额外宿主能力，显式配置才开）。

### R5 🟡 gRPC 插件协议：stdout 单行握手 + 静态 manifest 文件，verify 不 spawn 进程

- **证据**：规范 §6.5 要求"协议带版本协商、health、schema 声明"。现架构的两个硬约束：(a) 关卡 1 verify 承诺"静态、确定性、零副作用"（README/§3.1），spawn 任意子进程违反承诺；(b) ir.Build 在 verify 路径就调用 `reg.NewSource` 做实例化校验（ir.go:251）——外部插件若在此时 spawn 即破坏 (a)。另：HashiCorp go-plugin 的握手魔术（env 变量、TLS、多协议自动协商）对"任意语言按文档实现"是额外负担，不引入。
- **裁决**（协议 v1 全文落 docs/plugins.md，此处为裁决要点）：
  1. **静态声明与运行时声明分离**。配置侧 `grpc:` 块携带 `schema:` 指向**插件 manifest 文件**（JSON：`{kind, name, version, capabilities[], config_schema}`）；verify 全程只读 manifest（jsonschema 同内置编译路径、同诊断格式），不 spawn。运行时 spawn 后**握手交叉核对**：manifest 与握手行的 name/version/kind 不符 = 明确报错（防"文档说有、进程里没有"的漂移——§6.5 版本化的动机）。
  2. **握手**：插件进程启动 → 自选 `127.0.0.1:0` 监听 → **向 stdout 打印单行 JSON** `{"eventboat_plugin":1,"kind":"source","name":"ticker","version":3,"capabilities":["pull"],"listen":"127.0.0.1:54321","auth":"<随机令牌>"}` → 宿主 dial gRPC，令牌进 per-rpc metadata `eventboat-auth`（防本机其他进程搭车；非 TLS——本机回环 + 一次性令牌是 M3 威胁模型的合理边界，记档）。版本协商 = `eventboat_plugin` 协议主版本必须相等 + 声明版本必须相等；不符即拒。
  3. **服务面**（proto 包 `eventboat.plugin.v1`，一个 .proto 文件）：
     - `Source` 服务：`Init(state bytes)`、`Run(stream Event)`（连续源：流不终止）、`Pull(stream Event)`（作业源：翻页发射，**正常 return OK = 取尽**，流中 error = 源失败——与 registry.PullSource 语义逐字对应）、`Settled(through_seq) -> state`、`Close`；
     - `Sink` 服务：`Init`、`Write(batch Event[])`（引擎拥有批量，插件只管写——§6.4 不变量延伸到进程外）、`Close`；
     - health：标准 `grpc.health.v1`（Go 语言 `health.NewServer()` 一行，其他语言同样标准）；宿主连接时与错误路径检查，不做周期轮询（进程死亡由流错误/进程退出即刻暴露，M3 不做自动重启——见第 6 点）。
  4. **Event wire**：`payload bytes + meta map<string,MetaValue> + codec string + cursor string + src_seq int64 + src_name string`；`MetaValue` 自定义 oneof（string/int64/bool/double）——不用 `google.protobuf.Value`（第三方任何语言实现更省事），不用 `map<string,string>`（kafka_offset 是 int64，字符串化会破坏下游 CEL/CESQL 谓词的类型）。对照内置 kafka/http 源 meta（字符串 + int64）能力面足够。
  5. **背压**：宿主按 admission gate 节奏读流，gRPC 窗口传导背压；插件侧文档要求"遵守发送阻塞（TCP 背压即准入）"。
  6. **进程生命周期**：启动（argv = `grpc.command`，工作目录 = 配置文件目录）、**优雅停止**（Close RPC → 5s 宽限 → SIGKILL/Windows 强杀）、**崩溃策略 M3 = 快速失败**：进程死亡 → 源流错误上抛（连续管道：该源 goroutine 终止并记指标；作业：run failed）——自动重启留待后续（记入裁剪清单；规范 §6.5 未承诺自动重启，at-least-once 语义下重启重放由运行级别 SIGHUP/deploy 循环承担）。
  7. **协议首版字段充分性**（任务书核查点）：对照现 registry 接口——Source 四方法 + Pull 能力、Sink 批写、codec 传递、cursor（sql 源位点）齐备；缺省项：codec 注册面（外部插件不能自带 codec——文档明示 M3 外部插件只做 source/sink，decode/encode 仍用内置 codec 名）。

### R6 🔵 配置形态裁决：节点级 `grpc:` 框架字段 + 节点级 `version` 框架字段

- **证据**：任务书两候选："插件块内 grpc: {...}" vs "独立插件声明段"。插件块 = 节点内插件名键下的字段，schema 严格校验会拒绝未知键 `grpc`——**grpc 必须是节点级框架字段**（nodeWhitelist 增补），与 `decoder`/`workers` 同级：
  ```yaml
  sources:
    prices:
      grpc: { command: ["go", "run", "./examples/plugins/ticker-source"], schema: examples/plugins/ticker-source/manifest.json }
      ticker: { symbol: "USD/EUR" }     # 插件名仍为块键，字段按 manifest schema 严格校验
      version: 1                          # 可选：声明版本，不符 = verify 错误
  ```
- **裁决**：采用上图形态（任务书候选 A 的可行变体）。解析规则：有 `grpc:` 的节点，块键先查内置 registry，命中 = 错误（内置插件不许再挂 grpc 块）；未命中 = 外部插件路径。`version` 同为节点级框架字段，内置/外部长效（R11）。`grpc:` 字段集：`command`（argv 数组，必填）、`env`（map，可选）、`schema`（manifest 路径，相对管道文件，必填）。

### R7 🔵 WASM 配置形态：`wasm:` 主字段与 script|split 互斥

- **证据**：规范 §4.3/§5.6 已预留 `wasm` 为 transform 主字段三选一；reservedNames 已含 "wasm"。
- **裁决**：
  ```yaml
  transforms:
    heavy:
      from: [ingest]
      wasm:
        module: transforms/heavy.wasm      # 路径相对管道文件
        entrypoint: transform              # 导出名，默认 transform
        timeout_ms: 1000                   # 默认 1000
        max_memory_pages: 512              # 默认 512（32MiB）
        allow: []                          # 能力白名单，默认 []；可用: log
  ```
  config.Node 增 `Wasm *WasmConfig`；sections.go 主字段互斥扩展为三选一；ir 对模块文件做**存在性 + 编译**预检（编译一次复用 wazero，verify 即能报坏模块——编译 wasm 不执行代码，零副作用）。触发标准写 README（§4.6：性能/依赖才是理由，逻辑复杂不是）。

### R8 🟡 CESQL：复用官方解析器跑官方 TCK；`data.*` 扩展经**预解析重写**实现

- **证据**：官方解析器 lexer **无 DOT token**（grammar 实测核对），`data.foo` 无法通过官方 Parse；而规范 §4.7 要求 `data.*` 作为文档化扩展触达 payload。sdk-go 的 `Evaluate` 以 `cloudevents.Event` 为载体，上下文属性 + 扩展属性即全部可寻址空间。
- **裁决**（三层）：
  1. **纯模式（= 官方 TCK 语义）**：`when: { lang: cesql, expr: ... }`；宿主把 `meta` 键值映射为 CESQL 上下文属性（Eventboat meta 键全为 `[a-z0-9_]`——与 CESQL IDENTIFIER 词法天然吻合）；meta 值类型映射：string→string、bool→bool、int64→int32（kafka 偏移量域内）、float64 整数值→int32 / 非整数→字符串、其他（数组/对象）→JSON 字符串。键含 CESQL 词法外字符的 meta 项在 CESQL 模式下不可寻址（文档明示）。
  2. **TCK 验收**：18 个官方 YAML **vendored 入库**（保留 Apache-2.0 头与来源注释），runner 镜像官方 `tck_test.go` 语义（基准事件 `test.FullEvent()`、错误类型断言、int→int32 归一）——**纯模式整跑 TCK 100% 通过进 CI**。规范说"纯模式（仅上下文属性）"——TCK 全部用例本就只触达上下文属性（官方解析器无 data 支持），故"纯模式 TCK" = 整套 TCK。
  3. **扩展模式**：`data.x`（含嵌套 `data.x.y`）经**字符串字面量感知的预解析重写**为合成标识符 `data_x_y`（重写器只动引号外文本；`data` 后紧跟 `.` 的词素才改写），宿主把 payload 顶层及嵌套字段展平注入为 `data_*` 合成属性（值映射同上，null = 不注入 = missing attribute）。**保留名纪律**：CESQL 模式下 `data_` 前缀标识符保留给扩展——meta 键以 `data_` 开头时该节点 verify 报错（抢占早暴露）。扩展模式自建用例集进 CI；对外措辞遵守 §7 开放问题 #5：扩展模式 = "CESQL 子集 + 文档化扩展"，不挂 TCK 徽章。
  4. **错误契约**：Parse 失败 = verify 错误（编译期暴露，与 CEL 同关位）；Evaluate 失败（含 missing attribute）= 条件不通过 + `eventboat_predicate_errors_total` 计数（与 CEL 档同关位、同指标）——完全沿 CESQL/CEL 既有契约，无新语义。
- **连带裁决**：Edge.When 配置形态从"仅字符串"扩为"字符串（CEL，默认）| 对象 `{lang: cel|cesql, expr}`"；ir.Edge.When 从 `*celhost.Predicate` 改为小接口（`Lang() string; Eval(payload, meta) (bool, error)`），celhost/cesqlhost 各自实现——engine/explain 两个求值点零感知改动。

### R9 🔵 插件 ABI 版本化落地位置

- **证据**：§6.5"catalog 输出带版本；配置引用版本与运行时不符 = verify 错误"。
- **裁决**：`registry.RegisterSource/RegisterSink` 增 version 参数（内置插件全部 version 1）；`SourceMeta/SinkMeta` 与 catalog JSON 增 `version` 字段；config 节点级 `version`（R6）与注册版本不符 = verify 错误 `plugin_version_mismatch`（内置：对照 registry；外部：对照 manifest，运行时握手再对照进程实际版本）。缺省声明 = 不校验（宽松向后兼容，文档建议生产显式声明）。

### R10 🔵 基准方案：重度脚本 Starlark vs WASM 同题对拍

- **证据**：§7.4 M3 验收"基准证明 WASM 档对重度脚本的收益"；§4.6 会撞墙场景 = "大 payload 上的循环/加密、解释器税 10-100 倍"。
- **裁决**：同题双实现——payload 为含 2000 元素数组的 JSON，聚合计算（sum + max + 均值的多遍循环）；Starlark 版（conformance 风格脚本）vs Go reactor guest（同算法）。形式：`internal/wasmhost/bench_test.go` 双 Benchmark + `go test -bench -benchtime` 一键复现；CI 可选 job（`continue-on-error`，不做回归门——绝对数字跨机器不可比，门会假阳性）+ README 记录参考机数字与复现命令（"基准纪律"：可复现第一，门禁第二）。轻量脚本对照（简单字段拷贝）一并记录，诚实呈现"WASM 档对轻脚本无收益（实例化+编解码开销）"。

### R11 🔵 第三方插件验收测试形态（§7.4 锚点）

- **裁决**：`examples/plugins/ticker-source/` 为**独立 Go module**（自己的 go.mod），只 import proto 生成代码（经 replace 指回主仓库的 `internal/rpcplugin/proto` 包——模拟第三方仅凭 .proto + docs/plugins.md 复现，绝不 import 引擎内部包）；CI 内先 `go build` 出插件二进制，再以配置引用该二进制跑 verify→run 全链路断言（ticker 源每秒发 1 条价格事件，作业模式 pull 取尽）。

### R12 🔵 顺带清偿：作业管道多 pull 源 → verify warning

- **裁决**：ir.checkJobSemantics 增检测：job 模式下 pull 源数量 > 1 时 warning `job_multiple_pull_sources`（提示 `cursor` 参数绑定首个 pull 源的水位，多源场景需在脚本里显式用 `meta.cursor` 语义明确的源）——warning 不拦 verify（M2 审查 #6 裁决原意）。

### R13 🔵 docs 结构

- `docs/plugins.md`：gRPC 协议全文（握手行、manifest、proto、生命周期、背压、版本、安全边界）+ 最小 Go 示例 + 其他语言实现要点；验收锚点 = R11 测试仅凭此文档与 .proto 可复现。
- `docs/wasm.md`：ABI（eb_alloc/transform/长度前缀/错误信号）、能力模型（默认零宿主能力）、资源上限、Go/Rust/TinyGo 三语言最小导出示例、构建命令。
- README：M3 能力摘要、决策账本增补（本审查 R 编号引用）、基准数字、裁剪清单（gRPC 插件自动重启、外部 codec、WASM metadata 通道、CI 基准门禁）。

### R14 🔵 规范回写清单（实现合入后同步 redesign-v3.md v1.12）

- §4.7 补 `when` 对象形态与 `data_*` 保留名；§6.5 补 reactor ABI 要点与双上限资源模型、gRPC 握手行与 manifest 分离；§7.4 M3 验收行补"标准 Go 工具链构建 guest"。不改变任何已定语义。

---

## 三、承重墙复核（M3 范围）

- **七条不变量零改动**：WASM transform 失败走既有 delivery→死信路径（不变量 4 延续）；gRPC 源的 Settled 位点语义与内置源同接口（不变量 5/7 延续）；外部插件不参与 spool/checkpoint 内部机制。既有 `TestInvariant_*` 七测不改语义。
- **四道关卡纯度**：verify 不 spawn 进程（R5.1）、不执行 wasm 代码（仅编译预检）；外部插件 schema 校验走关卡 1。
- **分层依赖方向**：wasmhost/rpcplugin 与 starhost/celhost 平级（lang 层扩展位），ir 编译期消费其编译产物，engine 运行期消费其实例——无反向依赖。

**结论：按上述裁决进入实现，无需用户裁决项。**

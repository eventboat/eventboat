# redesign-v3.md 独立设计审查报告（实现前关卡）

日期：2026-09-03
评审对象：`redesign-v3.md`（v1.9 定稿：定名 Eventboat、Apache-2.0、POC 阶段、不向后兼容 v2）
评审方法：全文逐节细读 + 依赖假设逐项对照 pkg.go.dev 官方文档核实 + 与 `review-2026-08.md`（v2 教训清单）交叉对照 + M1 可行性推演。本报告沿袭 `design-review.md` / `review-2026-08.md` 的评审文件惯例。

## 总体结论

**通过，无阻塞项。** 可以进入实现。

- 发现 **0 项阻塞**（无设计自相矛盾、无不可行项、无依赖假设错误到推翻设计的程度）；
- **2 项中等**（R1/R2：go-starlark 沙箱机制表的归属表述不准，且照表实现会让文档自己的示例无法编译——实现侧修正即可，不动设计结论）；
- **11 项低**（缺口类：文档未覆盖但 M1 必须决策的细节，逐条给出建议决策，实现按建议执行并记录）。

设计本身的三块承重墙经核查全部成立：三段式拓扑与命名体系全文自洽；spool/settle/checkpoint/死信四方语义在 §5.8/§6.2/§6.3 间无矛盾；七条不变量相互独立、可各自成测试。

---

## 一、依赖假设核查（对照 pkg.go.dev，2026-09-03）

| 文档假设 | 核查结果 | 判定 |
|---|---|---|
| `Thread.SetMaxExecutionSteps(n)` 作为步数预算 | 存在：`func (thread *Thread) SetMaxExecutionSteps(max uint64)`，配套 `ExecutionSteps()`；另有 `Cancel(reason)` 可做 wall-clock 取消 | ✅ |
| 无异常模型：错误带 backtrace 行号交宿主 | 存在：`EvalError{Msg, CallStack}`，`Backtrace() string` 返回带行列的用户可读栈；`CallStack` 为 `[]CallFrame`（`Name` + `Pos syntax.Position`）可程序化提取行号 | ✅ |
| `resolve.AllowRecursion = false`（默认）禁递归 | 存在且默认 false，但**只管递归函数**（见 R1） | ⚠️ 表述偏差 |
| `while` 循环"挂在 recursion 选项下，默认禁" | 默认禁正确，但归属**错误**：`while` 由 `AllowGlobalReassign`（旧全局选项）或 `syntax.FileOptions.While`（新 API）控制，与 `AllowRecursion` 无关（见 R1） | ⚠️ R1 |
| 文档示例脚本可在"仅 AllowRecursion=false"下编译 | **不能**：§4.3/§5.8 示例全部使用**顶层 `if/else`**，标准 Starlark 方言默认禁止顶层控制流，需 `FileOptions.TopLevelControl=true`（见 R2） | ⚠️ R2 |
| 模块白名单 `json`/`math`/`strings` | `lib/json`、`lib/math` 存在且可白名单；**`strings` 不是可 load 的模块**——字符串方法是 `string` 类型内建方法，天然可用且无法（也无需）禁用（见 R3） | ⚠️ R3 |
| `math` 为确定性子集 | `lib/math` 仅含常量与纯函数（abs/ceil/floor/sqrt 等），无 random/时间 | ✅ |
| `SourceProgram` 预编译一次、程序不可变多线程复用 | `Program` 文档明示"immutable, contain no Values"；当前版本 `SourceProgram` 已标 deprecated，推荐 `SourceProgramOptions`（带 `*syntax.FileOptions` 参数）——假设方向正确，实现用新 API（见 R4） | ✅（用新 API） |
| `Thread.Load` 做宿主白名单加载 | 存在：`Thread.Load func(thread *Thread, module string) (StringDict, error)` 回调，非白名单模块返回错误即可 | ✅ |
| resolver 编译期抓未定义名/arity 错误 | `SourceProgram` 的 `isPredeclared` 回调 + resolve 阶段报 `undefined: x`；函数 arity 在编译期检查 | ✅ |
| cel-go 谓词编译求值、零自定义函数 | v2 已用 cel-go v0.22.1 验证过集成路径；环境声明 `payload`/`meta`/`constants` 为变量即可，最新 v0.32.0 可用 | ✅ |
| modernc.org/sqlite 适用性 | v1.58.0（SQLite 3.53.4），纯 Go 无 CGO、Windows amd64 官方支持、WAL 经 DSN `_pragma=journal_mode(WAL)`、driver 名 `"sqlite"` | ✅ |
| JSON Schema 注册强校验 | go-starlark 之外需一个 JSON Schema 校验库；选 `github.com/santhosh-tekuri/jsonschema/v6`（v6.0.3，支持 2020-12，纯 Go） | ✅（补充选型） |

**网络与版本核实记录**（goproxy.cn 实测可下载）：go.starlark.net `v0.0.0-20260828210309-6dd8f160a37f`、modernc.org/sqlite `v1.58.0`、santhosh-tekuri/jsonschema/v6 `v6.0.3`、cel-go `v0.32.0`、kafka-go `v0.4.51`、yaml.v3 `v3.0.1`。

## 二、问题清单

严重度：🔴 阻塞 / 🟡 中（实现必须处理，不改设计结论）/ 🔵 低（缺口补决策）。

### R1 🟡 沙箱表"`while` 挂在 recursion 选项下"机制归属错误

- **证据**：§4.3 沙箱表第二行；pkg.go.dev/go.starlark.net/resolve 的选项注释——`AllowGlobalReassign = false // allow reassignment to top-level names; while loops; and if/for/while at top-level`，`AllowRecursion = false // allow recursive functions`；deprecation 注记明说 `AllowGlobalReassign` 统控 FileOptions 的 `While`/`TopLevelControl`/`GlobalReassign` 三项。
- **影响**：默认结果（递归禁、while 禁）碰巧一致，但照表实现者会去动错误的开关。
- **建议处理**：实现统一用 `syntax.FileOptions{While: false, Recursion: false, TopLevelControl: true, GlobalReassign: false}`（TopLevelControl 见 R2）；文档下一版修正该行。

### R2 🟡 文档示例脚本依赖顶层控制流，标准方言默认拒绝

- **证据**：§4.3 示例 `if payload.total > 10000:` 与 §5.8 示例 `if not payload.region:` 均为**顶层语句**；resolve 文档：顶层 if/for/while 的可用性由 `AllowGlobalReassign`（旧）/`FileOptions.TopLevelControl`（新）控制，标准 Starlark（Bazel 方言）默认不允许。
- **影响**：若实现只按沙箱表设置 `AllowRecursion=false` 而不启用 `TopLevelControl`，设计文档自己的全部示例脚本都无法编译——这是"文档假设的机制"与"文档给出的用法"之间的实际不一致。
- **建议处理**：实现启用 `TopLevelControl=true`（`script` 是语句序列而非函数体，本就需要）；同时保持 `While=false`、`Recursion=false`、`GlobalReassign=false`。终止性不受影响（for 只能迭代有限序列 + 步数预算兜底）。文档下一版在沙箱表补一行"顶层控制流：启用（script 为语句序列）"。

### R3 🔵 模块白名单中的 `strings` 不存在

- **证据**：go.starlark.net 包列表只有 `lib/json`、`lib/math`、`lib/proto`、`lib/time`；字符串操作（split/upper/replace…）是 `string` 类型内建方法。
- **建议处理**：白名单实现为 `{json, math}`；"strings 能力"通过内建方法天然具备，在 README 说明。

### R4 🔵 `SourceProgram` 已 deprecated

- **证据**：pkg.go.dev 标注 `Deprecated: use SourceProgramOptions`（新签名带 `*syntax.FileOptions`）。
- **建议处理**：实现用 `SourceProgramOptions`；"预编译 + Program 不可变复用"的核心假设不变。

### R5 🟡（缺口）插件名即键 × node 层框架字段白名单存在撞名面

- **证据**：§5.3/§5.6 字段分层规则成立的前提是插件名永不与 `from/decoder/encoder/workers/order_key/batch`（node 层）撞名；设计未规定裁决规则。内置插件名（kafka/http/http_server/cron/file/drop/sql）当前无冲突，但协议上没有防线——这正是 v2"保留字与插件字段撞名"问题的残余面。
- **建议处理**：registry 拒绝注册名为框架保留字的插件（verify 期兜底）；写入实现记录。

### R6 🔵（缺口）transform 执行失败的重试策略归属未指明

- **证据**：§4.3"求值错误 = 中止 = 该消息沿边 `delivery` 走重试 → 死信"——transform 有多条入边/出边时，"沿边"指哪条未定义。
- **建议处理**：transform 执行失败按**入边** delivery（多入边取最严：retries 最大者；edge_defaults 兜底）重试，耗尽死信；POC 单入边场景直接用入边策略。

### R7 🔵（缺口）fan-out 零匹配行为未定义

- **证据**：§6.2 settle 跟踪只列三种分支终态；若某消息在某节点所有出边 `when` 均不匹配且无条件兜底边，文档未说 settle 还是死信。静默丢弃会重蹈 v2"静默失败"覆辙。
- **建议处理**：零匹配 = 正常 settle（过滤是条件路由的本意）+ 计数指标 `eventboat_fanout_no_match_total`；verify 语义 lint 对"全条件出边、无兜底"发 warning（POC 实现指标，lint 记裁剪）。

### R8 🔵（缺口）`split` transform 语义全文未定义

- **证据**：§5.1/任务范围均把 `script|split` 列为 transform 主字段，但全文唯一提及 split 语义处是 §5.8 `emit: page 配 transform.split` 一句。
- **建议处理**：POC 定义——split 将**数组型 payload** 拆为逐元素消息（元素为新 payload，meta 继承并深拷贝），子消息 settle 归并到父消息（父消息在全部子消息 settle 后 settle）。

### R9 🔵（缺口）开放问题 #6（spool 存原始字节 vs 解码形态）M1 必须落决定

- **建议处理**：按文档倾向：**原始字节 + codec 名标记**（回放兼容 codec 升级；§3.3 replay 语义需要）。解码在 DAG 执行期进行。

### R10 🔵（缺口）M1 验收项与任务 POC 范围的差异需记档

- **证据**：§7.4 M1 验收含"conformance 语料进 CI"与 `repl`/`plugin` 命令；任务 POC 范围 = verify/test/run + 基本插件 + 三 example。
- **建议处理**：conformance 语料与 repl/plugin 命令裁剪并记入 README；保留最小基准（celhost/starhost Go benchmark）以部分兑现 §4.6 基准纪律。

### R11 🔵（缺口）部署级配置形态未定（开放问题 #10）

- **建议处理**：POC 用 CLI 标志替代：`--data-dir`（SQLite 存储目录）、`--ephemeral`（内存态，本地开发/测试，§6.3 已列此标志）。

### R12 🔵 YAML "1.2" 声明 vs gopkg.in/yaml.v3 实际（1.1 语义为主）

- **建议处理**：yaml.v3 是 Go 生态事实标准且 v2 已用；JSON 文档作为 flow-style 子集可解析。无动作，记档。

### R13 🔵 不变量 6 的测试形态需明确

- **证据**：§6.2 不变量 6 本质是"文档化用户责任 + meta.message_id 供幂等键"。
- **建议处理**：专属测试断言"同一消息重复投递/重放时 `meta.message_id` 保持稳定且 sink 收到两次相同内容"——把用户责任的前提（幂等键可用）变成机器可验证。

## 三、一致性核查

1. **§5.1 命名决定 ↔ 全文示例**：逐处抽查 `edge_defaults`/`constants`/`parameters`/`when`/`route`/`delivery`/`required`/`dlq`/`max_in_flight`/`catchup_window`/`apiVersion: eventboat/v3`/`hooks`/`telemetry`——§5.3、§5.4、§5.7、§5.8、§5.10、§3.6 CLI 标志（`--parameters`、`replay --dlq`）相互一致；v1.5/v1.6 全称原则的回退项（dlq/args/dsn 保留）在 §5.10 与 §3.6 用法统一。未发现残留旧名。
2. **settle/spool/checkpoint/死信跨节自洽**：§6.2"死信写入成功 = 分支终态" ↔ 不变量 4"死信写入失败不得 settle"（正反两面）；§6.2"checkpoint = settled 消息的 spool 位点" ↔ 不变量 2；§5.8"水位只推进到已 settle 消息的 max(cursor_column)" ↔ 不变量 7（作业版同一原理）；§6.3 死信表字段（原始消息+错误+backtrace）↔ §4.3 错误模型。无矛盾。
3. **七条不变量独立可测**：1（spool 先于可见）/2（settle 先于 checkpoint）/3（kill-9 重放 ⊇ 未 settle）/4（死信写失败阻塞 settle）/5（required:false 隔离）/6（幂等键稳定）/7（水位 ≤ settled max）——无一条可由另一条推导，每条可写专属测试（测试名 `TestInvariant_*` 可检索）。
4. **§5.7 before/after 对照**与 §5.3 唯一写法一致；合体 step 在三段式下物理不可表达（结构杜绝）属实。
5. **v2 教训对照**（review-2026-08 三大主题 → v3 结构性回应）：
   - at-least-once 纸面承诺（WAL offset 先推进/CRC 缺失/双重 Ack）→ 入口持久化 + settle + 不变量逐条测试，且"不手写 WAL"直接移除被证伪的机制类别；
   - 文档口径脱节（WASM 空壳、validate 不做承诺校验）→ 注册强制 schema（schema 不存在 = verify 失败）、conformance 测试锁语义（R10 记档裁剪）；
   - 静默失败遍布（未知字段忽略、HOCON 回零、EQL 边界静默）→ 未知字段一律报错、`${VAR}` unset 报错、CEL/Starlark 错误契约明文化。
   - 判定：v3 对 v2 缺陷类别的规避成立。

## 四、M1 可行性核查

| M1 项 | 实现路径 | 判定 |
|---|---|---|
| celhost | cel-go `NewEnv(Variable("payload", DynType)…)` → `Compile` → 逐消息 `Eval`；求值 error → false + 计数（§4.2 错误契约） | ✅ v2 有先例 |
| starhost | `SourceProgramOptions(FileOptions{TopLevelControl:true,…})` 预编译；`predeclared` 注入自定义 `msgValue`（惰性 + COW，实现 `HasSetField/HasSetIndex/IterableMapping`）；`Thread.Load` 白名单；`SetMaxExecutionSteps(100k)`；`EvalError.Backtrace()` → 死信 | ✅（R1/R2 修正后） |
| engine+store | 内存 DAG + settle 计数器；SQLite 四表（spool/checkpoint/dead_letter + 预留 job_history）；`database/sql` + modernc 驱动；WAL 经 DSN | ✅ |
| registry | 编译期注册（name/kind/schema/factory）；config 加载后、verify 时按 schema 校验插件块（santhosh-tekuri v6）；保留字撞名防线见 R5 | ✅ |
| config | yaml.v3 → `map[string]any` → `${VAR}/${?VAR}` 树遍历替换 → 严格 decode（KnownFields 语义 + 手工白名单） | ✅ |
| verify/test CLI | §3.1 六类检查静态可实现；§3.2 合约测试格式（suite/pipeline/cases/inject/expect）信息充足 | ✅ |

## 五、分流结论

无阻塞项 → 本报告记档，按第一步（归档 v2）继续实现。R1/R2 在 starhost 实现中修正；R5–R11 作为"文档未覆盖细节"按本报告建议决策执行并记录于 commit message / README。

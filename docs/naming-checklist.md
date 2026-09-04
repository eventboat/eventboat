# Eventboat 定名前置清单（§8.4 研究性义务执行记录）

日期：2026-09-04（复查基线：2026-09-03 定名核查，redesign-v3.md §8）
性质：**只调研、不花钱、不做不可逆动作**。域名注册、商标申请、索引请求等全部留给用户，
见第五节清单。

## 一、结论（TL;DR）

§8.4 四项研究性义务**全部绿灯**（2026-09-04 复查）：

| §8.4 项 | 复查结果 | 判定 |
|---|---|---|
| ① 软件/GitHub 空间占用 | 无新占用者。全网检索仅旅游语料（波兰派对游船公司 Facebook 页、布达佩斯晚餐游船 #eventboat 标签）；无任何同名软件/库/产品 | ✅ 干净 |
| ② GitHub org `eventboat` | **已是我们自己的 org**（github.com/eventboat/eventboat，本仓库；§8.4 的"可用性"问题已被创建动作解决） | ✅ 已占有 |
| ③ 包名注册名 | npm `eventboat` 404（未注册）；crates.io 404（未注册）；PyPI 404（未注册）；pkg.go.dev 模块路径 = 本仓（尚未被索引——正常，见第五.3） | ✅ 四处干净 |
| ④ 三模型实测 | 1/3 已测（GLM，2026-09-04：零软件先验）；另两模型文案已备好（第四节） | 🟡 差 2/3，用户 5 分钟 |

eventboat.ch 仍为瑞士马焦雷湖游船旅游公司（DNS 解析正常、站点在线；不同行业、不同商标类别）。  
eventboat.io / eventboat.dev / eventboat.sh 三域名 **DNS 均无委派记录（NXDOMAIN）**——与 2026-09-03
"全部未注册"结论一致；注册级确认是购买时动作（NXDOMAIN 不等于法律上未注册，极小概率存在
"注册了但零记录"的域名）。

## 二、复查明细（2026-09-04）

1. **软件空间**：web 检索 `"eventboat"` + software/github/npm/package 组合——无同名软件。
   命中的相邻项全是噪声：npm/GitHub 的 `event-bus` 家族、Eventbrite、EventBridge（阿里云）。
   语料积累情况：GitHub 上 `eventboat` 命名空间只有本仓（0 star，2026-09 起）——**发布后
   语料独占正在按 §8.3 判据 ② 的预期建立**。
2. **eventboat.ch**：解析到 168.119.8.226，站点在线（对爬虫返回 403，人工可访问）。仍是
   游船旅游公司。商标类别不同（旅游服务 vs 软件，Nice 39/43 vs 9/42），无冲突路径。
3. **包名**：
   - npm：`https://www.npmjs.com/package/eventboat` → 404（未注册）。
   - crates.io：`https://crates.io/api/v1/crates/eventboat` → 404（未注册）。
   - PyPI：`https://pypi.org/pypi/eventboat/json` → 404（未注册）。
   - pkg.go.dev：`github.com/eventboat/eventboat` 是本模块路径；pkg.go.dev 尚未索引
     （正常：模块被代理抓取/首次请求后才会出现）。**不影响定名，只影响可见性**。
4. **域名**：`nslookup` 三域名均 NXDOMAIN（.io/.dev/.sh）。`.dev` 注册后强制 HTTPS
   （HSTS preload TLD）——对我们的用法无影响（会配证书），购置时知晓即可。

## 三、商标调研（软件类，美/欧/中）——途径与费用

**建议申请类别（Nice 分类）**：第 9 类（可下载软件）+ 第 42 类（SaaS / 平台即服务 /
软件设计开发）。开源数据面项目最贴切的是 9 + 42 双类；预算紧则先 9 类。

**检索途径（全部免费，申请前必做）**：

| 地区 | 官方检索入口 | 说明 |
|---|---|---|
| 美 | USPTO Trademark Search：https://tmsearch.uspto.gov | 免费；查字标 "EVENTBOAT"（含近似音/形） |
| 欧 | EUIPO eSearch：https://www.euipo.europa.eu/eSearch | 免费；覆盖 EU 商标 + 国际注册指定 EU |
| 中 | 中国商标网商标查询：https://sbj.cnipa.gov.cn | 免费；选"商标近似查询"，国际分类 9/42 |
| 全球兜底 | WIPO Global Brand Database：https://branddb.wipo.int | 免费；Madrid 体系国际注册一键扫 |

**官费（2026-09-04 核查；申请费随时可能调整，提交前以官网当日为准）**：

| 地区 | 官费 | 备注 |
|---|---|---|
| 美（USPTO） | **$350/类**（base application，2025-01-18 起 TEAS Plus/Standard 两档合并为单档）；用 ID Manual 现成表述=$350，基于 ID Manual 改写 +$100，自由文本 +$200 | Madrid 66(a) 途径 $600/类；建议用 ID Manual 标准表述锁定 $350 |
| 欧（EUIPO） | **€850 首类 + €50 第二类 + €150/类（第三类起）** | 电子申请价；9+42 双类 = €900 |
| 中（CNIPA） | **¥270/类（网上申请，限 10 个商品/服务项，超出每项 +¥30）**；纸质 ¥300/类 | 通过商标网上服务系统提交；代理费另计（通常 ¥300–800/类） |
| 国际（可选） | Madrid 体系：基础费约 653 瑞郎 + 指定国费用（各国不同） | 详见 https://www.wipo.int/madrid/fees；已有本国基础申请即可延伸，不必一开始就走 |

**建议路径（供参考，非法律意见）**：先跑四站检索（0 成本）→ 无冲突则按 **CN(9+42) ≈ ¥540
+ US(9) $350 + EU(9) €850** 优先级提交（团队在中国、用户面全球：本国 + 两个最大软件市场）。
检索如有近似（如 eventboat.ch 在旅游类注册了 EVENTBOAT 字标）不影响软件类申请——类别不同。

## 四、三模型实测问卷（§8.4 ③；现成文案）

**目的**：确认主流 LLM 对 "eventboat" 零软件先验（§8.3 判据 ②"LLM 语境干净"）。
**做法**：把下面文案原样发给三个不同厂商的模型（建议：GPT / Claude / Gemini 最新版），
把回答要点记进文末表格。

**英文文案（推荐，直接粘贴）**：

```text
Answer these three questions without searching the web, from your training
knowledge only:

1. What is "eventboat"? Name any product, company, library, or open-source
   project you know by this exact name.
2. Have you ever seen the token "eventboat" in software documentation,
   package registries (npm, PyPI, crates.io, Maven), or API references?
3. In one sentence, what would you guess a software project named
   "eventboat" does, based on the name alone?

If you have no prior knowledge, say so explicitly — that is a valid and
useful answer.
```

**中文文案（备选）**：

```text
请不联网、仅凭你的训练知识回答三个问题：
1. "eventboat" 是什么？你知道任何叫这个名字的产品、公司、软件库或开源项目吗？
2. 你在软件文档、包管理器（npm/PyPI/crates.io/Maven）或 API 参考里见过 eventboat
   这个词吗？
3. 只凭名字猜：一个叫 eventboat 的软件项目是做什么的？
如果完全没有先验知识，请明确说"不知道"——这也是有效答案。
```

**判定标准**：问题 1/2 的理想回答是"不知道/无先验"；问题 3 的理想回答方向是
"事件路由/消息传递/数据管道"（字面复合词的正确猜读）。任一模型报出真实同名软件 →
停下，回到 §8.2 台账重审。

**已测记录**：

| 模型 | 日期 | 问题 1/2（先验） | 问题 3（猜读） | 判定 |
|---|---|---|---|---|
| GLM（本项目编码模型） | 2026-09-04 | 零软件先验；仅字面复合词与旅游语料（eventboat.ch、游船 hashtag） | "事件传递/路由类基础设施"方向 | ✅ 通过 |
| （用户执行）GPT | | | | |
| （用户执行）Claude | | | | |
| （用户执行）Gemini | | | | |

## 五、人工动作清单（用户执行；本轮一律不做）

按优先级排序：

1. **三模型问卷**（5 分钟，0 成本）：第四节文案发三个模型，结果回写第四节表格。
2. **域名注册**（~$60–100/年·三域名）：在任一注册商（Cloudflare Registrar 成本价 /
   Porkbun / Namecheap）确认可注册后**同时购入 eventboat.io / eventboat.dev /
   eventboat.sh**（§8.4 原案；防抢注优先级高于商标——域名被占的救济成本高于商标）。
   主站建议 .io（行业习惯），.dev 留文档站，.sh 留短链/CLI 帮助页。
3. **pkg.go.dev 索引**（0 成本，打 tag 后）：v0.1.0-beta 发布后访问
   https://pkg.go.dev/github.com/eventboat/eventboat ——通常首次抓取后自动出现；
   若 48h 未出现，页面上的 "Request indexing" 按钮触发。
4. **商标检索四站**（30 分钟，0 成本）：第三节表格逐站查 "EVENTBOAT"（含近似），
   结果回写本文件。
5. **商标申请**（视检索结果，预算见第三节）：建议顺序 CN(9+42) → US(9) → EU(9)；
   预算紧先 CN+US。可自行网申（CNIPA 网上服务系统 / USPTO TEAS / EUIPO eSearch 提交），
   或委托代理（代理费另计）。
6. **防御性包名占位**（可选，0–成本极低）：npm/PyPI/crates.io 目前全空。当前无
   JS/Python/Rust 分发物——若担心抢注可在发版同期注册 `eventboat` 占位包（README 指
   回本仓）；不急。
7. ~~GitHub org~~：已完成（org `eventboat` 即本仓所在，无需动作）。
8. ~~仓库与 module path 更名~~：已完成（github.com/eventboat/eventboat，go.mod 一致）。

## 六、判据复核（对照 §8.2/§8.3）

- ① 全球唯一可注册：✅（软件空间零占用 + 三域名无委派 + 四包名空位）
- ② LLM 语境干净：🟡 1/3 模型已验，余两待用户执行（第四节）
- ③ 易读无歧义：✅（未变）
- ④ 隐喻贴路由：✅（未变）

**总判定：无阻塞。** 唯一悬置是三模型问卷的剩余两份——不阻塞任何代码/发版工作，
仅阻塞"购域名/提商标"这些不可逆动作（本就留给用户）。

---

复查来源（2026-09-04）：npm/crates.io/PyPI/GitHub 直接探测；nslookup 三域名；
[USPTO Trademark fee information](https://www.uspto.gov/trademarks/trademark-fee-information)、
[EUIPO Fees and payments](https://www.euipo.europa.eu/en/trade-marks/before-applying/fees-payments)、
[CNIPA 注册商标费用答复](https://www.cnipa.gov.cn/jact/front/mailpubdetail.do?transactId=480318&sysid=13)、
[通辽市监局知识产权问答（网申 ¥270/类）](https://amr.tongliao.gov.cn/ztzl/zscq/xwzx/202604/t20260427_1045374.html)。

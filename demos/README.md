# 12 阶段 Agent Mock 参考实现（供具体实现人员对照）

一条 CI 失败事件从「巡检发现」走到「结案报告」要经过 **12 个阶段、两条分支、三道闸门**。
这里的 12 个目录就是这 12 个阶段的**可独立运行的最小样例**：每个目录给一份真实下发的
`input.json`、一份该阶段的 `output.json`，加一篇说明「这个阶段凭什么这么判」。

Mock 只是**数据与判定规则的替身**，接口（消息格式、字段名、路由键、闸门语义）是生产口径。
实现人员接真实 CI / 真实 LLM / 真实工单系统时，只需替换 playbook 内部逻辑，
**输入输出契约不要动**——编排层按契约校验，改字段名会直接 `schema_invalid` 卡住。

## 1. 目录与阶段映射

| 目录 | 阶段 | Agent : mode | step id | 消息 | 主要出现在 |
|---|---|---|---|---|---|
| `01-monitor/` | ① 巡检发现 | monitor : scan | `scan` | `ci.cmd.monitor.scan` | 两条分支共用 |
| `02-triage/` | ② 分诊定级 | triage : classify | `t1` | `ci.cmd.triage.classify` | 两条分支共用 |
| `03-gate1/` | ③ **闸门 1**（走哪条分支） | reviewer : gate1 | `g1` | `ci.cmd.reviewer.gate1` | 两条分支共用 |
| `04a-remediation/` | ④a 内部修复 | remediation : fix | `r1` | `ci.cmd.remediation.fix` | internal |
| `05a-verify/` | ⑤a 修复验证 | reproduction : verify | `v1` | `ci.cmd.reproduction.verify` | internal |
| `06a-gate2/` | ⑥a **闸门 2**（修复验收） | reviewer : gate2 | `g2` | `ci.cmd.reviewer.gate2` | internal |
| `04b-evidence/` | ④b 受控复现 | reproduction : evidence | `e1` | `ci.cmd.reproduction.evidence` | external |
| `05b-regression-check/` | ⑤b 回归判定 | regression : check | `c1` | `ci.cmd.regression.check` | external |
| `06c-bisect/` | ⑥c guilty patch 定位 | regression : bisect | `b1` | `ci.cmd.regression.bisect` | external（且是回归） |
| `07b-ticketing/` | ⑦b 建单 | ticketing : create | `k1` | `ci.cmd.ticketing.create` | external |
| `08b-gate3/` | ⑧b **闸门 3**（建单验收） | reviewer : gate3 | `g3` | `ci.cmd.reviewer.gate3` | external |
| `09-reporting/` | ⑨ 结案报告 | reporting : case | `p1` | `ci.cmd.reporting.case` | 两条分支共用 |

`09-reporting` 另有 `daily` 模式（日报，`ci.cmd.reporting.daily`），不在单 Case 链路上。

## 2. 两条分支

```
                     ┌─────────────────────────────── internal（自家 CI / 环境 / 工具链）
 ①scan ── ②classify ─ ③gate1 ┤                                  ④a fix ─ ⑤a verify ─ ⑥a gate2 ─┐
                     └─────────────────────────────── external（产品缺陷）
                                                                 ④b evidence ─ ⑤b check ─〔回归?〕─ ⑥c bisect ─ ⑦b create ─ ⑧b gate3 ─┤
                                                                                                                    └──────────────────────────┴─ ⑨reporting
```

- 分支由 **② 分诊预判**、**③ 闸门 1 决策**：`decision=internal|external`，
  闸门 1 可以推翻分诊的预判（`03-gate1/input.internal.json` 与 `input.json` 就是同一套字段的两个结论）。
- `⑥c bisect` 只在 ⑤b 判定「是回归」时才跑；不是回归就直接去 ⑦b。
- 三条闸门的 `verdict` 都是 `approve | reject | needs_info | rework`，
  分别对应「放行 / 终止 / 打回上游补信息 / 回炉重做」。

## 3. 三个文件

| 文件 | 作用 |
|---|---|
| `input.json` | 该阶段**收到**的 `payload.input`，由编排层 `graph._build_input` 生成 |
| `output.json` | 该阶段**返回**的 `payload.result`，必须通过 `stage-contracts.json` 校验 |
| `README.md` | 判定规则、字段含义、易错点 |

带 `*.internal.json` 的目录（`02-triage`、`03-gate1`）是同一个阶段的另一分支样例。

## 4. 怎么跑

```bash
# 列 12 个阶段
python demos/harness.py --list

# 跑完整两条分支（约 2 秒，不需要 RabbitMQ / Postgres）
python demos/harness.py --all

# 只跑一个阶段（会自动回放它之前的所有阶段，再用本目录 input.json 覆盖输入）
python demos/harness.py 03-gate1
python demos/harness.py 03-gate1 --chain internal

# 改了 input.json 想刷新 output.json
python demos/harness.py --write
```

`harness.py` 不做任何「模拟」：它直接调用生产代码
`iqe_agents.<stage>.playbook.handler`，用 `iqe_agents.runtime.Ctx` 构造上下文，
DB 换成内存替身 `iqe_agents.tools.selfcheck.FakeDB`。**跑通 demo ≠ 跑通集群**，
它证明的是「剧本逻辑 + 契约」没问题，不覆盖消息总线、重试、DLQ。

## 5. 输入从哪来（改契约前必读）

`input.json` 逐字段对齐 `orchestrator/src/iqe_orchestrator/graph.py::_build_input`：

- **公共底座**：`case_id / build / source / job`（来自 Case 与 Monitor 观测）；
- **②之后所有阶段**都带分诊结论 `rca / component / signature / key_frames`；
- **③reviewer** 另加 `signal / path / confidence / evidence_count / loop_seq`，
  `gate2` 再挂 `remediation` + `reproduction(verify)`，
  `gate3` 再挂 `reproduction(evidence)` + `regression` + `ticketing`；
- **⑤b regression** 挂 `reproduction(evidence)`，`bisect` 另加 `baseline / change_window`；
- **⑦b ticketing** 挂 `reproduction(evidence)` + `regression` + `duplicate_of`；
- **⑨reporting:case** 只有 `{case_id, day}`——报告内容自己去 DB 汇总。

上游没跑过的字段就是 `null`，不要在下游「猜一个默认值」糊过去：闸门会把缺字段判成
`*_missing` 直接 `rework`，这正是设计意图。

## 6. 输出契约在哪里

`agents/src/iqe_agents/common/data/schemas/stage-contracts.json`（权威）。
必填字段极少（`case_id` + 每阶段 1~3 个关键字段），其余是可选但**闸门会读**的字段，例如：

- `reviewer` 的 `checks[].id` 是闸门规则的唯一标识，改名等于改规则；
- `reproduction` 的 `fix_verified` 只有 `verify` 模式有值，`evidence` 模式必须为 `null`；
- `regression` 的 `guilty_patch` 未收敛时必须为 `null`，不能给空对象。

## 7. 从 demo 到真实实现

| 环节 | Mock 现在怎么做 | 接真实系统时替换成 |
|---|---|---|
| Monitor 数据源 | 读 `fixtures/ci_feed/*.json` 夹具 | Jenkins / 构建系统 API + 日志指纹 |
| Triage 判定 | 组件→根因表 + 知识库引用 | LLM 分类 + 历史签名匹配 |
| Remediation | 固定脚本改动 + ADO 工作项 | 真实补丁生成 / 变更评审 |
| Reproduction | 伪造 attempts / matrix | 真机锁机、跑用例、抓 dmesg |
| Regression | 固定基线 + 固定 guilty commit | 真实基线库 + `git bisect` |
| Ticketing | jira-mock | Jira / ADO API |
| Reviewer | 机械 checks（可复算） | **保持机械**，最多把 `quality_scores` 换成 LLM 评分 |
| Reporting | 模板渲染 | 同左，指标口径不要改 |

闸门（reviewer）建议长期保持「规则可复算」：出问题时要能回答「为什么放行」，
LLM 只能用来给 `quality_scores` 打分，不能直接决定 `verdict`。
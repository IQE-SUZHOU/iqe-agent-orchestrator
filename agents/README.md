# iqe-agents —— CI 分诊的 8 个 Mock Agent

8 个独立消费者承载流程图的 12 个阶段：`monitor` / `triage` / `reviewer` / `remediation` /
`reproduction` / `regression` / `ticketing` / `reporting`。每个 Agent 只做三件事：
**订阅自己的命令队列 → 按 `payload.mode` 跑对应剧本 → 把 result 发回总线**。
它不决定下游是谁（星型拓扑，编排层决定），也不暴露业务 HTTP，只有 `/healthz`。

本目录是一个**自包含 Python 项目**：`pyproject.toml`、`src/`、`tests/`、`MANIFEST.in` 齐全，
可以单独 `pip install`、单独打镜像；与仓库里的 [`orchestrator/`](../orchestrator/README.md) 无代码依赖。

## 安装与运行

```bash
pip install -e ./agents               # 只装 Agent 侧
ci-agent-monitor                      # 每个 Agent 一个 console script
ci-agent-reviewer                     # （线上 = 8 个 Deployment，同一镜像换 command）
ci-selfcheck                          # 离线自检：144 项断言，不连 MQ/DB
```

| console script | 模块 |
|---|---|
| `ci-agent-monitor` | `iqe_agents.monitor.main` |
| `ci-agent-triage` | `iqe_agents.triage.main` |
| `ci-agent-reviewer` | `iqe_agents.reviewer.main` |
| `ci-agent-remediation` | `iqe_agents.remediation.main` |
| `ci-agent-reproduction` | `iqe_agents.reproduction.main` |
| `ci-agent-regression` | `iqe_agents.regression.main` |
| `ci-agent-ticketing` | `iqe_agents.ticketing.main` |
| `ci-agent-reporting` | `iqe_agents.reporting.main` |
| `ci-selfcheck` | `iqe_agents.tools.selfcheck` |

## 8 个 Agent × 13 个 mode

`stage-contracts.json` 是 mode 归属与出入参形状的唯一权威；runtime 收到不属于本 stage 的 mode 直接进 DLQ。

| stage | 命令 subject | 结果 subject | 阶段 | 干什么 |
|---|---|---|---|---|
| `monitor` | `ci.cmd.monitor.scan` | `ci.result.monitor` | 1 | 扫 CI feed，算失败指纹与签名族，抑制已知噪声，判定 `signal=none/error` |
| `triage` | `ci.cmd.triage.classify` | `ci.result.triage` | 2 | 分类组件/类型，检索知识库，判 internal/external，给出 `duplicate_of` 与可复现性 |
| `reviewer` | `ci.cmd.reviewer.gate1\|gate2\|gate3` | `ci.result.reviewer` | 3 / 6a / 8b | 三道闸门：闸门 1 批分流、闸门 2 批修复、闸门 3 批工单；可 `needs_info`/`rework`/`reject` |
| `remediation` | `ci.cmd.remediation.fix` | `ci.result.remediation` | 4a | 采纳知识库修复方案，产出 change_type / target / diff / ADO 工作项 |
| `reproduction` | `ci.cmd.reproduction.evidence\|verify` | `ci.result.reproduction` | 4b / 5a | `evidence`：锁机受控重跑给复现矩阵；`verify`：应用 4a 改动重跑给 `fix_verified` |
| `regression` | `ci.cmd.regression.check\|bisect` | `ci.result.regression` | 5b / 6c | `check`：是否回归；`bisect`：二分到可疑 commit/PR 并给置信度 |
| `ticketing` | `ci.cmd.ticketing.create` | `ci.result.ticketing` | 7b | 按段落组装工单、定优先级与负责人、查重（重复则关联既有单） |
| `reporting` | `ci.cmd.reporting.case\|daily` | `ci.result.reporting` | 9 | `case`：结案报告 + `kb_writes`；`daily`：日报（根因/终态/高频签名） |

三道闸门的结论词优先于剧本：低置信、验证失败、缺段落时**恒为** `needs_info`/`rework`，
夹具只能在「机械结论为 approve」时把结论改严（或 `force`），不能把该拒绝的改成通过。

## 模块地图

| 路径 | 职责 |
|---|---|
| `src/iqe_agents/runtime.py` | `AgentRuntime` 消费循环：订阅 → mode/形状校验 → 四层幂等 claim → 执行剧本 → 发布 result（等 confirm）→ ack；毒消息 nack 进 DLQ |
| `src/iqe_agents/<stage>/main.py` | 该 Agent 的进程入口：装配 Runtime + playbook 后用 console script 起 |
| `src/iqe_agents/<stage>/playbook.py` | 纯函数剧本：`(ctx) -> output`，mode 分派在剧本内部，可脱离总线单测 |
| `src/iqe_agents/triage/skills/*.md` | triage 的分类知识（环境/驱动/用例），剧本按指纹族引用 |
| `src/iqe_agents/common/` | 共享层副本（总线、DB、信封/幂等、契约、知识库、配置、健康探针、日志、Mock、夹具） |
| `src/iqe_agents/common/data/` | 与编排层同源的 `schemas/` `workflows/` `fixtures/`（契约唯一权威） |
| `src/iqe_agents/migrations/001_init.sql` | 与编排层同源 DDL（Agent 只写 `evidence` / `msg_dedup`） |
| `src/iqe_agents/tools/selfcheck.py` | 离线自检：13 个 mode × 场景 + 19 份 CI feed + 计划契约与语义 |

## 证据与写权

* Agent 只写 `evidence`（一条动作一条证据，`kind` 区分 `repro_log` / `repro_plan` / `scan` …）。
* 业务表（cases / stage_runs / reviews / remediations / verifications / regressions / tickets /
  kb_entries）全部由编排层**收到 result 后**写入，Agent 不碰。
* Reporting 想把经验写进知识库时，把条目放进输出的 `kb_writes`，由编排层落库。

## 幂等与可靠性

1. **消息级**：`msg_dedup`（`idempotency_key`）claim，重复投递直接 ack 不重跑。
2. **阶段级**：`stage_runs` 判 `already_done`，结果已存在则复用。
3. **副作用级**：工单/复现动作先查已存在的 backref。
4. **发布先于 ack**：publish 拿 publisher confirm 后才 ack；发布失败不 ack，让 broker redeliver。
5. **毒消息**：schema 不合法、mode 不属于本 stage 或 TTL 过期无法处理 → `nack(requeue=False)` → `ci.dlx` → `q.dlq`。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `AMQP_URL` / `PG_DSN` | 本地 guest / `ci_triage` | 总线与状态库 |
| `SVC` | `unknown` | 单元名（日志与指标维度，如 `agent-triage`） |
| `MOCK_SCENARIO` | `default` | 剧本分支（也可由命令的 `scenario_hint` 指定） |
| `MOCK_DELAY_MS` | `150:500` | 模拟耗时区间（`lo:hi`，毫秒） |
| `MOCK_FAILURE_RATE` | `0.0` | 注入随机失败，验证重试与 DLQ |
| `NOISE_THRESHOLD` | `0.85` | triage 噪声抑制阈值 |
| `REPRO_ENABLED` / `REPRO_MAX_RUNS` | `true` / `3` | 复现开关与重跑次数上限 |
| `TICKET_BACKEND` / `TICKET_AUTOCREATE` | `jira-mock` / `true` | 工单桩与是否自动建单 |
| `HTTP_PORT` / `LOG_JSON` / `LOG_LEVEL` | `8081` / `true` / `info` | 健康探针与日志 |

## 测试

```bash
cd agents && pytest -q        # 契约单测：stage-contracts、19 份 ci_feed、mode 校验
ci-selfcheck                  # 8 Agent / 13 mode 逐条断言（退出码 0 = 全绿）
```

`ci-selfcheck` 全程离线：把 `MOCK_DELAY_MS` 打到 0、失败率置 0，用内存替身 DB 收集证据与产物，
再逐条核对分流结论、闸门结论、工单优先级、报告段落与计划语义。

## 共享层

`src/iqe_agents/common/` 是 `orchestrator/src/iqe_orchestrator/common/` 的**字节级副本**。
**不要在这里手改共享层**——改真源后跑 `make sync-common`，否则
`tests/contract/test_common_parity.py` 会失败。

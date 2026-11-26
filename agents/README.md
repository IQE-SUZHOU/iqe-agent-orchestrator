# iqe-agents —— CI 分诊的 6 个 Mock Agent

六个独立消费者：`monitor` / `triage` / `reproduction` / `ticket` / `review` / `reporting`。
每个 Agent 只做三件事：**订阅自己的命令队列 → 跑剧本产出结构化结果 → 把 result 发回总线**。
它不决定下游是谁（星型拓扑，编排层决定），也不暴露业务 HTTP，只有 `/healthz`。

本目录是一个**自包含 Python 项目**：`pyproject.toml`、`src/`、`tests/`、`MANIFEST.in` 齐全，
可以单独 `pip install`、单独打镜像；与仓库里的 [`orchestrator/`](../orchestrator/README.md) 无代码依赖。

## 安装与运行

```bash
pip install -e ./agents               # 只装 Agent 侧
ci-agent-monitor                      # 每个 stage 一个 console script
ci-agent-triage                       # （线上 = 6 个 Deployment，同一镜像换 command）
ci-selfcheck                          # 离线自检，不连 MQ/DB
```

| console script | 模块 |
|---|---|
| `ci-agent-monitor` | `iqe_agents.monitor.main` |
| `ci-agent-triage` | `iqe_agents.triage.main` |
| `ci-agent-reproduction` | `iqe_agents.reproduction.main` |
| `ci-agent-ticket` | `iqe_agents.ticket.main` |
| `ci-agent-review` | `iqe_agents.review.main` |
| `ci-agent-reporting` | `iqe_agents.reporting.main` |
| `ci-selfcheck` | `iqe_agents.tools.selfcheck` |

## 六个阶段

| stage | 命令 subject | 结果 subject | 干什么 |
|---|---|---|---|
| `monitor` | `ci.cmd.monitor.scan` / `ci.cmd.monitor.run` | `ci.result.monitor` | 扫 CI feed，算失败指纹，判定 `signal=none/error`，产出 observation |
| `triage` | `ci.cmd.triage.run` | `ci.result.triage` | 分类、打标、判可复现性；`mode=lite` 时只分类不建 Case |
| `reproduction` | `ci.cmd.reproduction.run` | `ci.result.reproduction` | 受控重跑（Mock），给 `reproducible` 与复现证据 |
| `ticket` | `ci.cmd.ticket.run` | `ci.result.ticket` | 建工单（`TICKET_AUTOCREATE`），回 `ticket_status` |
| `review` | `ci.cmd.review.run` | `ci.result.review` | 复核：`approve` / `reject`（→ 回炉）/ `escalate` |
| `reporting` | `ci.cmd.reporting.case` / `ci.cmd.reporting.daily` | `ci.result.reporting` | 汇总 Case 证据出日报 |

## 模块地图

| 路径 | 职责 |
|---|---|
| `src/iqe_agents/runtime.py` | `AgentRuntime` 消费循环：订阅 → schema 校验 → 三层幂等 claim → 执行剧本 → 发布 result（等 confirm）→ ack；毒消息 nack 进 DLQ |
| `src/iqe_agents/<stage>/main.py` | 该 stage 的进程入口：装配 Runtime + playbook 后用 console script 起 |
| `src/iqe_agents/<stage>/playbook.py` | 纯函数剧本：`(ctx, cmd) -> result`，可脱离总线单测 |
| `src/iqe_agents/triage/skills/*.md` | triage 的分类知识（环境/驱动/用例），剧本按指纹族引用 |
| `src/iqe_agents/common/` | 共享层副本（总线、DB、信封/幂等、配置、路径、健康探针、日志、Mock、夹具） |
| `src/iqe_agents/common/data/` | 与编排层同源的 `schemas/` `workflows/` `fixtures/`（契约唯一权威） |
| `src/iqe_agents/migrations/001_init.sql` | 与编排层同源 DDL（Agent 只会写 `evidence` / `msg_dedup`） |
| `src/iqe_agents/tools/selfcheck.py` | 离线自检：6 剧本 × 场景 × 19 份 CI feed × 计划契约 |

## 幂等与可靠性

1. **消息级**：`msg_dedup`（`idempotency_key`）claim，重复投递直接 ack 不重跑。
2. **阶段级**：`stage_runs` 判 `already_done`，结果已存在则复用。
3. **副作用级**：工单/复现动作先查已存在的 backref。
4. **发布先于 ack**：publish 拿 publisher confirm 后才 ack；发布失败不 ack，让 broker redeliver。
5. **毒消息**：schema 不合法或 TTL 过期无法处理 → `nack(requeue=False)` → `ci.dlx` → `q.dlq`。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `AMQP_URL` / `PG_DSN` | 本地 guest / `ci_triage` | 总线与状态库 |
| `SVC` | `unknown` | 单元名（日志与指标维度，如 `agent-triage`） |
| `MOCK_SCENARIO` | `default` | 剧本分支 |
| `MOCK_DELAY_MS` | `150:500` | 模拟耗时区间 |
| `MOCK_FAILURE_RATE` | `0.0` | 注入随机失败，验证重试与 DLQ |
| `NOISE_THRESHOLD` | `0.85` | triage 噪声抑制阈值 |
| `REPRO_ENABLED` / `REPRO_MAX_RUNS` | `true` / `3` | 复现开关与次数上限 |
| `TICKET_BACKEND` / `TICKET_AUTOCREATE` | `jira-mock` / `true` | 工单桩与是否自动建单 |
| `HTTP_PORT` / `LOG_JSON` / `LOG_LEVEL` | `8081` / `true` / `info` | 健康探针与日志 |

## 测试

```bash
cd agents && pytest -q        # 契约单测：stage-contracts、19 份 ci_feed、mode 校验
ci-selfcheck                  # 6 剧本逐条断言（退出码 0 = 全绿）
```

## 共享层

`src/iqe_agents/common/` 是 `orchestrator/src/iqe_orchestrator/common/` 的**字节级副本**。
**不要在这里手改共享层**——改真源后跑 `make sync-common`，否则
`tests/contract/test_common_parity.py` 会失败。

# CI Staging 分诊系统（8 Agent + 编排层，可演示实现）

业务口径来自总体流程图：**12 个阶段、两条分支、三道闸门**。阶段用「8 个 Agent × mode」承载，
共 **13 个执行单元**（monitor/triage/reviewer/remediation/reproduction/regression/ticketing/reporting）；
1 个编排层（Python + LangGraph）负责分流、路由、记账与收敛。

Agent 之间**只通过 RabbitMQ 通信**，编排层用**封存计划**驱动执行；Agent 业务逻辑是 mock（夹具驱动），
但消息拓扑、计划解析/校验、分支路由、幂等、超时重试、闸门回炉、DLQ、记账全部是真的。

数据库与消息队列**复用集群里已有的服务**：只新增 RabbitMQ 的 vhost `ci` 与 Postgres 库 `ci_triage`。

```text
1 Monitor(scan) ─► 2 Triage(classify) ─► 3 Reviewer(gate1) ─┬─ internal ─► 4a Remediation(fix)
                                                            │              ─► 5a Reproduction(verify)
                                                            │              ─► 6a Reviewer(gate2) ─┐
                                                            └─ external ─► 4b Reproduction(evidence)
                                                                           ─► 5b Regression(check)
                                                                           ─► 6c Regression(bisect)
                                                                           ─► 7b Ticketing(create)
                                                                           ─► 8b Reviewer(gate3) ─┤
                                                                                                   ▼
                                                                              9 Reporting(case | daily)
```

---

## 1. 30 秒上手（本机，连集群现有中间件）

```bash
cd /home/amd/sut-target-os/python_langraph_os_demo/code
VENV=../.venv/bin/python

# 1) 建 vhost / 8 个 Agent 账号 / 权限（幂等，可反复跑）+ 建库（自动跑 migrations）
$VENV -m iqe_orchestrator.tools.setup_broker --mgmt http://127.0.0.1:15672
$VENV -m iqe_orchestrator.migrate

# 2) 起 1 个编排层 + 8 个 Agent（共 9 个进程）
LOG_DIR=/tmp/ci-run bash scripts/run_local.sh

# 3) UI / API
open http://127.0.0.1:8080/            # Case 列表 + 封存计划 + 实时进度
curl -s http://127.0.0.1:8080/api/scenarios | $VENV -m json.tool

# 4) 端到端验收（覆盖 19 个 ci_feed 场景）
ORCH_BASE_URL=http://127.0.0.1:8080 PG_DSN="$PG_DSN" $VENV -m pytest -m integration -q
```

本机端口：编排层 `8080`；Agent `8101..8108` =
`monitor / triage / reviewer / remediation / reproduction / regression / ticketing / reporting`
（只暴露 `/healthz`，没有业务 HTTP）。

`run_local.sh` 默认连：RabbitMQ `10.100.200.12:5672`（集群 `infra-database/rabbitmq`）、
Postgres `10.96.75.0:5432`（`infra-database/postgres`）。可用 `AMQP_HOST_LOCAL` / `PG_DSN` / `ORCH_PORT` 覆盖。

离线自检（不连 MQ；`graph_smoke` 只要一个 Postgres 存 checkpoint）：

```bash
$VENV -m iqe_agents.tools.selfcheck                  # 144 项断言：8 Agent / 13 mode / 19 份 ci_feed / 计划语义
$VENV -m iqe_orchestrator.tools.graph_smoke          # 状态机 + 计划层，FakeGateway，13 个场景
$VENV -m iqe_orchestrator.tools.ui_render_check      # 离线渲染 4 个 Web UI 页面（空库 + 合成数据）
```

> `ci-selfcheck` 是同一入口的 console script（与 `python -m iqe_agents.tools.selfcheck` 等价）。

---

## 2. 目录

| 目录 | 内容 |
|---|---|
| `orchestrator/src/iqe_orchestrator/common/` | 共用底座的**真源**：`bus.py`（13 个队列的拓扑与收发）、`db.py`、`envelope.py`（命令/结果/事件信封 + 幂等键）、`contracts.py`（stage/mode 契约）、`kb.py`（知识库检索）、`config.py` `paths.py`、`mock.py`（夹具）、`health.py`、`logs.py`、`data/{schemas,workflows,fixtures}` |
| `agents/src/iqe_agents/common/` | 上面这份底座的**字节级副本**（`make sync-common` 同步，`tests/contract/test_common_parity.py` 门禁），Agent 侧禁止手改 |
| `agents/src/iqe_agents/runtime.py` | Agent 消费循环：mode/形状校验 → 四层幂等 claim → 跑剧本 → 发布 result → ack |
| `agents/src/iqe_agents/<stage>/` | 8 个 Agent 各自的 `main.py`（入口）与 `playbook.py`（按 `payload.mode` 分派的剧本）；`triage/skills/` 是三份 SKILL 提示卡 |
| `orchestrator/src/iqe_orchestrator/` | `app.py`（HTTP API + Web UI）、`graph.py`（状态机 + CaseEngine：按 step_id 存输出、按 `on.<verb>` 路由）、`dispatch.py`（下发/等待/DLQ 归档）、`plan.py`（注册表 + LLMPlanner + V0–V9）、`scan.py`、`scheduler.py`、`reporting.py`、`migrate.py`、`migrations/001_init.sql`、`ui/templates/` |
| `orchestrator/src/iqe_orchestrator/common/data/workflows/` | 内置计划注册表：`s1_observe_only` / `s2_full` / `s3_test_no_repro` |
| `orchestrator/src/iqe_orchestrator/common/data/fixtures/` | mock 数据源：`ci_feed/*.json`（19 个 Monitor 场景）、`known_issues.json`、`triage/`、`reviewer/`、`remediation/`、`repro/`、`regression/`、`ticketing/`、`reporting/`、`plans/`（LLM 计划夹具） |
| `orchestrator/src/iqe_orchestrator/common/data/schemas/stage-contracts.json` | **权威契约**：8 个 stage 的 mode 归属与 input/output JSON Schema（两侧副本必须一致） |
| `tests/integration/test_e2e_monitor_scenarios.py` | 端到端验收（真 MQ + 真 DB + 真 9 进程） |
| `scripts/` | `run_local.sh`（起 9 个进程）、`sync_common.sh`、`check_secrets.sh` |
| `tools/`（包内） | `orchestrator/src/iqe_orchestrator/tools/`：`setup_broker.py`、`graph_smoke.py`、`ui_render_check.py`；`agents/src/iqe_agents/tools/selfcheck.py` |
| `deploy/` | `k8s/00-infra.yaml`（vhost/库说明）、`k8s/20-orchestrator.yaml`、`k8s/21-agents.yaml`（8 个 Deployment）、`deploy.sh` |

---

## 3. 消息拓扑（星型，broker 权限强制）

```
                 ┌────────────── ci.cmd  (topic) ──────────────┐
                 │  ci.cmd.<stage>.<mode>     只有编排层能写   │
                 ▼        ▼        ▼         ▼         ▼        ▼        ▼        ▼
          q.agent.monitor triage reviewer remediation reproduction regression ticketing reporting
                 │        │        │         │         │        │        │        │
                 └────────┴────────┴────┬────┴─────────┴────────┴────────┘
                                        ▼
                    ci.result → q.orch.results (+ q.audit.results, 7d TTL)
                    ci.event  → q.orch.events  (+ q.audit.events, 30d TTL)
                    任何 nack(requeue=False) / 过期 → ci.dlx → q.dlq
```

**13 个队列** = 8 个 `q.agent.<stage>` + `q.orch.results` + `q.orch.events` + `q.audit.results` +
`q.audit.events` + `q.dlq`。

| stage | 命令 subject | result subject | prefetch |
|---|---|---|---|
| `monitor` | `ci.cmd.monitor.scan` | `ci.result.monitor` | 1 |
| `triage` | `ci.cmd.triage.classify` | `ci.result.triage` | 4 |
| `reviewer` | `ci.cmd.reviewer.gate1\|gate2\|gate3` | `ci.result.reviewer` | 4 |
| `remediation` | `ci.cmd.remediation.fix` | `ci.result.remediation` | 2 |
| `reproduction` | `ci.cmd.reproduction.evidence\|verify` | `ci.result.reproduction` | 4 |
| `regression` | `ci.cmd.regression.check\|bisect` | `ci.result.regression` | 2 |
| `ticketing` | `ci.cmd.ticketing.create` | `ci.result.ticketing` | 2 |
| `reporting` | `ci.cmd.reporting.case\|daily` | `ci.result.reporting` | 2 |

* **第三段就是 mode**：`dispatch` 发命令时 `action = mode`，payload 固定 `{"mode": …, "input": …}`；
  Agent 收到不属于本 stage 的 mode 直接进 DLQ（`contract_violation`）。
* **结果只按 stage 区分**：`ci.result.<stage>` 不带 mode，靠 `idempotency_key` 与 step 配对。
* **Agent 不能互相下命令**：`ci_agent_<stage>` 账号 `write` 只有 `ci.result`，`read` 只有自己的队列。
* **Monitor 不是 plan step**：由 Scheduler 或 `POST /api/scan` 触发，幂等键 `<run_id>:scan:1`，
  对应 `stage_runs` 行 `case_id IS NULL`、`scan_id` 非空。
* **只有编排层声明拓扑**（`declare=True`）；Agent 侧 `passive=True`，队列不存在就报错。

### 权限矩阵（`tools/setup_broker.py`，实测校准过）

| 账号 | configure | write | read |
|---|---|---|---|
| `ci_orch` | `ci.*` + `q.orch.*` `q.dlq` `q.audit.*` `q.agent.*` | `ci.cmd` `ci.event` `ci.dlx` + 全部队列 | `ci.cmd` `ci.result` `ci.event` `ci.dlx` + 全部队列 |
| `ci_agent_<stage>`（8 个） | `q.agent.<stage>` + `ci.result` | `ci.result` | `q.agent.<stage>` |

> **实测坑**：RabbitMQ 把 `queue.bind` 判为「对该队列的 **write** + 对该 exchange 的 **read**」。
> 编排层是唯一做绑定声明的单元，所以它必须对 `q.agent.*` 有 write、对 `ci.cmd` 等 exchange 有 read，
> 否则 `ACCESS_REFUSED`。**星型约束仍然成立**：只有 `ci_orch` 对 exchange `ci.cmd` 有 write。

---

## 4. 计划驱动的执行（编排层核心）

```
观测 → PlanResolver.match（workflows/*.yaml，按 -priority,key 排序）
        ├─ 命中 signal / job_pattern / job_not_pattern / signature_family_prefix → 实例化计划（source=workflow）
        └─ 未命中 → LLMPlanner（mock：fixtures/plans/<hint>.yaml，source=llm）
      → 同一个 JSON Schema(V0) + V1–V9 语义校验器
      → 校验两次都不通过 → 降级到内置安全网（s1_observe_only / s2_full / s3_test_no_repro），plans.degraded=true
      → 封存（plans 表存 YAML 原文 + sha256；/api/cases/{id}/plan.yaml 可核对哈希）
      → 单一确定性执行器（LangGraph：resolve_plan → step → advance → finalize）
```

* 执行期**不再调用模型**：计划一旦封存就是确定的 YAML，重试/回炉/超时全部由执行器负责。
* **step 输出按 `step_id` 存放**：reviewer 在同一张计划里跑三道闸门，按 stage 存会互相覆盖。
* `next` 表达分支，`jump` 表达回炉（只能指向更早的 step，`max_loops` 限制次数）。

### 三份内置 workflow

| key | 命中条件 | 形状 |
|---|---|---|
| `s1_observe_only` | `signal=none`（priority 100） | `t1 → p1`（只观测，triage 后直接报告） |
| `s2_full` | `signal=error` 且 job 不是 `llm-plan-demo`（priority 100） | `t1 → g1`；internal：`r1 → v1 → g2 → p1`；external：`e1 → c1 →(regression) b1 → k1 → g3 → p1`；非回归 `c1 → k1` |
| `s3_test_no_repro` | 签名族前缀 `test:`（priority 150） | 同 s2_full，但**跳过 4b 受控复现**：external 从 `c1` 起 |

### 闸门结论与路由动词

| 闸门 | checks（`output.checks[].id`，即契约编号） | 结论 → 路由 |
|---|---|---|
| `gate1`（阶段 3） | `evidence_present` `triage_confident` `path_known` `loop_not_exhausted` | `approve`+`decision=internal\|external` → `r1` / `e1`；`needs_info` → `jump t1`（`max_loops: 2`）；`reject` → `end` |
| `gate2`（阶段 6a） | `fix_applied` `change_target` `verification_present` `fix_verified` `evidence_present` `loop_not_exhausted` | `approve` → `p1`；`rework` → `jump r1`；`reject` → `end` |
| `gate3`（阶段 8b） | `ticket_status` `ticket_sections` `owner_assigned` `duplicate_consistent` `loop_not_exhausted` | `approve` → `p1`；`needs_info` → `jump k1`；`reject` → `end` |

* 机械预检优先：低置信（`< 0.70`）、验证失败、缺段落时**恒为** `needs_info` / `rework`；
  夹具只能在机械结论为 `approve` 时把结论改严（或 `force`），不能把该拒绝的改成通过。
* `regression:check` 的路由动词是 `regression | not_regression`；其余 stage 用 `done`。
* `_route_for` 的顺序：`on.<verb>` 命中 → 分支；无 `on` → 顺序推进；只有 `on.done` → 兜底；都不命中 → `blocked`。
* 回炉会**清空下游输出**（按 step_id 删除），`review_loop_seq` 记录实际回炉次数；
  预算用完仍 `needs_info` → `reject(loop_exhausted)`。
* 结果状态映射：`ok→done`、`blocked→blocked`、`retryable/timeout→failed`；
  编排层超时先落 `failed`，Agent 迟到的结果把该行改判 `late`（不改变 Case 结论）。
* Case 终态只有三种：`closed` / `closed_rejected` / `blocked`。

### 幂等四层

| 层 | 机制 | 位置 |
|---|---|---|
| 1 消息级 | `msg_dedup(idempotency_key)` 抢占：同一命令重复投递只执行一次 | Agent `runtime.py` |
| 2 阶段级 | `stage_runs(idempotency_key)` UNIQUE + `already_done` 复用已有结论 | 编排层 `dispatch` / `graph` |
| 3 副作用级 | `tickets(case_id, backend)` UNIQUE；复现动作先查 backref | Agent 剧本 / DDL |
| 4 发布级 | 先 `publish` 拿到 confirm 再 `ack`；发布失败不 ack，让 broker redeliver | Agent `runtime.py` |

幂等键公式：`{case_id 或 run_id}:{step_id}:{attempt}`；`attempt` 在同一 Case 生命周期内**单调递增**，
回放（`POST /api/cases/{id}/replay`）复用同一份封存计划，但先用 DB 里该 Case 的历史最大 attempt 播种，
保证新 attempt 更大、键全新（否则会被幂等层 1 当重复消息丢掉）。

---

## 5. Web UI 与 API

UI：`/`（Case 列表 + 扫描）、`/ui/cases/{id}`（计划、每个 step 的状态/尝试/耗时、trace 时间线、证据、三道闸门的复核卡）、
`/ui/plans`（计划注册表与已封存计划）、`/ui/health`（依赖与队列水位）。

| 端点 | 用途 |
|---|---|
| `GET /healthz` `GET /readyz` `GET /metrics` | 存活 / 就绪（k8s readinessProbe 用 `/readyz`）/ Prometheus 文本计数 |
| `POST /api/scan` `GET /api/scans` `GET /api/scenarios` | 触发扫描（`{"scenario":"new_failure","wait":true}`）/ 扫描历史 / 可用夹具场景 |
| `GET /api/cases` `GET /api/cases/{id}` `GET /api/cases/{id}/trace` | Case 列表 / 详情（含 progress、attempts、终态）/ 时间线 |
| `GET /api/cases/{id}/plan` `GET /api/cases/{id}/plan.yaml` | 封存计划 JSON / YAML 原文（含 `yaml_sha256`） |
| `POST /api/cases/{id}/replay` | 复用同一计划重跑（新 attempt） |
| `GET /api/plans` `GET /api/workflows` `GET /api/dlq` `GET /api/stats` | 计划库 / 注册表 / 死信详情 / 统计 |
| `POST /api/report/daily` `GET /api/daily-report?day=` | 触发生成日报 / 读取日报 |
| `POST /api/ops/inject` | 注入命令做故障演示：`duplicate_command`（验证幂等）/ `bad_command`（验证 DLQ） |

---

## 6. 场景矩阵（本轮实测）

`fixtures/ci_feed/<scenario>.json` → `POST /api/scan {"scenario": "<scenario>"}`。
计数含义：`observed` = 扫到的 build 观测，`errors_promoted` = 升格建 Case 的报错数，
`suppressed` = 被降噪/合并的观测数。

| 场景 | observed / promoted / suppressed | Case | 实测链路（stage:mode） | 终态 |
|---|---|---|---|---|
| `all_success` | 3 / 0 / 0 | 3（signal=none） | `triage:classify > reporting:case`（`s1_observe_only`） | closed ×3 |
| `new_failure` | 3 / 1 / 0 | 3 | error：`t1 > g1 > e1 > c1 > b1 > k1 > g3 > p1`（`s2_full` 外部） | closed |
| `known_issue_noise` | 3 / 0 / 3 | 0 | 全抑制，只留 Monitor 证据与计数 | — |
| `mixed` | 4 / 1 / 1 | 3 | error（toolchain 编译失败 → environment + internal）：`t1 > g1 > r1 > v1 > g2 > p1` | closed |
| `duplicate_merge` | 4 / 1 / 3 | 1 | 同签名族只留最大 build（`duplicate_merge_count`）；`test:*` → `s3_test_no_repro` 内部分支 | closed |
| `flaky_unstable` | 1 / 1 / 0 | 1 | `promoted_reason=unstable_result`；外部全链，复现结论 `flaky` | closed |
| `aborted_infra` | 1 / 1 / 0 | 1 | `ci-infra` → infrastructure（0.88）→ `s2_full` 内部分支 | closed |
| `many_errors` | 5 / 5 / 0 | 5 | driver → 外部；test → `s3` 内部；apt-repo → 内部；image-build / timeout → 外部 | closed ×5 |
| `blocked_no_machine` | 1 / 1 / 0 | 1 | `t1 > g1 > e1` 报 `source_unavailable` | blocked |
| `timeout` | 1 / 1 / 0 | 1 | `e1` 第 1 次 `late`、第 2 次 `timeout`（60s × 2） | blocked |
| `not_reproducible` | 1 / 1 / 0 | 1 | 复现结论 `no`，仍走 5b → 6c → 7b → 8b → 9 | closed |
| `needs_info` | 1 / 1 / 0 | 1 | `g1` needs_info → `jump t1` → 第 2 轮 approve → 外部全链；`loop_seq=1` | closed |
| `insufficient` | 1 / 1 / 0 | 1 | `g1` 连续 3 轮 needs_info → `reject(loop_exhausted)`；`loop_seq=2` | closed_rejected |
| `reject` | 1 / 1 / 0 | 1 | `g1` 直接 reject | closed_rejected |
| `duplicate` | 1 / 1 / 0 | 1 | 外部全链；工单 `linked_duplicate`（`OST-2211`）→ `gate3` approve | closed |
| `unknown_low_confidence` | 1 / 1 / 0 | 1 | 分诊置信度 0.30 → `triage_confident` 不通过 → 3 轮后 reject | closed_rejected |
| `llm_warming` | 1 / 1 / 0 | 1 | job 含 `llm-plan-demo` → 注册表不命中 → LLM 计划（`source=llm`）→ 外部全链 | closed |
| `llm_invalid` | 1 / 1 / 0 | 1 | LLM 计划故意非法 → 校验两次拒绝 → 降级 `s2_full`（`plans.degraded=true`） | closed |
| `source_unavailable` | 无夹具 | 0 | 扫描本身 `blocked`（`error.code=source_unavailable`），不出现在 `/api/scenarios` | — |

> 夹具命名约定：`context.mock_scenario` = 该 build 期望走的剧本名，Agent 取
> `fixtures/<stage>/<scenario>.json`，缺同名文件则回落 `default`。所以新增 ci_feed 场景时，
> 要同步补对应 stage 的同名夹具，否则语义会退化成 `default` 的 happy path。

---

## 7. 部署到 k8s（同一镜像，不同 command）

```bash
bash deploy/deploy.sh            # selfcheck → 打镜像 → 推 Harbor → 配置 MQ/DB → apply → 盯 rollout
# 可选：TAG=manual-01 bash deploy/deploy.sh   覆盖镜像 tag
```

* **9 个 Deployment**：`orchestrator`（Service `ci-triage-ui`，NodePort **30099**）+
  `agent-{monitor,triage,reviewer,remediation,reproduction,regression,ticketing,reporting}`，
  同一个镜像、不同 `command: ["ci-orchestrator"]` / `["ci-agent-<stage>"]`（console script）。
* 镜像 tag 用 **内容哈希**（`YYYYMMDD-HHMM-<sha256(所有文件)>`）而不是 git hash：
  `python_langraph_os_demo/` 在 git 里是未跟踪目录，`git rev-parse --short HEAD` 恒定不变，
  同 tag 重推会让 Harbor / 节点用旧缓存。部署结束会打印 digest，作为回滚锚点。
* 拉取用 `imagePullSecrets: harbor-pull`；只复用集群现有 `infra-database/{rabbitmq,postgres}`
  （集群内 `rabbitmq.infra-database.svc.cluster.local:5672`、`postgres...:5432`；
  本机脚本走 ClusterIP `10.100.200.12` / `10.96.75.0`，见第 9 节）。
* 探针：readiness `/readyz`、liveness `/healthz`；`orchestrator` 用 `Recreate` 策略
  （避免两个编排层同时消费 `q.orch.results`）。
* 调度：9 个 pod 都带 nodeAffinity 只允许 `dell-r7625`（原因见第 9 节）。

### 7.1 全自包含演示镜像（`iqe-agent-orchestrator/ci-triage-allinone`）

集群里这套是「无状态进程 + 外部 PG/MQ」；给评审/离线演示另有一个**单容器**口径：

```bash
PUSH=0 TAG=local-dev bash deploy/build_allinone.sh   # 只构建
bash deploy/build_allinone.sh                        # 构建 + 推 Harbor（含 :latest）
docker run -d --name ci-allinone -p 18080:8080 -p 15672:15672 \
  172.31.0.3:7777/iqe-agent-orchestrator/ci-triage-allinone:latest
```

* 容器内：Postgres 17 + RabbitMQ（vhost `/ci` + 9 个账号）+ 编排层 `:8080` + 8 Agent `:8101..8108`，
  由 `deploy/allinone/entrypoint.sh` 按 PG → MQ → 拓扑 → `/readyz` → Agent 的顺序拉起，任一步失败即退出（不「假活」）；
* 口令同源：`-e CI_DEMO_PASS=xxx` 同时改 PG 与 MQ 的口令；
* 实测：`/readyz` 200、9 个 `/healthz` 全 200、`new_failure` 扫描跑完 11 步外部分支并结案；
* **与集群现有 Deployment 互斥**：两套 broker 同时消费 `q.agent.*` 会抢消息，
  要演练先 `kubectl -n ci-triage scale deploy --all --replicas=0`，再 apply `deploy/k8s/30-allinone.yaml`（本轮未 apply）。

---

## 8. 已知偏差与简化（演示范围）

1. **9 个 Deployment 共用一个镜像**，只换 `command`/`env`。设计上 8 个 Agent 各自独立部署，
   演示期用一个镜像省掉 8 次构建；隔离靠各自的队列与 broker 权限，不靠镜像。
2. **LLM 计划器是 mock**：`fixtures/plans/<hint>.yaml` 充当模型输出；真实 endpoint 通过
   `PLAN_LLM_ENDPOINT` / `PLAN_MODEL` 接入，接口形状不变。`MOCK_LLM_ENABLED=false` 可关掉。
3. **Agent 业务是 mock**：日志、复现结果、工单、闸门结论都来自 `fixtures/`；
   `TICKET_BACKEND=jira-mock`，`TICKET_AUTOCREATE=true`。
4. 工单/复核的**真实外部系统对接**、`workflows/*.yaml` 的评审流程、
   以及 `ci.event.*` 事件的下游消费者，都不在本演示范围内（见 `architecture/06-边界与待确认.md`）。

## 9. 实现期踩过的坑（改代码前先看）

* `workflows/*.yaml` 里路由键必须写 **`"on":`**（YAML 1.1 会把裸 `on` 解析成 `true`）。
* `bus.py`：aio-pika 10 的 `channel()` **没有** `confirm_timeout` 参数；`publish()` 会等 publisher confirm。
* **`/readyz` 必须存在**：`run_local.sh` 与 k8s `readinessProbe` 都打它；缺失时 `run_local.sh`
  会白等 60s 才启动 Agent，第一次定时扫描必然超时。
* 编排层消费结果的回调要 **`finally: await message.ack()`**：结果处理里抛异常又不 ack，
  配合 prefetch 会把 `q.orch.results` 堵死，表现为「每个 stage 都 timeout」。
* `log_with(logger, level, event, **ctx)` 的事件名是位置参数：**不要**用 `event=` 作为 kwarg。
* asyncpg：`$1::date` 的参数必须是 `datetime.date`（`db.py` 的 `_as_date()`）；
  jsonb 读写要在 `DB.create` 与测试 `_fetch` 都设置 `json/jsonb` 文本 codec；
  **查询 jsonb 列拿到的是 `str`**，写脚本时必须先 `json.loads`。
* **`TemplateResponse(request, name, context)`**：starlette 1.x 删掉了旧签名
  （症状：UI 全站 500 `TypeError: unhashable type: 'dict'`，而 JSON API 全绿）。跑 `ui_render_check`。
* **Jinja 里 `x.items` 不是键 `"items"`**：dict 属性查找先命中 `dict.items` 方法，写 `dlq["items"]`。
* **日报的“天”必须统一时区**：`created_at::date` 与本机日期（+8）对不上，跨天全 0。
  统一走 `common/dates.py` + `REPORT_TZ`，半开区间 `[day 00:00, day+1 00:00)`。
* DLQ 里“原始 routing key”要从 x-death 的 **`routing-keys`（数组）** 取，不是 `message.routing_key`
  （后者已经是 `ci.dlq.<stage>`），RabbitMQ 4.3.6 实测。
* **Dockerfile 的 `USER` 要写数字 UID**：`runAsNonRoot: true` 时 `USER appuser` 会被 kubelet 拒绝
  （`image has non-numeric user (appuser)`）。
* **`ErrImagePull: server gave HTTP response to HTTPS client`**：Harbor 走 HTTP(`172.31.0.3:7777`)，
  只有 `dell-r7625`、`jsd` 配了 insecure registry；用 nodeAffinity 绕开（正解是补 `certs.d`）。
* **`jsd` 当前 `DiskPressure=True`**，落上去的 pod 立刻 `Evicted`。
  清理驱逐残留：`kubectl -n ci-triage delete pods --field-selector=status.phase=Failed`。
* 本机连集群中间件：Postgres 走 ClusterIP `10.96.75.0:5432`（`172.31.1.15:5432` 会被 `pg_hba` 拒），
  RabbitMQ 管理台只能 `kubectl -n infra-database port-forward svc/rabbitmq 15690:15672`
  （`172.31.0.3:30094` 是 MinIO，不是 RabbitMQ）。
* Agent 的 `prefetch` 就是它的并发度（见 §3 表）：`prefetch=1` 时一个慢剧本（复现 sleep 75s）
  会把队列里别的 Case 卡住。reproduction / triage / reviewer 是 4。
* 清理进程时 `pkill -f "run_local.sh"` 会匹配到自己的 `bash -c` 命令行把自己杀掉；
  用 `pkill -f "[r]un_local.sh"`，并且停止与启动分成两次调用。
* **从 6 Agent 扩到 8 Agent 后 broker 账号要补齐**：`ci_agent_reviewer/remediation/regression/ticketing`
  原先不存在，Agent 启动即 `ProbableAuthenticationError: ACCESS_REFUSED`。先 `setup_broker --verify` 再重跑。
* **k8s 里的旧 Deployment 会与本机栈抢同一个 vhost**：`q.agent.*` 出现 `consumers=2`，
  旧代码收到新 payload 直接 nack，表现为「triage 消息秒进 DLQ、Case 卡在 created」。
  本机演练前先 `kubectl -n ci-triage scale deploy --all --replicas=0`，演练完再按新镜像恢复。
* **`workflows` 表只 upsert 会留旧 key**：计划改名（`s1_lite`→`s1_observe_only` 等）后
  `/api/workflows` 仍返回旧行。启动时 `db.prune_workflows(keep)` 清掉 `source='file'` 的残留。
* **console script 不能直接指向协程**：`ci-selfcheck = "...:main"` 而 `main` 是 `async def`，
  调用后只拿到一个 coroutine、静默退出且退出码 0。入口必须包 `asyncio.run`（现为 `:cli`）。
* **夹具名必须与 `mock_scenario` 同名**：`needs_info` / `insufficient` / `reject` 这类场景
  若没有同名 reviewer 夹具，会回落 `default`（闸门全部 approve），场景语义静默丢失。
* **step 超时与夹具 sleep 要对齐**：`s2_full` 的 `e1` 是 `timeout_s: 60`，
  复现夹具的 `sleep_s` 必须 > 60（现 75）才演示得出超时。
* **分诊置信度阈值 0.70**：`environment` 类根因原先给 0.68，会被闸门 1 判 `triage_confident` 失败；
  现调为 0.72。低置信语义由 `unknown`（0.30）专门演示。
* **`run_local.sh` 的 cleanup trap 会连带杀掉 8 个 Agent**：脚本退出（含被终端信号带走）时
  `trap cleanup EXIT INT TERM` 按 `$LOG_DIR/*.pid` 逐个 kill 并删 pid 文件。症状是编排层还活着、
  `/healthz` 也 200，但每次扫描都 `blocked` 且错误是「等待 monitor 结果超过 30.0s」（`q.agent.monitor`
  没有消费者）。常驻演练请用 `setsid nohup` 单独起 9 个进程，并用 `curl :8101..8108/healthz` 核对。
* **信封没有顶层 `mode` 字段**：`payload.mode` 才是分支。`publish_command_only` 曾用
  `env.get("mode")` 拼 routing key，恒定退化成 `ci.cmd.<stage>.run`，
  导致 DLQ 的 `original_routing_key` 丢失分支信息（现取 `payload.mode`）。
* **`reviews` 表没有 `decision` 列**：`save_review` 只落 `verdict/reason_code/requested_info/checks/quality/loop_seq`。
  `decision=internal|external` 只在 `reviewer:gate1` 的 stage 输出（与 reporting 的入参）里，
  查库断言要从 `stage_runs.output` 取。

---

## 10. 验收结果（实测）

### 10.1 本轮：8 Agent 新模型（本机 9 进程）

| 项 | 结果 |
|---|---|
| 离线自检 `ci-selfcheck` | **144 项断言全通过**（8 Agent / 13 mode / 19 份 ci_feed / 计划语义） |
| 状态机 `graph_smoke` | 13 个场景（观察 / 内部分支 / 外部分支 / 三闸门回炉 / reject / blocked / timeout / s3 / LLM） |
| 端到端 | 18 个 ci_feed 场景逐个 `POST /api/scan`，Case 全部按 §6 的期望收敛 |
| 集成测试 | `pytest -m integration` **43 passed**（19 场景参数化 + 分支形态 + 三闸门 + 计划层 + 幂等/DLQ/重放 + API/UI） |
| 12 阶段 demo | `python demos/harness.py --all` 两条分支 12 个阶段全绿；`pytest tests/unit/test_demos_*.py` 2 passed |
| 全自包含镜像 | `ci-triage-allinone` 单容器 9 进程就绪（PG 17 / RabbitMQ 4.0.5）；`new_failure` 走完 triage→gate1→fix→verify→gate2→evidence→check→bisect→create→gate3→reporting |

扫 18 个场景（另含 1 次调度器自动扫描，共 19 次 `scan`）后直接查 Postgres：

```
scans 19 · cases 30 · stage_runs 178 · evidence 327 · plans 30 · reviews 42
remediations 5 · verifications 16 · regressions 22 · tickets 11 · kb_entries 48
msg_dedup 178 · workflows 3 · dlq 0
```

* Case 终态：`closed 25` / `closed_rejected 3`（insufficient / reject / unknown_low_confidence）/
  `blocked 2`（blocked_no_machine / timeout）。
* 计划来源：`workflow 28` / `llm 2`（其中 1 份被校验器拒绝后降级 `degraded=true`）。
* stage 结果（`stage:mode status=count`）：`monitor:scan 19`、`triage:classify 35`、
  `reviewer:gate1 26`、`reviewer:gate2 5`、`reviewer:gate3 11`、`remediation:fix 5`、
  `reproduction:evidence 11 done + 1 blocked + 2 late`、`reproduction:verify 5`、
  `regression:check 11`、`regression:bisect 11`、`ticketing:create 11`、`reporting:case 25`。
* 闸门：`gate1 approve 18 / needs_info 5 / reject 3`；`gate2 approve 5`；`gate3 approve 11`。
* 工单：`created 10` + `linked_duplicate 1`（`duplicate_of=OST-2211`）。
* 星型拓扑：每个 `q.agent.*` 恰好 **1 个消费者**（本机栈），`q.dlq` 只有编排层 1 个消费者。

### 10.2 历史：6 Agent 时代（口径已变，仅作对照）

上一版模型（Monitor / Triage / Reproduction / Ticket / Review / Reporting）在集群 7 个 Deployment 上跑过：

| 环境 | 命令 | 结果 |
|---|---|---|
| 本机 7 进程 | `pytest tests/integration/test_e2e_monitor_scenarios.py -q` | 42 passed in 217.65s |
| 集群内 7 Deployment（NodePort 30099） | 同上 | 43 passed in 243.97s |

那一轮的落库量（`truncate` 14 张业务表 + 11 个队列 purge 之后）：

```
scans 42 · cases 66 · stage_runs 349 · evidence 677 · plans 67 · reviews 64
tickets 39 · dlq 1 · msg_dedup 351 · daily_reports 1 · workflows 4 · checkpoints 943
```

> 对照时注意：`ticket 64 done` / `review 64 done` / DLQ 原始 rk `ci.cmd.review.run` /
> `workflows 4` 都是旧口径。新模型下应分别为 `ticketing:create` / `reviewer:gate1|gate2|gate3`、
> `ci.cmd.reviewer.<mode>`、`workflows 3`。

### 10.3 当前线上镜像与回滚

```
tag     172.31.0.3:7777/library/ci-triage:20261126-0215-d1c0e0cf8854
digest  sha256:fc1c684caff48f47f4d85e516058135803bd2fd33adeebb672c02839acefdcc4   # 回滚锚点
上一版  sha256:5d26664a0b7cafac70b9ef3f7c088d1a6c956ea64414bc63567f196b4e20c2d8
```

> 该镜像是 **6 Agent 时代**构建的，与新代码不兼容：恢复 `ci-triage` 命名空间前必须先按新代码重新打镜像，
> 否则旧 Agent 会把新 payload 全部 nack 进 DLQ。回滚一个单元的写法：

```bash
kubectl -n ci-triage set image deployment/orchestrator app=172.31.0.3:7777/library/ci-triage@sha256:<旧 digest>
kubectl -n ci-triage rollout status deployment/orchestrator
```

自检三件套（改代码后按顺序跑）：

```bash
$VENV -m iqe_agents.tools.selfcheck                  # deploy.sh 的第一步，不过就不部署
$VENV -m iqe_orchestrator.tools.ui_render_check      # UI 页面渲染（空库 + 真实数据）
$VENV -m iqe_orchestrator.tools.graph_smoke          # 状态机与计划层
```
# CI Staging 分诊系统（6 Agent + 编排层，可演示实现）

设计文档在 [`architecture/`](architecture/README.md)，本目录是它的**可运行实现**：
6 个 Agent（Monitor / Triage / Reproduction / Ticket / Review / Reporting）+ 1 个编排层
（Python + LangGraph），Agent 之间**只通过 RabbitMQ 通信**，编排层用**封存计划**驱动执行，
并提供 Web UI 查看任务、计划与实时进度。Agent 业务逻辑是 mock（夹具驱动），
但消息拓扑、计划解析/校验、幂等、超时重试、回炉、DLQ、记账全部是真的。

数据库与消息队列**复用集群里已有的服务**：只新增 RabbitMQ 的 vhost `ci` 与 Postgres 库 `ci_triage`。

---

## 1. 30 秒上手（本机，连集群现有中间件）

```bash
cd /home/amd/sut-target-os/python_langraph_os_demo/code
VENV=../.venv/bin/python

# 1) 建 vhost / 用户 / 权限（幂等，可反复跑）+ 建库（自动跑 migrations）
$VENV -m iqe_orchestrator.tools.setup_broker --mgmt http://127.0.0.1:15672
$VENV -m iqe_orchestrator.migrate

# 2) 起 1 个编排层 + 6 个 Agent（共 7 个进程）
LOG_DIR=/tmp/ci-run bash scripts/run_local.sh

# 3) UI / API
open http://127.0.0.1:8080/            # 任务列表 + 编排计划 + 实时进度
curl -s http://127.0.0.1:8080/api/scenarios | $VENV -m json.tool

# 4) 端到端验收（覆盖 Monitor 的每一类场景）
ORCH_BASE_URL=http://127.0.0.1:8080 PG_DSN="$PG_DSN" \
  $VENV -m pytest tests/integration/test_e2e_monitor_scenarios.py -q
```

`run_local.sh` 默认连：RabbitMQ `10.100.200.12:5672`（集群 `infra-database/rabbitmq`）、
Postgres `10.96.75.0:5432`（`infra-database/postgres`）。可用 `AMQP_HOST` / `PG_DSN` / `ORCH_PORT` 覆盖。

离线自检（不连 MQ/DB，改 Agent 剧本后先跑这个）：

```bash
$VENV -m iqe_orchestrator.tools.graph_smoke          # 编排层状态机 + 计划层，FakeGateway，11 个场景
$VENV -m iqe_agents.tools.selfcheck     # 6 个 Agent 剧本 + 夹具 + 契约校验，内置 FakeDB（deploy.sh 的第一步）
$VENV -m iqe_orchestrator.tools.ui_render_check      # 离线渲染 4 个 Web UI 页面（空库 + 真实/合成数据），模板改动的守门员
```

---

## 2. 目录

| 目录 | 内容 |
|---|---|
| `orchestrator/src/iqe_orchestrator/common/` | 共用底座的**真源**：`bus.py`（拓扑与收发）、`db.py`（Postgres 访问）、`envelope.py`（命令/结果/事件信封 + 幂等键）、`config.py` `paths.py`、`mock.py`（夹具）、`health.py`（`/healthz` `/readyz` `/metrics`）、`logs.py`（结构化日志 + 计数器）、`data/{schemas,workflows,fixtures}` |
| `agents/src/iqe_agents/common/` | 上面这份底座的**字节级副本**（`make sync-common` 同步，`tests/contract/test_common_parity.py` 门禁），Agent 侧禁止手改 |
| `agents/src/iqe_agents/runtime.py` | Agent 消费循环、契约校验、幂等三层 |
| `agents/src/iqe_agents/<stage>/` | 每个 Agent 的 `main.py`（入口）与 `playbook.py`（剧本 = mock 业务逻辑）；`triage/skills/` 是三份 SKILL 提示卡 |
| `orchestrator/src/iqe_orchestrator/` | `app.py`（HTTP API + Web UI）、`graph.py`（LangGraph 状态机 + CaseEngine）、`dispatch.py`（命令下发/结果等待/DLQ 归档）、`plan.py`（注册表 + LLMPlanner + 校验器）、`scan.py`、`scheduler.py`、`reporting.py`、`migrate.py`、`migrations/001_init.sql`、`ui/templates/` |
| `common/data/workflows/` | 内置计划注册表（`s1_lite` `s2_full` `s4_test_no_repro` `s6_infra_direct`） |
| `common/data/fixtures/` | mock 数据源：`ci_feed/*.json`（19 个 Monitor 场景）、`known_issues.json`、`triage/`、`repro/`、`ticket/`、`review/`、`reporting/`、`plans/`（LLM 计划夹具） |
| `common/data/schemas/stage-contracts.json` | **权威契约**：每个 stage 的 input/output JSON Schema（两侧副本必须一致） |
| `tests/integration/test_e2e_monitor_scenarios.py` | 端到端验收（真 MQ + 真 DB + 真 7 进程） |
| `scripts/` | `run_local.sh`（起 7 个进程）、`sync_common.sh`（共享层同步）、`check_secrets.sh` |
| `tools/`（包内） | `orchestrator/src/iqe_orchestrator/tools/`：`setup_broker.py`、`graph_smoke.py`、`ui_render_check.py`；`agents/src/iqe_agents/tools/selfcheck.py` |
| `deploy/` | `k8s/00-infra.yaml`（vhost/库的配置说明）、`k8s/20-orchestrator.yaml`、`k8s/21-agents.yaml`、`deploy.sh` |

---

## 3. 消息拓扑（星型，broker 权限强制）

```
                 ┌────────────── ci.cmd  (topic) ──────────────┐
                 │  ci.cmd.<stage>.<action>   只有编排层能写   │
                 ▼          ▼          ▼          ▼      ▼      ▼
        q.agent.monitor  q.agent.triage  q.agent.reproduction  q.agent.ticket  q.agent.review  q.agent.reporting
                 │          │            │            │          │      │
                 └──────────┴────────────┴─────┬──────┴──────────┘
                                               ▼
                          ci.result → q.orch.results (+ q.audit.results, 7d TTL)
                          ci.event  → q.orch.events  (+ q.audit.events, 30d TTL)
                          任何 nack(requeue=False) / 过期 → ci.dlx → q.dlq
```

* **Agent 不能互相下命令**：`ci_agent_<stage>` 账号 `write` 只有 `ci.result`，`read` 只有自己的队列。
* **Monitor 不是 plan step**：由 Scheduler 或 `POST /api/scan` 触发，幂等键 `<run_id>:scan:1`，
  对应 `stage_runs` 行 `case_id IS NULL`、`scan_id` 非空。
* **只有编排层声明拓扑**（`declare=True`）；Agent 侧用 `passive=True`，队列不存在就报错，
  避免 Agent 自己造出没有绑定的队列。

### 权限矩阵（`tools/setup_broker.py`，实测校准过）

| 账号 | configure | write | read |
|---|---|---|---|
| `ci_orch` | `ci.*` + `q.orch.*` `q.dlq` `q.audit.*` `q.agent.*` | `ci.cmd` `ci.event` `ci.dlx` + 全部队列 | `ci.cmd` `ci.result` `ci.event` `ci.dlx` + 全部队列 |
| `ci_agent_<stage>` | `q.agent.<stage>` + `ci.result` | `ci.result` | `q.agent.<stage>` |

> **实测坑**：RabbitMQ 把 `queue.bind` 判为「对该队列的 **write** + 对该 exchange 的 **read**」。
> 编排层是唯一做绑定声明的单元，所以它必须对 `q.agent.*` 有 write、对 `ci.cmd` 等 exchange 有 read，
> 否则 `ACCESS_REFUSED`。**星型约束仍然成立**：只有 `ci_orch` 对 exchange `ci.cmd` 有 write，
> Agent 物理上无法给别的 Agent 发命令。

---

## 4. 计划驱动的执行（编排层核心）

```
观测 → PlanResolver（workflows/*.yaml 注册表匹配）
        ├─ 命中 → 该 YAML 计划
        └─ 未命中 → LLMPlanner（当前是 mock：fixtures/plans/<hint>.yaml）
      → 同一个 JSON Schema(V0) + V1–V8 语义校验器
      → 校验两次都不通过 → 降级到内置兜底计划（s1_lite / s2_full），Case 上标 degraded
      → 封存（plans 表存 YAML 原文 + sha256；UI 上 /api/cases/{id}/plan.yaml 可核对哈希）
      → 单一确定性执行器（LangGraph: resolve_plan → validate_plan → step → advance → finalize）
```

* 执行期**不再调用模型**：计划一旦封存就是确定的 YAML，重试/回炉/超时全部由执行器负责。
* 路由动词：triage `classified_error|classified_none`；review `approve|needs_info|reject`；其余 `done`。
* `needs_info: { jump: t1, max_loops: 2 }` → 回炉到 triage 重跑，并**清空下游输出**（工单/复核结论作废）。
  `review_loop_seq` 记录**实际发生的回炉次数**；预算用完还是 `needs_info` → Case `blocked`。
* 结果状态 → `stage_runs` 状态映射：`ok→done`、`blocked→blocked`、`retryable→failed`、`expired→expired`；
  编排层超时先落 `failed`，Agent 迟到的结果把该行改判 `late`（不改变 Case 结论）。
* Case 终态：`closed`（正常走完）、`closed_rejected`（复核 reject）、`blocked`（超时/耗尽回炉/资源不可用）。

### 幂等三层

| 层 | 机制 | 位置 |
|---|---|---|
| 1 | `msg_dedup(dedup_key)` 抢占：同一命令重复投递只执行一次 | Agent 侧 `agent_runtime` |
| 2 | `stage_runs.idempotency_key` UNIQUE：同一 step 同一 attempt 只有一行 | 编排层 `dispatch` |
| 3 | `tickets(case_id, backend)` UNIQUE：同一 Case 不重复开单 | Ticket Agent |

幂等键公式：`{case_id 或 run_id}:{step_id}:{attempt}`；`attempt` 在同一 Case 生命周期内**单调递增**，
回放（`POST /api/cases/{id}/replay`）复用同一份封存计划，但先用 DB 里该 Case 的历史最大 attempt 播种，
保证新 attempt 更大、键全新（否则会被幂等层 1 当重复消息丢掉）。

---

## 5. Web UI 与 API

UI：`/`（Case 列表 + 扫描）、`/ui/cases/{id}`（计划、每个 step 的状态/尝试/耗时、trace 时间线、证据、复核卡）、
`/ui/plans`（计划注册表与已封存计划）、`/ui/health`（依赖与队列水位）。

| 端点 | 用途 |
|---|---|
| `GET /healthz` `GET /readyz` `GET /metrics` | 存活 / 就绪（k8s readinessProbe 用 `/readyz`）/ Prometheus 文本计数 |
| `POST /api/scan` `GET /api/scans` `GET /api/scenarios` | 触发扫描（`{"scenario":"new_failure","wait":true}`）/ 扫描历史 / 可用夹具场景 |
| `GET /api/cases` `GET /api/cases/{id}` `GET /api/cases/{id}/trace` | Case 列表 / 详情（含 progress、attempts）/ 时间线 |
| `GET /api/cases/{id}/plan` `GET /api/cases/{id}/plan.yaml` | 封存计划 JSON / YAML 原文（含 `yaml_sha256`） |
| `POST /api/cases/{id}/replay` | 复用同一计划重跑（新 attempt） |
| `GET /api/plans` `GET /api/workflows` `GET /api/dlq` `GET /api/stats` | 计划库 / 注册表 / 死信详情 / 统计 |
| `POST /api/report/daily` `GET /api/daily-report?day=` | 触发生成日报 / 读取日报 |
| `POST /api/ops/inject` | 注入命令做故障演示（`duplicate_command` / `bad_command`）验证幂等与 DLQ |

---

## 6. Monitor 场景矩阵（验收覆盖目标）

`fixtures/ci_feed/<scenario>.json` → `POST /api/scan {"scenario": "<scenario>"}`。
计数含义：`observed` = 扫到的 build 观测数，`errors_promoted` = 升格建 Case 的报错数，`suppressed` = 被降噪/合并的观测数。

| 场景 | observed / promoted / suppressed | 建 Case | 期望链路 |
|---|---|---|---|
| `all_success` | 3 / 0 / 0 | 3（signal=none） | 只有 `triage`（`s1_lite`），终态 `closed` |
| `new_failure` | 3 / 1 / 0 | 3（1 error + 2 none） | error 走完整 6 Agent（`s2_full`） |
| `known_issue_noise` | 3 / 0 / 3 | 0 | 全抑制，只留 Monitor 证据与计数 |
| `mixed` | 4 / 1 / 1 | 3 | 完整链 + 观测链 + 抑制 |
| `duplicate_merge` | 4 / 1 / 3 | 1 | 同签名族只留最大 build，Case 上标 `duplicate_merge_count`；命中 `s4_test_no_repro`（不占重跑机位） |
| `flaky_unstable` | 1 / 1 / 0 | 1 | `promoted_reason=unstable_result`，复现结论 `flaky` |
| `aborted_infra` | 1 / 1 / 0 | 1 | triage 判 `infrastructure` → `s6_infra_direct`，**跳过复现** |
| `many_errors` | 5 / 5 / 0 | 5 | 5 个不同签名族，各自终态（受 `MAX_INFLIGHT_CASES=4` 限流） |
| `blocked_no_machine` | 1 / 1 / 0 | 1 | 复现报 `source_unavailable` → Case `blocked`，不下游 |
| `timeout` | 1 / 1 / 0 | 1 | 复现剧本 sleep 45s > step `timeout_s: 20` → `retryable` → 重试 → `blocked` |
| `not_reproducible` | 1 / 1 / 0 | 1 | 复现结论 `no`，仍可成单复核 |
| `needs_info` | 1 / 1 / 0 | 1 | 复核 `needs_info` → 回炉 triage → `approve` → 报告，`review_loop_seq=1` |
| `insufficient` | 1 / 1 / 0 | 1 | 两轮都 `needs_info` → 预算用尽 → `blocked`，无 reporting |
| `reject` | 1 / 1 / 0 | 1 | `closed_rejected`，无 reporting |
| `duplicate` | 1 / 1 / 0 | 1 | 工单 `linked_duplicate`（`duplicate_of=OST-4821`），复核第 6 条卡失败 |
| `unknown_low_confidence` | 1 / 1 / 0 | 1 | 分诊置信度 0.30 → 复核 `triage_confidence_ok` 不通过 |
| `llm_warming` | 1 / 1 / 0 | 1 | 注册表故意不命中（job 名含 `llm-plan-demo`）→ LLM 计划 `source=llm` |
| `llm_invalid` | 1 / 1 / 0 | 1 | LLM 计划故意非法 → 两次拒绝 → 降级内置 `s2_full`（`degraded=true`） |
| `source_unavailable` | 无夹具 | 0 | 扫描本身 `blocked`（`stage_runs.error.code=source_unavailable`），且不出现在 `/api/scenarios` |

---

## 7. 部署到 k8s（同一镜像，不同 command）

```bash
bash deploy/deploy.sh            # selfcheck → 打镜像 → 推 Harbor → 配置 MQ/DB → apply → 盯 rollout
# 可选：TAG=manual-01 bash deploy/deploy.sh   覆盖镜像 tag
```

* 7 个 Deployment：`orchestrator`（Service `ci-triage-ui`，NodePort **30099**）+
  `agent-{monitor,triage,reproduction,ticket,review,reporting}`，同一个镜像、不同
  `command: ["python","-m","orchestrator.main"]` / `["python","-m","agents.<stage>.main"]`。
* 镜像 tag 用 **内容哈希**（`YYYYMMDD-HHMM-<sha256(所有文件)>`）而不是 git hash：
  `python_langraph_os_demo/` 在 git 里是未跟踪目录，`git rev-parse --short HEAD` 恒定不变，
  同 tag 重推会让 Harbor / 节点用旧缓存。部署结束会打印 digest，作为回滚锚点。
* 拉取用 `imagePullSecrets: harbor-pull`；只复用集群现有 `infra-database/rabbitmq`、`infra-database/postgres`
  （集群内地址 `rabbitmq.infra-database.svc.cluster.local:5672`、`postgres.infra-database.svc.cluster.local:5432`；
  本机跑脚本要走 ClusterIP `10.100.200.12` / `10.96.75.0`，见第 9 节）。
* 探针：readiness `/readyz`、liveness `/healthz`；`orchestrator` 用 `Recreate` 策略
  （避免两个编排层同时消费 `q.orch.results`）。
* 调度：7 个 pod 都带 nodeAffinity 只允许 `dell-r7625`（原因见第 9 节：insecure registry 与磁盘压力）。

---

## 8. 已知偏差与简化（演示范围）

1. **7 个 Deployment 共用一个镜像**，只换 `command`/`env`。设计上 6 个 Agent 各自独立部署，
   演示期用一个镜像省掉 6 次构建；隔离靠各自的队列与 broker 权限，不靠镜像。
2. **LLM 计划器是 mock**：`fixtures/plans/<hint>.yaml` 充当模型输出；真实 endpoint 通过
   `PLAN_LLM_ENDPOINT` / `PLAN_MODEL` 接入，接口形状不变。`MOCK_LLM_ENABLED=false` 可关掉。
3. **Agent 业务是 mock**：日志、复现结果、工单、复核结论都来自 `fixtures/`；
   `TICKET_BACKEND=jira-mock`，`TICKET_AUTOCREATE=true`（真实实现里复核通过才自动开单）。
4. 工单/复核的**真实外部系统对接**、`workflows/*.yaml` 的评审流程、
   以及 `ci.event.*` 事件的下游消费者，都不在本演示范围内（见 `architecture/06-边界与待确认.md`）。

## 9. 实现期踩过的坑（改代码前先看）

* `workflows/*.yaml` 里路由键必须写 **`"on":`**（YAML 1.1 会把裸 `on` 解析成 `true`）。
* `orchestrator/src/iqe_orchestrator/common/bus.py`：aio-pika 10 的 `channel()` **没有** `confirm_timeout` 参数；`publish()` 会等 publisher confirm。
* **`/readyz` 必须存在**：`scripts/run_local.sh` 与 k8s `readinessProbe` 都打它；缺失时
  `run_local.sh` 会白等 60s 才启动 Agent，第一次定时扫描必然超时。
* 编排层消费结果的回调要 **`finally: await message.ack()`**：结果处理里抛异常又不 ack，
  配合 prefetch 会把 `q.orch.results` 堵死，表现为「每个 stage 都 timeout」。
* `log_with(logger, level, event, **ctx)` 的事件名是位置参数：**不要**用 `event=` 作为 kwarg（会撞名）。
* asyncpg：`$1::date` 的参数必须是 `datetime.date`（`db.py` 里 `_as_date()`）；
  jsonb 读写需要在 `DB.create` 与测试的 `_fetch` 都设置 `json/jsonb` 文本 codec。
* **`TemplateResponse(request, name, context)`**：starlette 1.x 删掉了旧签名
  `TemplateResponse(name, {"request": request, ...})`，症状是 UI 全站 500
  （`TypeError: unhashable type: 'dict'`），而 JSON API 测试全绿。跑 `tools/ui_render_check.py`。
* **Jinja 里 `x.items` 不是键 `"items"`**：dict 的属性查找先命中 `dict.items` 方法，
  `/ui/health` 因此 500（`'builtin_function_or_method' object is not iterable`）。写 `dlq["items"]`。
* **日报的“天”必须统一时区**：`created_at::date` 跟本机日期（+8）对不上，跨天全 0。
  现在统一走 `orchestrator/src/iqe_orchestrator/common/dates.py` + `REPORT_TZ`，半开区间 `[day 00:00, day+1 00:00)`。
* DLQ 里“原始 routing key”要从 x-death 的 **`routing-keys`（数组）** 取，不是 `message.routing_key`
  （后者已经是 `ci.dlq.<stage>`），RabbitMQ 4.3.6 实测。
* **Dockerfile 的 `USER` 要写数字 UID**：`runAsNonRoot: true` 时 `USER appuser` 会被 kubelet 拒绝
  （`image has non-numeric user (appuser)`），pod 直接创建失败。
* **`ErrImagePull: server gave HTTP response to HTTPS client`**：Harbor 走 HTTP(`172.31.0.3:7777`)，
  只有 `dell-r7625`、`jsd` 配了 insecure registry；新节点 `d-001-g2-gfx1151` 没配。
  本 demo 用 nodeAffinity 绕开（正解是给节点补 `certs.d` 配置）。
* **`jsd` 当前 `DiskPressure=True`**，落上去的 pod 立刻 `Evicted`；`dell-r7625` 实测无压力。
  清理驱逐残留：`kubectl -n ci-triage delete pods --field-selector=status.phase=Failed`。
* 本机连集群中间件：Postgres 走 ClusterIP `10.96.75.0:5432`（`172.31.1.15:5432` 会被 `pg_hba` 拒），
  RabbitMQ 管理台只能 `kubectl -n infra-database port-forward svc/rabbitmq 15690:15672`
  （`172.31.0.3:30094` 是 MinIO，不是 RabbitMQ）。
* Agent 的 `prefetch` 就是它的并发度：`prefetch=1` 时一个慢剧本（复现 sleep 45s）
  会把队列里别的 Case 卡住 → 交叉污染测试结果。现在 reproduction/triage/review 是 4。
* DLQ 行里的 `original_routing_key` 取自 `x-death` 头（死信后 `message.routing_key` 已经变成 `ci.dlq.<stage>`）。
* 清理进程时 `pkill -f "run_local.sh"` 会匹配到自己的 `bash -c` 命令行把自己杀掉；
  用 `pkill -f "[r]un_local.sh"`，并且停止与启动分成两次调用。

---

## 10. 验收结果（实测）

### 10.1 两轮验收都跑过

| 环境 | 命令 | 结果 |
|---|---|---|
| 本机 7 进程（`scripts/run_local.sh`，连集群 MQ/DB） | `pytest tests/integration/test_e2e_monitor_scenarios.py -q` | **42 passed in 217.65s**（`/tmp/ci-run/pytest4.log`） |
| **集群内 7 Deployment**（NodePort 30099） | 同上，`ORCH_BASE_URL=http://172.31.0.3:30099` | **43 passed in 243.97s**（`/tmp/ci-run/pytest_cluster.log`） |

集群那一轮比本机多 1 项，就是 `test_ui_pages_render_with_real_data`（Web UI 三个页面 + Case 详情页
真的渲染出数据）。跑之前做了清场：`truncate` 14 张业务表（保留 `workflows`）+ checkpoint 表，
11 个队列 purge，所以证据里没有历史数据混进来。

### 10.2 集群这轮落库的量（跑完直接查 Postgres）

```
scans 42 · cases 66 · stage_runs 349 · evidence 677 · plans 67 · reviews 64
tickets 39 · dlq 1 · msg_dedup 351 · daily_reports 1 · workflows 4 · checkpoints 943
```

* Case 终态：`closed` 43（含 signal=none 23）· `closed_rejected` 6 · `blocked` 13；
  计划来源 `workflow` 63 / `llm` 4（其中 2 个是被校验器拒绝后降级 `degraded=True` 的安全网）。
* stage 结果：`triage 91 done`、`reproduction 56 done + 2 blocked + 4 late`、
  `ticket 64 done`、`review 64 done`、`reporting 26 done`、`monitor 40 done + 2 blocked`（源不可用）。
* 复核：`approve 25` / `needs_info 33`（`missing_triage 16`、`loop_exhausted 9`、
  `insufficient_evidence 4`、`duplicate_ticket 2`、`missing_repro 2`）/ `reject 6`；
  回炉轮次 `loop_seq` 分布 1:40 / 2:15 / 3:9（`max_review_loops=2` → 最大 3 轮，没有被突破）。
* 工单：`jira-mock` 35 张 `created`（都有 external_key/url）+ 4 张 `linked_duplicate`
  （`external_key` 为空、`duplicate_of` 指向已有单，例如 `OST-4821`）。
* 日报：`day=2026-11-25` 一份，`cases_total=60 scans=40 errors=41 suppressed=13 observed_only=19
  tat_ms_avg=6299 open_cases=0`。
* DLQ：1 条（`review` / `contract_violation: 'run' was expected` / 原始 rk `ci.cmd.review.run`）——
  故意的非法 `mode` 被 broker 路由到 DLX、编排层归档、原始 routing key 取对了。
* RabbitMQ（vhost `ci`，计数器是从建队列起累计的，含本机那几轮）：每个 `q.agent.*` 恰好
  **1 个消费者**、`ready=0`、`delivered == published`、`rejected=0`、`redelivered=0`；
  `q.dlq` 只有编排层 1 个消费者。星型拓扑与「不积压不重复消费」在这里能看到数字。

### 10.3 当前线上镜像与回滚

```
tag     172.31.0.3:7777/library/ci-triage:20261126-0215-d1c0e0cf8854
digest  sha256:fc1c684caff48f47f4d85e516058135803bd2fd33adeebb672c02839acefdcc4   # 回滚锚点
上一版  sha256:5d26664a0b7cafac70b9ef3f7c088d1a6c956ea64414bc63567f196b4e20c2d8
```

回滚（只换 orchestrator 或全部）：

```bash
kubectl -n ci-triage set image deployment/orchestrator app=172.31.0.3:7777/library/ci-triage@sha256:<旧 digest>
kubectl -n ci-triage rollout status deployment/orchestrator
```

自检三件套（改代码后按顺序跑）：

```bash
$VENV -m iqe_agents.tools.selfcheck     # deploy.sh 的第一步，不过就不部署
$VENV -m iqe_orchestrator.tools.ui_render_check      # UI 页面渲染（空库 + 真实数据）
$VENV -m iqe_orchestrator.tools.graph_smoke          # 编排层状态机与计划层
```

# iqe-orchestrator —— CI 分诊编排层

LangGraph 状态机 + 计划层（workflow 优先 / LLM 兜底）+ Dispatch-Await 网关 + HTTP API 与 Web UI。
**编排层是唯一的下游决定者**：Agent 之间不互相调用，也不直连编排层，一切跨单元通信只走 RabbitMQ。

本目录是一个**自包含 Python 项目**：`pyproject.toml`、`src/`、`tests/`、`MANIFEST.in` 齐全，
可以单独 `pip install`、单独打镜像；与仓库里的 [`agents/`](../agents/README.md) 无代码依赖。

## 安装与运行

```bash
pip install -e ./orchestrator          # 只装编排层
# 或在仓库根：make install-dev（两个子项目一起装 + 开发工具）

ci-broker-setup --mgmt http://127.0.0.1:15672   # 声明 exchange/queue/binding（幂等）
ci-db-migrate                                   # 建库 + 建表 + 连通性检查（幂等）
ci-orchestrator                                 # uvicorn :8080，后台跑引擎/网关/调度器
```

| console script | 模块 | 用途 |
|---|---|---|
| `ci-orchestrator` | `iqe_orchestrator.main` | 服务进程：API + UI + LangGraph 引擎 + 网关 + 调度器 |
| `ci-db-migrate` | `iqe_orchestrator.migrate` | 建库、建表、`/readyz` 前置检查 |
| `ci-broker-setup` | `iqe_orchestrator.tools.setup_broker` | 建 vhost/账号/权限/拓扑 |
| `ci-graph-smoke` | `iqe_orchestrator.tools.graph_smoke` | 离线跑状态机（FakeGateway + 真 checkpointer） |
| `ci-ui-check` | `iqe_orchestrator.tools.ui_render_check` | 4 个页面离线渲染断言 |

## 模块地图

| 文件 | 职责 |
|---|---|
| `main.py` | 进程入口：uvicorn 承载 API/UI，后台起引擎、网关、调度器 |
| `app.py` | FastAPI 应用：`/healthz` `/readyz` `/metrics`、`/api/*`、Web UI 页面 |
| `graph.py` | 编排图：`resolve_plan → validate_plan → step → advance → finalize`；图结构固定，路径由 plan 决定 |
| `plan.py` | 计划层：Workflow Registry、`LLMPlanner`、`validate_plan`（V0–V8）、`PlanStore.seal/read`、`builtin_plan` |
| `dispatch.py` | Dispatch–Await：编排层唯一的总线写者，按 `idempotency_key` 配对 in-process Future |
| `scan.py` | 扫描执行：把 Monitor 观测转成 Case 并起各自的 plan 图（抑制规则在观察层生效） |
| `scheduler.py` | 定时触发：Monitor 扫描周期 + 每日日报（只发命令，不替 Agent 决策） |
| `reporting.py` | 日报触发命令（走与 Agent 相同的命令通道） |
| `migrate.py` | 建库 + `init_schema` + 连通性检查 |
| `common/` | 共享层（消息总线、DB、信封/幂等、配置、路径、健康探针、日志、Mock），见下 |
| `common/data/` | 权威契约与资源：`schemas/`（envelope / stage-contracts / plan）、`workflows/`、`fixtures/` |
| `migrations/001_init.sql` | 14 张表的 DDL（Case / stage_runs / evidence / plans / workflows / 工单 / 日报 …） |
| `ui/templates/` | Jinja2 模板：Case 列表、Case 详情、计划、健康 |
| `tools/` | 运维脚本：`graph_smoke` / `ui_render_check` / `setup_broker` |

## 计划层

```text
触发信息 ─┬─ 命中 common/data/workflows/*.yaml → 实例化 plan.yaml（source=workflow，零 LLM）
          └─ 未命中                            → LLM 写同构 plan.yaml（source=llm）
                              │
        yaml.safe_load → JSON Schema → V1–V8 校验（白名单/顺序/跳转/步数/防注入）
                              │
        非法或超时 → 带错误重试 1 次 → degraded 内置安全网（s1_lite / s2_full）
```

人工与模型**同一 schema、同一校验器、同一执行器**；执行期零模型调用，重放读已落盘的 YAML。

## 主要环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `AMQP_URL` | `amqp://guest:guest@127.0.0.1:5672/ci` | 总线 |
| `PG_DSN` | `postgresql://postgres@127.0.0.1:5432/ci_triage` | 状态库 |
| `HTTP_PORT` | `8080` | API / UI |
| `SCHEMAS_DIR` `FIXTURES_DIR` `WORKFLOWS_DIR` | 包内 `common/data/*` | 资源目录覆盖 |
| `PLANS_DIR` | `$PWD/.plans` | 封存计划落盘目录 |
| `PLAN_MODE` | `auto` | `auto` \| `strict`（不调 LLM）\| `llm_only` |
| `MOCK_LLM_ENABLED` / `PLAN_MOCK_SCENARIO` | `true` / `warming_ok` | 离线演练计划生成 |
| `STAGE_TIMEOUT_S` `STAGE_MAX_ATTEMPTS` `MAX_REVIEW_LOOPS` | `30` / `2` / `2` | 阶段超时、重试、复核回炉上限 |
| `SCAN_STARTUP_DELAY_S` `MONITOR_SCAN_INTERVAL_S` | `30` / `3600` | 首次扫描宽限、扫描周期 |
| `SCHEDULER_ENABLED` `REPORT_AT` `REPORT_TZ` | `true` / `18:00` / `UTC` | 定时触发 |

完整清单见 `common/config.py`（单一 `Config` dataclass）。

## 测试

```bash
cd orchestrator && pytest -q                 # 计划层单测（registry 匹配、V4/V8、YAML 往返）
cd ..        && make test                    # 仓库根：unit + contract（含共享层一致性）
PG_DSN=... make smoke ui-check               # 状态机 / UI 渲染（只要 Postgres 存 checkpoint）
```

## 共享层

`src/iqe_orchestrator/common/` 是共享层的**唯一真源**；`agents/src/iqe_agents/common/` 是它的
字节级副本（用 `make sync-common` 同步，`tests/contract/test_common_parity.py` 门禁）。
改共享层只改这里，然后同步。

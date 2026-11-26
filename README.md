# IQE CI 分诊系统（orchestrator + agents）

CI Staging 失败分诊的可运行实现：**1 个编排层**（Python + LangGraph）与 **6 个 Mock Agent**
（Monitor / Triage / Reproduction / Ticket / Review / Reporting）。Agent 之间只通过 RabbitMQ
通信，编排层用**封存计划（YAML）**驱动执行，并提供 Web UI 查看 Case、计划与实时进度。

Agent 业务是夹具驱动的 mock，但**消息拓扑、计划解析与校验、幂等、超时重试、复核回炉、
DLQ、记账**都是真的。架构设计与取舍见 [`docs/architecture/`](docs/architecture/README.md)。

## 两个顶级子项目

仓库按交付边界拆成两个**自包含**子项目：各自有 `pyproject.toml`、`src/`、`tests/`、
`README.md`，可以单独安装、单独打镜像、单独发版。

| 子项目 | 包名 | 入口 | 职责 |
|---|---|---|---|
| [`orchestrator/`](orchestrator/README.md) | `iqe-orchestrator` | `ci-orchestrator` / `ci-graph-smoke` / `ci-ui-check` / `ci-broker-setup` / `ci-db-migrate` | LangGraph 状态机、计划层（workflow 优先 + LLM 兜底）、Dispatch-Await 网关、HTTP API 与 Web UI、迁移 |
| [`agents/`](agents/README.md) | `iqe-agents` | `ci-agent-{monitor,triage,reproduction,ticket,review,reporting}` / `ci-selfcheck` | 6 个 Agent 的消费循环、契约校验、三层幂等、剧本与夹具 |

### 共享层：一份源码、两处副本

消息总线、DB、信封/幂等、配置、路径、健康探针、日志这些底座代码在
`<子项目>/src/<pkg>/common/`。为满足「两个子项目完全独立」，**common 在每个子项目各存一份**：

* 唯一真源：`orchestrator/src/iqe_orchestrator/common/`
* 同步命令：`make sync-common`（`scripts/sync_common.sh`）
* 防漂移门禁：`tests/contract/test_common_parity.py` 对两份做 sha256 全量比对，CI 必跑

```text
.
├── orchestrator/                     # 子项目 1
│   ├── pyproject.toml  MANIFEST.in  README.md
│   ├── src/iqe_orchestrator/
│   │   ├── common/                   # 共享层副本（真源）
│   │   │   ├── bus.py db.py envelope.py config.py paths.py health.py logs.py mock.py dates.py
│   │   │   ├── tools/{fingerprint,logdistiller}.py
│   │   │   └── data/{schemas,workflows,fixtures}/
│   │   ├── app.py graph.py plan.py dispatch.py scheduler.py scan.py reporting.py migrate.py main.py
│   │   ├── migrations/  ui/templates/  tools/
│   └── tests/
├── agents/                           # 子项目 2
│   ├── pyproject.toml  MANIFEST.in  README.md
│   ├── src/iqe_agents/
│   │   ├── common/                   # 同一份共享层副本
│   │   ├── runtime.py                # Agent 消费循环 / Ctx / 契约校验
│   │   ├── {monitor,triage,reproduction,ticket,review,reporting}/
│   │   ├── migrations/  tools/selfcheck.py
│   └── tests/
├── tests/                            # 跨子项目：unit / contract / integration
├── docs/                             # 架构文档与运行手册
├── deploy/                           # k8s 清单 + 部署脚本
├── scripts/                          # run_local / sync_common / check_secrets
├── pyproject.toml                    # 工作区：统一 lint/format/test 配置 + dev 依赖
├── Makefile
└── Dockerfile                        # 一个镜像装两个子项目，7 个 Deployment 换 command
```

## 快速开始

```bash
# 0) 安装（editable：两个子项目一起装）
make install-dev

# 1) 中间件（复用集群已有 RabbitMQ / Postgres；幂等）
export PG_DSN='postgresql://postgres:<pass>@10.96.75.0:5432/ci_triage'
ci-broker-setup --mgmt http://127.0.0.1:15672
ci-db-migrate

# 2) 起 7 个进程（1 编排 + 6 Agent），连集群中间件
bash scripts/run_local.sh

# 3) UI / API
open http://127.0.0.1:8080/          # Case 列表 / 计划 / 实时进度
curl -s http://127.0.0.1:8080/api/scenarios | python -m json.tool

# 4) 端到端验收（需要上面的栈在跑）
ORCH_BASE_URL=http://127.0.0.1:8080 PG_DSN="$PG_DSN" pytest -m integration
```

改代码后先跑（前两行完全离线；后两行只要一个 Postgres 存 checkpoint，不连 MQ）：

```bash
make test                     # 单元 + 契约（共享层一致性 / 计划 schema / 信封）
make selfcheck                # 6 个剧本 + 19 份 CI feed + 计划夹具，逐条断言
PG_DSN=... make smoke         # 编排层状态机（FakeGateway + Postgres checkpointer）
PG_DSN=... make ui-check      # 4 个 Web UI 页面：真实数据 + 空库边界各渲染一遍
```

## 计划层（编排的核心）

```text
触发信息（signal / job / signature）
        │
        ├─ 命中 registry（common/data/workflows/*.yaml）──→ 实例化 plan.yaml（source=workflow，零 LLM）
        └─ 未命中 ────────────────────────────────────────→ LLM 写同构 plan.yaml（source=llm）
                                                              │
                        safe_load → JSON Schema → V1–V8 校验 ─┘
                                     │
                    step / cursor 确定性推进（执行期零模型调用）
                                     │
                    非法/超时 → 带错误重试 1 次 → degraded 内置安全网
```

* 人工编排 = 写 `workflows/<key>.yaml`；LLM 编排 = 写 `plans/<case_id>/<plan_id>.yaml`，
  两者**同一 schema、同一校验器、同一执行器**。
* 执行期不调用模型；恢复与重放读已落盘的计划文件（DB 只存索引与 `yaml_sha256`）。

## 部署

```bash
bash deploy/deploy.sh     # selfcheck → 打镜像 → 推 Harbor → 建 Secret → apply → 等 rollout
```

7 个 Deployment 共用一个镜像，靠不同 console script 起进程；只复用集群现有
`infra-database/{rabbitmq,postgres}`。细节与踩坑记录见 [`docs/operations.md`](docs/operations.md)。

## 文档

| 文档 | 内容 |
|---|---|
| [`docs/architecture/README.md`](docs/architecture/README.md) | 架构文档索引（v1.2） |
| [`docs/architecture/01-架构总览.md`](docs/architecture/01-架构总览.md) | 拓扑、状态机、编排来源、设计原则 |
| [`docs/architecture/02-消息总线与消息契约.md`](docs/architecture/02-消息总线与消息契约.md) | exchange/queue/routing key、信封、幂等、DLQ |
| [`docs/architecture/03-编排层LangGraph设计.md`](docs/architecture/03-编排层LangGraph设计.md) | 计划层、Validator V1–V8、图定义、恢复重放 |
| [`docs/architecture/04-Mock-Agent设计.md`](docs/architecture/04-Mock-Agent设计.md) | 六剧本、夹具、契约 |
| [`docs/architecture/05-数据模型与部署演示.md`](docs/architecture/05-数据模型与部署演示.md) | 14 张表、D1–D11 演示、部署 |
| [`docs/architecture/06-边界与待确认.md`](docs/architecture/06-边界与待确认.md) | 不做清单、风险、待定项 |
| [`docs/architecture/00-代码结构与拆分.md`](docs/architecture/00-代码结构与拆分.md) | 设计条目 ↔ 代码位置、拆分与同步规则 |
| [`docs/operations.md`](docs/operations.md) | 运行手册：场景表、权限矩阵、验收数据、踩坑 |

# IQE CI 失败分诊流水线（orchestrator + agents）

CI Staging 失败分诊的可运行实现：**1 个编排层**（Python + LangGraph）与 **8 个 Mock Agent**。

业务口径来自总体流程图：**12 个阶段、两条分支、三道闸门**。阶段用「8 个 Agent × mode」承载，
共 13 个执行单元：

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

* **三道闸门**：闸门 1 决定走 internal 还是 external；闸门 2 复核「修复 + 验证」；闸门 3 复核工单。
  任一闸门都可以 `needs_info` / `rework` 回炉（jump 更早的 step，受 `MAX_REVIEW_LOOPS` 限制）或 `reject` 结案。
* **两条分支**：internal 走「自动修复 + 修复验证」，external 走「受控复现 + 回归判定 + 建单」，
  测试类失败（签名族 `test:*`）跳过受控复现，直接把取证合并进 5b。
* 知识库在 2（历史同类失败）、4a（修复方案）、6c（可疑补丁）三处检索，9 阶段回写。

Agent 业务是夹具驱动的 mock，但**消息拓扑、计划校验、分支路由、幂等、超时重试、闸门回炉、
DLQ、记账**都是真的。设计与取舍见 [`docs/architecture/`](docs/architecture/README.md)。

## 两个顶级子项目

仓库按交付边界拆成两个**自包含**子项目：各自有 `pyproject.toml`、`src/`、`tests/`、
`README.md`，可以单独安装、单独打镜像、单独发版。

| 子项目 | 包名 | 入口 | 职责 |
|---|---|---|---|
| [`orchestrator/`](orchestrator/README.md) | `iqe-orchestrator` | `ci-orchestrator` / `ci-graph-smoke` / `ci-ui-check` / `ci-broker-setup` / `ci-db-migrate` | LangGraph 状态机、计划层（workflow 优先 + LLM 兜底）、按 step_id 的路由与分支跳转、Dispatch-Await 网关、HTTP API 与 Web UI、迁移 |
| [`agents/`](agents/README.md) | `iqe-agents` | `ci-agent-{monitor,triage,reviewer,remediation,reproduction,regression,ticketing,reporting}` / `ci-selfcheck` | 8 个 Agent 的消费循环、契约校验、四层幂等、13 个 mode 的剧本与夹具 |

### 共享层：一份源码、两处副本

消息总线、DB、信封/幂等、契约、配置、路径、健康探针、日志、知识库这些底座代码在
`<子项目>/src/<pkg>/common/`。为满足「两个子项目完全独立」，**common 在每个子项目各存一份**：

* 唯一真源：`orchestrator/src/iqe_orchestrator/common/`
* 同步命令：`make sync-common`（`scripts/sync_common.sh`）
* 防漂移门禁：`tests/contract/test_common_parity.py` 对两份做 sha256 全量比对，CI 必跑

```text
.
├── orchestrator/                     # 子项目 1
│   ├── src/iqe_orchestrator/
│   │   ├── common/                   # 共享层副本（真源）
│   │   │   ├── bus.py db.py envelope.py contracts.py kb.py config.py paths.py health.py logs.py mock.py dates.py
│   │   │   ├── tools/{fingerprint,logdistiller}.py
│   │   │   └── data/{schemas,workflows,fixtures}/
│   │   ├── app.py graph.py plan.py dispatch.py scheduler.py scan.py reporting.py migrate.py main.py
│   │   ├── migrations/  ui/templates/  tools/
│   └── tests/
├── agents/                           # 子项目 2
│   ├── src/iqe_agents/
│   │   ├── common/                   # 同一份共享层副本
│   │   ├── runtime.py                # Agent 消费循环 / Ctx / 契约校验
│   │   ├── {monitor,triage,reviewer,remediation,reproduction,regression,ticketing,reporting}/
│   │   ├── migrations/  tools/selfcheck.py
│   └── tests/
├── tests/                            # 跨子项目：unit / contract / integration
├── docs/                             # 架构文档与运行手册
├── deploy/                           # k8s 清单 + 部署脚本
├── scripts/                          # run_local / sync_common / check_secrets
├── pyproject.toml                    # 工作区：统一 lint/format/test 配置 + dev 依赖
├── Makefile
└── Dockerfile                        # 一个镜像装两个子项目，9 个 Deployment 换 command
```

## 快速开始

```bash
# 0) 安装（editable：两个子项目一起装）
make install-dev

# 1) 中间件（复用集群已有 RabbitMQ / Postgres；幂等）
export PG_DSN='postgresql://postgres:<pass>@10.96.75.0:5432/ci_triage'
ci-broker-setup --mgmt http://127.0.0.1:15672
ci-db-migrate

# 2) 起 9 个进程（1 编排 + 8 Agent），连集群中间件
bash scripts/run_local.sh

# 3) UI / API
open http://127.0.0.1:8080/          # Case 列表 / 计划 / 实时进度
curl -s http://127.0.0.1:8080/api/scenarios | python -m json.tool

# 4) 端到端验收（需要上面的栈在跑）
ORCH_BASE_URL=http://127.0.0.1:8080 PG_DSN="$PG_DSN" pytest -m integration
```

改代码后先跑（前两行完全离线；后两行只要一个 Postgres 存 checkpoint，不连 MQ）：

```bash
make test                     # 单元 + 契约（共享层一致性 / 计划 schema / 计划语义）
make selfcheck                # 8 Agent × 13 mode 的行为断言 + 19 份 CI feed + 计划夹具
PG_DSN=... make smoke         # 编排层状态机（FakeGateway + Postgres checkpointer）
PG_DSN=... make ui-check      # 4 个 Web UI 页面：真实数据 + 空库边界各渲染一遍
```

## 12 阶段 demo（给具体实现人员）

`demos/` 把 12 个阶段拆成 12 个可独立运行的目录：每个目录一份**真实下发**的 `input.json`、
一份该阶段的 `output.json`，加一篇「凭什么这么判」的说明。内部链
（①→②→③→④a→⑤a→⑥a→⑨）与外部分支（①→②→③→④b→⑤b→⑥c→⑦b→⑧b→⑨）各跑一遍。

```bash
make demos                                   # 或 python demos/harness.py --all，约 2 秒
python demos/harness.py 03-gate1             # 只跑一个阶段（自动回放上游）
python demos/harness.py 03-gate1 --chain internal
```

harness 直接调用生产 handler（`iqe_agents.<stage>.playbook`）+ 内存 `FakeDB`，输出按
`schemas/stage-contracts.json` 校验。**跑通 demo 只证明剧本与契约没问题**，
消息总线 / 重试 / DLQ 仍由集成测试覆盖。索引见 [`demos/README.md`](demos/README.md)。

## 计划层（编排的核心）

```text
触发信息（signal / job / signature）
        │
        ├─ 命中 registry（common/data/workflows/*.yaml）──→ 实例化 plan.yaml（source=workflow，零 LLM）
        └─ 未命中 ────────────────────────────────────────→ LLM 写同构 plan.yaml（source=llm）
                                                              │
                        safe_load → JSON Schema → V0–V9 校验 ─┘
                                     │
                    step / cursor 确定性推进（执行期零模型调用）
                                     │
                    非法/超时 → 带错误重试 1 次 → degraded 内置安全网
```

* 分支用 `next` 表达（闸门 1 的 internal/external、非回归跳过 6c），回炉用 `jump`（只能指向更早的 step）；
  校验器 V9 强制闸门 1 双路由、闸门 2/3 的 approve 路由与 5b 的两条出口闭合，否则计划无法执行。
* step 输出按 **step_id** 存放：reviewer 在同一张计划里跑三道闸门，按 stage 存会互相覆盖。
* 人工编排 = 写 `workflows/<key>.yaml`；LLM 编排 = 写 `plans/<case_id>/<plan_id>.yaml`，
  两者**同一 schema、同一校验器、同一执行器**；执行期不调用模型。

## 部署

```bash
bash deploy/deploy.sh     # selfcheck → 打镜像 → 推 Harbor → 建 Secret → apply → 等 rollout
```

9 个 Deployment（1 编排 + 8 Agent）共用一个镜像，靠不同 console script 起进程；只复用集群现有
`infra-database/{rabbitmq,postgres}`。细节与踩坑记录见 [`docs/operations.md`](docs/operations.md)。

另有**全自包含演示镜像**（Postgres + RabbitMQ + 编排层 + 8 Agent 装进一个容器），
用于没有中间件的机器上「一条命令看完整流水线」：

```bash
bash deploy/build_allinone.sh                    # 构建并推 Harbor：iqe-agent-orchestrator/iqe-agent-orchestrator-allinone
docker run -d -p 18080:8080 <image>              # 起 PG/MQ/9 进程，等 /readyz
curl -s -XPOST localhost:18080/api/scan -H 'content-type: application/json' \
  -d '{"scenario":"new_failure","wait":true}'
```

配套清单 `deploy/k8s/30-allinone.yaml` 是参考件（**未 apply**：集群已有 `infra-database`，
两套 broker 会抢 `q.agent.*`）。说明见 [`deploy/allinone/README.md`](deploy/allinone/README.md)。

## 文档

| 文档 | 内容 |
|---|---|
| [`docs/architecture/README.md`](docs/architecture/README.md) | 架构文档索引 |
| [`docs/architecture/01-架构总览.md`](docs/architecture/01-架构总览.md) | 12 阶段两分支三闸门、拓扑、状态机、设计原则 |
| [`docs/architecture/02-消息总线与消息契约.md`](docs/architecture/02-消息总线与消息契约.md) | exchange/queue/routing key（8 个 Agent 队列）、信封、幂等、DLQ |
| [`docs/architecture/03-编排层LangGraph设计.md`](docs/architecture/03-编排层LangGraph设计.md) | 计划层、Validator V0–V9、分支/回炉、恢复重放 |
| [`docs/architecture/04-Mock-Agent设计.md`](docs/architecture/04-Mock-Agent设计.md) | 8 个 Agent × 13 个 mode 的剧本、夹具、契约 |
| [`docs/architecture/05-数据模型与部署演示.md`](docs/architecture/05-数据模型与部署演示.md) | 表的写权分工、演示流程、部署 |
| [`docs/architecture/06-边界与待确认.md`](docs/architecture/06-边界与待确认.md) | 不做清单、风险、待定项 |
| [`docs/architecture/00-代码结构与拆分.md`](docs/architecture/00-代码结构与拆分.md) | 设计条目 ↔ 代码位置、拆分与同步规则 |
| [`docs/operations.md`](docs/operations.md) | 运行手册：场景表、权限矩阵、验收数据、踩坑 |
| [`demos/README.md`](demos/README.md) | 12 阶段离线 demo：每阶段的输入/输出样例与判定规则 |

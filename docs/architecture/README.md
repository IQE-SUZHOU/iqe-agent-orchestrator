# CI 失败分诊 Agent 流水线 — 最小化架构设计（v1.2）

> 从零起炉的最小可演示架构：**6 个 Agent + 消息总线 + LangGraph 编排层**，每个单元独立部署。
> 本阶段目标：**完整演示「交互层」**（消息契约、发布/订阅、编排分支、状态推进、失败重试）。
> **6 个 Agent 均为 Mock 实现**：只做「订阅命令 → 返回结构化 Mock 数据」，不实现真实 Jenkins/Jira/复现逻辑。

状态：设计稿 ｜ 范围：设计（不含实施）｜ 读者：架构 / 平台 / Agent 开发

> **v1.1 变更**：消息总线由 NATS JetStream 改为**RabbitMQ 4.1**（topic exchange + 声明式 DLX）。
> 业务契约（subject 字符串、envelope 字段、result 三态、幂等键、路由规则）**全部不变**，只替换总线实现层；
> 新增审计影子队列 `q.audit.*` 补偿"无程序化回放"。逐项差异见 [02 §12](02-消息总线与消息契约.md)。

> **v1.2 变更**：编排层增加**两级编排来源**——预定义 workflow 优先、LLM 兜底（**无运行时人工临时编排**）。
> 人工在 `workflows/*.yaml` 定义流程（PR 评审），registry 未命中时由 LLM 写 `plans/<case_id>/*.yaml`；
> **两者产物同构（YAML）、过同一个 validator（V1–V8）、由同一个执行器消费**。
> 详见 [03 §2](03-编排层LangGraph设计.md) 与 [schemas/plan.schema.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/plan.schema.json)。

---

## 1. 一句话架构

> 编排层（Python + LangGraph）先定下“走哪几步”（一份**plan YAML**：命中预定义 workflow 则实例化，未命中则让 LLM 写同构的一份），再把**任务命令**投递到消息总线（RabbitMQ topic exchange），6 个 Agent 各自消费自己的 durable 队列、干完活把**结果**发回总线，编排层收到结果后按计划推进。

两条编排路径（由 Monitor 结果里的 `signal` 字段决定）：

| 场景 | 判定 | 走过的 Agent |
|---|---|---|
| **S1 正常路径** | Monitor 消息**不含报错**（`signal=none`） | Monitor → Triage（仅分类/打标）→ 结束 |
| **S2 异常路径** | Monitor 消息**含报错**（`signal=error`） | Monitor → Triage → Reproduction → Ticket → Reviewer → Reporting（完整串行） |

编排来源只分两级，**产物形态完全一致**（都是 plan YAML）：

| 优先级 | 来源 | 何时使用 | 产物 |
|---|---|---|---|
| 1 | **预定义 workflow**（含人工新增） | 触发 info 命中 `workflows/*.yaml`（显式 `workflow_key` 或 `match` 规则） | `plans/<case_id>/*.yaml`（`source=workflow`） |
| 2 | **LLM 自主编排**（兜底） | registry 未命中（如 `workflow_key=warming` 但无 `warming.yaml`） | 同目录同 schema（`source=llm`）；非法则重试 1 次后落 `degraded` 安全网 |

---

## 2. 部署单元（9 个，各自独立部署）

| # | 单元 | 形态 | 语言/运行时 | 对外接口 | 副本 |
|---|---|---|---|---|---|
| 1 | **message-bus**（RabbitMQ 4.1 + management） | 容器 | Erlang（官方镜像） | `5672` AMQP / `15672` UI / `15692` metrics | 1 |
| 2 | **state-store**（Postgres 16） | 容器 | — | `5432` | 1 |
| 3 | **orchestrator** | 容器（Deployment） | Python 3.12 + LangGraph | `8080` 管理 HTTP API | 1（单活） |
| 4 | **agent-monitor** | 容器 | Python | `8081` 仅 `/healthz` | 1（可 2） |
| 5 | **agent-triage** | 容器 | Python | `8081` | 1（可 N） |
| 6 | **agent-reproduction** | 容器 | Python | `8081` | 1 |
| 7 | **agent-ticket** | 容器 | Python | `8081` | 1 |
| 8 | **agent-reviewer** | 容器 | Python | `8081` | 1 |
| 9 | **agent-reporting** | 容器 | Python | `8081` | 1 |

> 其中 #1–#8 是本设计要求的**8 个独立部署单元**（6 Agent + 消息总线 + 编排层）；#2 Postgres 是编排层的状态支撑（LangGraph checkpointer 与 Case 库），不是 Agent 逻辑单元。
>
> Agent 之间**不互相调用**，也不直连编排层；一切跨单元通信只走总线。Agent 不暴露业务 HTTP，只暴露健康检查。

---

## 3. 核心设计决策（摘要）

| 决策点 | 结论 | 理由（最小化取舍） |
|---|---|---|
| 拓扑 | **星型（编排层中介）**：只有编排层决定「下一个 Agent」 | 分支/重试/审计集中在一处；Agent 可任意增删而不改链路 |
| 总线 | **RabbitMQ 4.1**（topic exchange + durable queue + DLX） | 声明式死信零代码、管理 UI 自助排障、权限可按 exchange 强制"只有编排层能发命令"；代价是无程序化回放（用审计队列 + DB 补偿） |
| 死信 | 声明式 DLX：agent 队列 → `ci.dlx` → `q.dlq` | 毒消息/重试耗尽自动隔离并留证据，无需自研转发器 |
| 编排 | **LangGraph `StateGraph` + Postgres checkpointer**；图结构固定（`step`/`advance`），路径由 plan YAML 决定 | 计划显式化，可审可回放；执行期零模型调用 |
| 编排来源 | **预定义 workflow 优先 → LLM 兜底**（两级，无第三级） | 命中定义就用定义（零模型延迟、零幻觉）；未命中才让模型补，人工通过写 workflow 参与 |
| 计划载体 | **YAML 文件**（`workflows/` 人工定义 + `plans/` 运行产物），schema 校验 | 可读可 diff 可留存；人工与模型共用同一校验器与执行器 |
| 计划校验 | JSON Schema + V1–V8（白名单/顺序/跳转/步数/防注入） | 非法计划永不执行；失败重试 1 次后落 `degraded` 安全网 |
| Agent 通信语义 | **命令队列 + 结果回投**（at-least-once）+ **幂等键** | 无需分布式事务；重启后靠重投恢复 |
| Agent 实现 | **Mock**：固定剧本 + 可调延迟/失败率 | 先把交互层跑通，后续替换成真实取证逻辑不改契约 |
| 存储 | Postgres（Case/阶段/证据/工单/日报）；证据正文存库 | 演示阶段不需要对象存储 |
| LLM | 每 Agent 每阶段**≤1 次**调用，Mock 阶段可完全不调用 | 避免模型抽风影响编排 |
| HITL | 演示阶段关闭人工批准（`TICKET_AUTOCREATE=true`），保留开关节 | 交互演示不需要人 |

---

## 4. 文档地图

| 文件 | 内容 |
|---|---|
| [00-代码结构与拆分.md](00-代码结构与拆分.md) | 代码布局、两个子项目的拆分规则、共享层真源/副本/同步/门禁、设计条目 ↔ 代码位置 |
| [01-架构总览.md](01-架构总览.md) | 组件拓扑、两条路径时序、Case 状态机、设计原则 |
| [02-消息总线与消息契约.md](02-消息总线与消息契约.md) | exchange/queue/binding 设计、subject→routing key 规则、Envelope、幂等、DLX 死信、权限矩阵、可观测性 |
| [03-编排层LangGraph设计.md](03-编排层LangGraph设计.md) | 两级编排来源（workflow / LLM）、统一 YAML 流水线、计划驱动图、State schema、Validator V1–V8、LLMPlanner、Dispatch-Await、恢复与重放 |
| [04-Mock-Agent设计.md](04-Mock-Agent设计.md) | 6 个 Mock Agent 的订阅/发布契约、Mock 剧本与夹具、行为注入 |
| [05-数据模型与部署演示.md](05-数据模型与部署演示.md) | 最小 DDL（含 workflows/plans）、目录骨架、部署拓扑、演示脚本（D1–D11）、观测 |
| [06-边界与待确认.md](06-边界与待确认.md) | 非目标、风险、开放问题、从 Mock 到真实实现的替换点 |
| [schemas/envelope.schema.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/envelope.schema.json) | 消息信封 JSON Schema |
| [schemas/stage-contracts.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/stage-contracts.json) | 6 个阶段的输入/输出契约（含 Mock 示例值） |
| [schemas/plan.schema.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/plan.schema.json) | 编排计划契约（YAML 载体）：来源、steps、on 跳转、validator 版本 + 4 个示例（S1/S2/LLM/反例） |

---

## 5. 术语

- **Case**：一个被流水线跟踪的 CI 失败/回归/异常，从发现到关闭的全生命周期实体（`case_id` = ULID）。
- **Plan**：一次 Case 的执行路径，**YAML 文件**。来源 `plan_source`：`workflow`（命中 `workflows/*.yaml`，含人工新增）或 `llm`（registry 未命中时生成）。二者结构同构，过同一 validator 与执行器。
- **Workflow**：可复用的流程定义（`workflows/<key>.yaml` = plan 的 `steps` + `key/title/enabled/priority/match`），人编写、PR 评审后发布；S1/S2 是内置条目。
- **Stage**：一次 Agent 执行。**stage id 与总线 subject 都用全称**（`monitor|triage|reproduction|ticket|review|reporting`，即 `stage_runs.stage` 值域，也是 `ci.result.<stage>` 的段名；实现里 rk 直接由 `command["stage"]` 拼出，见 `orchestrator/src/iqe_orchestrator/common/bus.py:70-71`）。历史文档里的缩写 `ci.result.reproduce` / `ci.result.report` **代码中不存在，已废弃**。映射见 02 契约表与 03 §8.0。
- **Command / Result / Event**：编排层→Agent 的任务消息 / Agent→编排层的结论消息 / 广播的领域事件。三者以**routing key**（= 完整 subject）投递到对应 exchange（`ci.cmd` / `ci.result` / `ci.event`）。
- **Signal**：Monitor 对一次构建的判定（`none` = 无报错，`error` = 有报错），是编排分支的唯一依据。
- **Signature**：失败指纹（Monitor 计算，用于去噪与查重）。

---

## 6. 明确不做（v1 边界）

- 不做真实 Jenkins 轮询/webhook、真实 Jira/Azure 写操作、真实受控重跑（全部 Mock 化）。
- 不做多总线节点、多编排实例高可用、跨机房、Kafka 级流处理。
- 不做工单人工审批回路、语义查重、Grafana 看板、权限体系（仅 env 级密钥）。
- 不做 Agent 之间的直接通信或 Agent 自行决定下游（拓扑固定为星型）。
- 不做运行时人工临时编排（人工只通过新增/修改 workflow 定义参与）、LLM 自由 DAG 或自由工具调用。

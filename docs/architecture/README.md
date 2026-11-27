# CI 失败分诊 Agent 流水线 — 架构设计

> **12 个阶段 / 两条分支 / 三道闸门**，由 **8 个 Agent × 13 个 mode** 承载；编排层用封存计划
> （YAML）驱动，Agent 之间只经 RabbitMQ 通信。8 个 Agent 均为 Mock 实现：只做
> 「订阅命令 → 按 mode 跑剧本 → 回结果」，不实现真实 Jenkins/Jira/复现逻辑。

读者：架构 / 平台 / Agent 开发。代码是唯一权威，文档与代码冲突时以代码为准。

---

## 1. 一句话架构

> 编排层（Python + LangGraph）先定下「走哪几步」（一份 plan YAML：命中预定义 workflow 则实例化，
> 未命中则让 LLM 写同构的一份），再把命令投到消息总线；8 个 Agent 各自消费自己的 durable 队列、
> 按 `payload.mode` 干完活把结果发回总线；编排层按结果里的结论词（approve / internal / regression /
> needs_info …）选下一步，分支与回炉全部由计划里的 `next` / `jump` 表达。

## 2. 12 个阶段

| 阶段 | 执行单元 | 说明 |
|---|---|---|
| 1 失败发现 | `monitor:scan` | 扫 CI feed，算指纹与签名族，去噪后判 `signal=none/error` |
| 2 根因分诊 | `triage:classify` | 分类组件/类型、检索知识库、判 internal/external、给 `duplicate_of` |
| 3 **闸门 1** | `reviewer:gate1` | 批准路径：internal 走 4a–6a，external 走 4b–8b |
| 4a 自动修复 | `remediation:fix` | 采纳知识库方案，产出改动与 ADO 工作项 |
| 5a 修复验证 | `reproduction:verify` | 应用 4a 改动重跑，给 `fix_verified` |
| 6a **闸门 2** | `reviewer:gate2` | 复核「修复 + 验证」，可 rework 回 4a |
| 4b 受控复现 | `reproduction:evidence` | 锁机 + 同参重跑 + 复现矩阵 |
| 5b 回归判定 | `regression:check` | 是否回归；非回归跳过 6c |
| 6c 二分定位 | `regression:bisect` | 定位可疑 commit/PR（检索知识库） |
| 7b 建单 | `ticketing:create` | 组装段落、定优先级/负责人、查重关联 |
| 8b **闸门 3** | `reviewer:gate3` | 复核工单，可 needs_info 回 7b |
| 9 报告与回写 | `reporting:case` / `daily` | 结案报告 / 日报，`kb_writes` 由编排层落库 |

测试类失败（签名族 `test:*`）不占重跑机位：external 分支从 5b 开始（`s3_test_no_repro`）。

## 3. 部署单元

| # | 单元 | 形态 | 对外接口 | 副本 |
|---|---|---|---|---|
| 1 | `message-bus`（RabbitMQ 4.1 + management） | 容器 | `5672` / `15672` / `15692` | 1 |
| 2 | `state-store`（Postgres 16） | 容器 | `5432` | 1 |
| 3 | `orchestrator` | Deployment | `8080` API + UI | 1（单活） |
| 4–11 | `agent-{monitor,triage,reviewer,remediation,reproduction,regression,ticketing,reporting}` | Deployment | 各自 `8080/healthz` | 各 1 |

> 9 个 Deployment = 1 编排 + 8 Agent，同一镜像换 command；中间件复用集群现有 RabbitMQ / Postgres。
> Agent 之间不互相调用，也不直连编排层；Agent 不暴露业务 HTTP，只暴露健康检查。

## 4. 核心设计决策（摘要）

| 决策点 | 结论 | 理由 |
|---|---|---|
| 拓扑 | 星型：只有编排层决定「下一个执行单元」 | 分支、回炉、审计集中在一处 |
| 阶段承载 | 8 个 Agent × mode，共 13 个执行单元 | 一个 Agent 的多种 mode 共享技能与夹具，队列与账号按 Agent 划分 |
| 总线 | RabbitMQ 4.1（topic + durable queue + 声明式 DLX） | 声明式死信零代码；权限可强制「只有编排层发命令」 |
| 编排 | LangGraph `StateGraph` + Postgres checkpointer，图结构固定 | 计划显式化、可审可回放；执行期零模型调用 |
| 计划载体 | YAML（`workflows/` 人工定义 + `plans/` 运行产物） | 可读可 diff；人工与模型共用同一 schema/校验器/执行器 |
| 计划校验 | JSON Schema + V0–V9（白名单/顺序/路由方向/回炉上限/分支闭合/防注入） | 非法计划永不执行；LLM 两次不过则降级内置安全网 |
| 分支表达 | `next`（前向，含跳过 6c）与 `jump`（回炉，只能更早） | 双分支与三闸门回炉用同一套路由语义 |
| step 输出 | 按 `step_id` 存 | 同一张计划里 reviewer 跑三次，按 stage 存会互相覆盖 |
| Agent 实现 | Mock：夹具剧本 + 可调延迟/失败率 | 先跑通交互层，替换内部逻辑不改契约 |
| 写权 | Agent 只写 `evidence`；业务表由编排层收结果后写 | 记账口径唯一，Agent 无跨表副作用 |

## 5. 文档地图

| 文件 | 内容 |
|---|---|
| [00-代码结构与拆分.md](00-代码结构与拆分.md) | 代码布局、两个子项目拆分、共享层真源/副本/门禁、设计条目 ↔ 代码位置 |
| [01-架构总览.md](01-架构总览.md) | 组件拓扑、两分支时序、状态机、设计原则 |
| [02-消息总线与消息契约.md](02-消息总线与消息契约.md) | exchange/queue/binding、subject 注册表、Envelope、幂等、DLX、权限矩阵 |
| [03-编排层LangGraph设计.md](03-编排层LangGraph设计.md) | 计划来源、统一 YAML 流水线、Validator V0–V9、分支/回炉、Dispatch-Await、恢复重放 |
| [04-Mock-Agent设计.md](04-Mock-Agent设计.md) | 8 个 Agent × 13 mode 的剧本、夹具、证据与结论词 |
| [05-数据模型与部署演示.md](05-数据模型与部署演示.md) | 表的写权分工、目录骨架、部署拓扑、演示脚本、观测 |
| [06-边界与待确认.md](06-边界与待确认.md) | 非目标、风险、从 Mock 到真实的替换点 |
| [schemas/stage-contracts.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/stage-contracts.json) | mode 归属与出入参形状（唯一权威） |
| [schemas/plan.schema.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/plan.schema.json) | 计划契约：steps / on 路由 / validator 版本 |
| [schemas/envelope.schema.json](../../orchestrator/src/iqe_orchestrator/common/data/schemas/envelope.schema.json) | 消息信封 |

## 6. 术语

- **Case**：一个被跟踪的 CI 失败/回归/异常（`case_id` = ULID），从发现到关闭的全生命周期实体。
- **Plan**：一次 Case 的执行路径（YAML）。`source=workflow`（命中 `workflows/*.yaml`）或 `source=llm`。
- **Workflow**：可复用的流程定义（`key/title/enabled/priority/match` + `steps`），人工编写、PR 评审。
- **Step**：计划里的一步 = 一个 Agent + 一个 mode（`id` 在计划内唯一，输出按 `id` 存放）。
- **Execution unit**：`stage:mode`，共 13 个（monitor 1 / triage 1 / reviewer 3 / remediation 1 /
  reproduction 2 / regression 2 / ticketing 1 / reporting 2）。
- **Verb**：result 里的结论词，路由用它选下一步：`internal/external/approve/rework/needs_info/reject/
  regression/not_regression/blocked/end`。
- **Signal**：Monitor 对一次构建的判定（`none` / `error`）。
- **Signature**：失败指纹，`component:error_code`，签名族用于去噪、查重与计划匹配。

## 7. 明确不做（v1 边界）

- 不做真实 Jenkins/Jira 写操作、真实受控重跑（全部 Mock 化）。
- 不做多总线节点、多编排实例高可用、跨机房。
- 不做人工审批 UI（闸门是 Agent 内部的结论，可在 UI 看结论但不在 UI 点批）。
- 不做 Agent 之间的直接通信或 Agent 自选下游；不做 LLM 自由 DAG。

# 文档索引

两层：**架构文档**（为什么要这样设计）与**运行手册**（怎么跑、怎么排障、踩过哪些坑）。

## 架构文档 `architecture/`

| 文档 | 读它来回答 |
|---|---|
| [00 · 代码结构与拆分](architecture/00-代码结构与拆分.md) | 设计概念落在哪个文件？为什么拆成两个子项目？共享层怎么同步？改契约要动哪几处？ |
| [README · 设计总览（v1.2）](architecture/README.md) | 一句话架构、9 个部署单元、核心决策表、术语 |
| [01 · 架构总览](architecture/01-架构总览.md) | 组件拓扑、S1/S2 两条路径时序、Case 状态机、设计原则 |
| [02 · 消息总线与消息契约](architecture/02-消息总线与消息契约.md) | exchange/queue/binding、subject → routing key、信封、幂等键、DLX、权限矩阵 |
| [03 · 编排层 LangGraph 设计](architecture/03-编排层LangGraph设计.md) | 两级编排来源、计划驱动的图、Validator V0–V8、LLMPlanner、Dispatch-Await、恢复与重放 |
| [04 · Mock Agent 设计](architecture/04-Mock-Agent设计.md) | 6 个 Agent 的订阅/发布契约、剧本与夹具、行为注入 |
| [05 · 数据模型与部署演示](architecture/05-数据模型与部署演示.md) | 14 张表 DDL、部署拓扑与 env、演示脚本 D1–D11、观测 |
| [06 · 边界与待确认](architecture/06-边界与待确认.md) | 非目标、风险清单、开放问题、从 Mock 到真实实现的替换点 |

> 文档里的路径一律按**当前代码布局**书写：`common/data/schemas/` 指包内资源目录
> （`orchestrator/src/iqe_orchestrator/common/data/schemas/`，agents 侧同构副本）。

## 运行手册

| 文档 | 内容 |
|---|---|
| [operations.md](operations.md) | 场景表、演示步骤、权限矩阵、验收数据、历史踩坑记录 |

## 与代码同步的约定

* 契约的**唯一权威**是 `common/data/schemas/` 下的 JSON 文件；文档解释它，不改写它。
* 新增/修改计划结构、subject、表结构时，先改 schema 与 `migrations/`，再回填文档。
* 拆分相关规则（共享层真源、同步命令、门禁测试）见 [00 · 代码结构与拆分](architecture/00-代码结构与拆分.md)。

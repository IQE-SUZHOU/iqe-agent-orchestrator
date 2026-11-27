# ⑨ 结案报告（reporting : case）

**一句话**：把这条 Case 从巡检到闸门的全部事实汇总成**一份人读的 Markdown 报告** +
一份**机器读的 metrics**，并给出知识库回写清单（`kb_writes`）。

阶段 9 是**只读汇总**，不做任何推理：数据全来自 Postgres（`cases` / `stage_runs` / `evidence` / `scans`）。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.reporting.case`（日报是 `ci.cmd.reporting.daily`） |
| 队列 / 绑定 | `q.agent.reporting` ← `ci.cmd.reporting.#` |
| 上游 | internal：⑥a 闸门 2；external：⑧b 闸门 3 |
| 下游 | 无（Case 终结），`kb_writes` 由编排层落库 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` | 汇总哪条 Case |
| `day` | `case` 模式恒为 `null`；日报模式传 `YYYY-MM-DD` |

**输入就这么两个字段**——报告要什么自己去 DB 查。这是刻意的：报告不能依赖上游消息里
「顺手带过来的漂亮话」，必须能从事后审计的数据里重建。

## 输出 `output.json`

| 字段 | 说明 |
|---|---|
| `report_md` | Markdown 正文：结论摘要 / 阶段执行 / 证据链 / 未解决项 |
| `metrics` | 机器读指标（见下），看板与周报都吃这个 |
| `kb_writes[]` | 知识库回写清单，`kind ∈ {failure, guilty_patch, fix, best_practice}` |
| `case_id` / `day` | 回显 |

`metrics` 的关键字段：`path` / `rca` / `confidence` / `verdicts{gate1,gate2,gate3}` /
`reproducible` / `is_regression` / `guilty_patch` / `ticket_status` / `tat_ms` /
`evidence_count` / `unresolved_count` / `stages[]`（每阶段的 status 与耗时）。

## 报告长什么样

```
# Case 结案报告 demo-case-external

## 结论摘要
- 根因：driver（置信度 0.82，组件 driver）
- 分流：external（闸门 1 approve）
- 回归：is_regression=True，guilty patch=a1b2c3d
- 工单：created OST-1654（P1 / OS-Driver，闸门 3 approve）

## 阶段执行        ← 每阶段 status / 用时 / 尝试次数
## 证据链          ← evidence 表逐条
## 未解决项        ← 例如「闸门 2 未执行」
```

## Mock 剧本怎么演的

- 报告正文由 `fixtures/reporting/template.md` 渲染，字段全部来自 DB；
- `kb_writes` 按 Case 形态生成：失败经验（`failure`）、回归定罪的补丁（`guilty_patch`）、
  修复方案（`fix`，internal 分支才有）、最佳实践（`best_practice`）；
- 日报模式（`daily`）用 `daily_metrics(day)` 汇总当天所有 Case。

## 跑一下

```bash
python demos/harness.py 09-reporting
cat demos/09-reporting/output.json | head -40
```

## 实现要点

- **Agent 不直写知识库**：只返回 `kb_writes`，由编排层落 `kb_entries`（写权分工）；
- `tat_ms`（turnaround time）是核心效能指标，必须能算出来：依赖每个 `stage_runs` 的
  `started_at/finished_at`，别在 Agent 里用「当前时间减创建时间」凑；
- `unresolved_count` 要如实统计（没走闸门 2 就写「未走修复路径」），
  报告不能说「一切顺利」；
- 报告是**审计物**：同一条 Case 重跑两次，除时间戳外结论应一致；不一致说明上游有非确定性。
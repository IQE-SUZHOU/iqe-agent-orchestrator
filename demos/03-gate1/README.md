# ③ 闸门 1（reviewer : gate1）—— 决定走哪条分支

**一句话**：检查分诊结论「够不够可信、能不能用来分流」，然后**签字**：
`decision=internal`（自己修）或 `decision=external`（提单给产品/环境侧）。

这是全流程**唯一**决定分支的地方。分诊只是建议，闸门 1 可以推翻它（改夹具即可演示）。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.reviewer.gate1` |
| 队列 / 绑定 | `q.agent.reviewer` ← `ci.cmd.reviewer.#`（三道闸门共用一个 Agent，靠 mode 区分） |
| 上游 | ② Triage |
| 下游 | `internal` → ④a 修复；`external` → ④b 受控复现 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座 |
| `rca` / `component` / `signature` / `key_frames` | 分诊结论 |
| `path` | 分诊**预判**的分支 |
| `confidence` | 分诊置信度（闸门用 0.70 作为门槛） |
| `evidence_count` | Case 已挂证据条数（编排层算好传进来） |
| `loop_seq` | 第几轮回炉（`MAX_REVIEW_LOOPS=2`） |
| `signal` / `requested_focus` | 原始信号 / 人工关注点 |

## 输出 `output.json`

必填：`case_id` / `gate` / `verdict`；关键可选：`decision` / `reason_code` / `checks` / `requested_info`。

| 字段 | 说明 |
|---|---|
| `verdict` | `approve` / `needs_info`（打回 ② 补信息）/ `reject`（终止） |
| `decision` | **只有 approve 有意义**：`internal` / `external`；其它情况是 `none` |
| `checks[]` | 机械预检明细，`id` 即规则编号（见下表） |
| `requested_info[]` | 打回时要上游补什么；编排层原样带回给 ②（`requested_focus`） |
| `quality_scores` | 证据/一致性/路径三维打分（Mock 是确定性的） |

### gate1 的四条检查

| `id` | 通过条件 | 失败时的 `reason_code` |
|---|---|---|
| `evidence_present` | 证据条数 > 0 **或** 有关键帧 | `insufficient_evidence` |
| `triage_confident` | `rca != unknown` 且 `confidence >= 0.70` | `insufficient_evidence` |
| `path_known` | `path ∈ {internal, external}` | `insufficient_evidence` |
| `loop_not_exhausted` | `loop_seq <= 2` | `loop_exhausted` |

任一条不过 → `verdict=needs_info`，`decision=none`，`requested_info` 给出要补什么。

## Mock 剧本怎么演的

- `checks` 全部是**可复算的机械规则**（不调 LLM），所以「为什么放行」随时能解释；
- 夹具 `fixtures/reviewer/gate1_internal.json` / `gate1_reject.json` 等可以覆盖结论
  （甚至反转 `decision`），用来演示「闸门不认同分诊」；
- `first_loop_only=true` 时只在第一轮覆盖，回炉后按真实规则重算——防止「永远 approve」。

## 跑一下

```bash
python demos/harness.py 03-gate1                    # external：driver 缺陷 → 提单
python demos/harness.py 03-gate1 --chain internal    # internal：toolchain 问题 → 自己修
```

两份样例：

| 文件 | 结论 | 含义 |
|---|---|---|
| `input.json` / `output.json` | `approve` + `decision=external` | 产品侧缺陷，走 ④b 复现取证 |
| `input.internal.json` / `output.internal.json` | `approve` + `decision=internal` | 自家 CI/工具链问题，走 ④a 内部修复 |

## 实现要点

- **decision 必须显式给**：编排层按 `decision` 选下一条边，缺了会当 `external` 兜底——这是最容易静默走错的地方；
- `verdict` 与 `decision` 是两个维度：`approve + internal` / `approve + external` 都合法，
  `needs_info` 时 `decision` 必须是 `none`；
- 打回（`needs_info`）要走 `jump` 回到 ②，别自己改分诊结论；
- `requested_info` 写成**可执行的动作**（「补 dmesg 尾部」而不是「证据不足」），
  它会直接变成下一轮 ② 的 `requested_focus`。
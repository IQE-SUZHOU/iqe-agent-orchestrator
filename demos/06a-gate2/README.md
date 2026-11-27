# ⑥a 闸门 2（reviewer : gate2）—— 修复验收

**一句话**：判断「修复是不是真的被验证通过了」。过了 → 直接去 ⑨ 出结案报告；
没过 → `rework` 打回 ④a 重做（有轮次上限）；明确修不了 → `reject` 终止。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.reviewer.gate2` |
| 队列 / 绑定 | `q.agent.reviewer` ← `ci.cmd.reviewer.#` |
| 上游 | ④a 修复 + ⑤a 验证（两者都挂在输入里） |
| 下游 | approve → ⑨ reporting；`rework` → ④a |

## 输入 `input.json`

在闸门 1 的基础上，多两个**完整的上游输出**：

| 字段 | 说明 |
|---|---|
| `remediation` | ④a 输出：`fixed` / `change_type` / `target` / `risk` |
| `reproduction` | ⑤a 输出（`mode=verify`）：`fix_verified` / `attempts` |
| `evidence_count` | 加上 ④⑤ 落的证据后的总数 |
| `loop_seq` | 第几轮（`rework` 每次 +1，上限 2） |

## 输出 `output.json`

必填：`case_id` / `gate` / `verdict`；关键可选：`reason_code` / `checks` / `requested_info`。

### gate2 的六条检查

| `id` | 通过条件 | 失败时的 `reason_code` |
|---|---|---|
| `fix_applied` | `remediation.fixed == true` | `missing_repro` |
| `change_target` | `remediation.target` 非空 | `insufficient_evidence` |
| `verification_present` | 存在 verify 输出且 `attempts > 0` | `missing_repro` |
| `fix_verified` | `reproduction.fix_verified == true` | `fix_not_verified` |
| `evidence_present` | `evidence_count > 0` | `insufficient_evidence` |
| `loop_not_exhausted` | `loop_seq <= 2` | `loop_exhausted` |

结论映射：

| 情况 | verdict | 编排动作 |
|---|---|---|
| 全部通过 | `approve` | 走 `reporting:case` 结案 |
| `fix_verified` 不过 | `rework` | 回炉 ④a（`requested_info` 带重跑要求） |
| `fixed=false` / 缺 verify | `rework` | 回炉 ④a 补修复或补验证 |
| 回炉超限 / 明确不可修 | `reject` / `loop_exhausted` | 终止，转人工 |

## Mock 剧本怎么演的

- 全部按上表机械计算，`verdict` 来自规则而不是夹具（防止「永远 approve」）；
- 夹具 `fixtures/reviewer/gate2_rework.json` 用来演示 `rework` 时的两条 `requested_info`
  （重跑要求 + 补证据要求）；
- `quality_scores.checks_passed/checks_total` 是「6/6」这种可直接看懂的指标。

## 跑一下

```bash
python demos/harness.py 06a-gate2
```

## 实现要点

- 闸门 2 **不看**分诊置信度，只看「修了没有 + 验了没有」——两个问题不要混；
- `rework` 必须给可执行的 `requested_info`，编排层会把它作为 `requested_focus` 带回 ④a；
- 回炉次数上限（`MAX_REVIEW_LOOPS=2`）一定要卡住，否则会出现「修不好就一直修」的死循环；
- `reject` 与 `rework` 的区别：`rework` 是「再试一次能好」，`reject` 是「自动化到此为止」。
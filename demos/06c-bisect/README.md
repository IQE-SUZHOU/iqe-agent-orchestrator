# ⑥c guilty patch 定位（regression : bisect）—— external 分支

**一句话**：在 ⑤b 给出的变更区间里做二分，收敛到**具体哪个提交/PR 引入的**，
给出 `guilty_patch`、作者与置信度，供 ⑦b 建单时直接 @ 责任人。

只在 `is_regression=true` 时才跑；没收敛也允许返回 `bisected=false`（不硬编一个嫌疑人）。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.regression.bisect` |
| 队列 / 绑定 | `q.agent.regression` ← `ci.cmd.regression.#`（与 check 同一个 Agent） |
| 上游 | ⑤b 回归判定（`baseline` + `change_window` 是它的输出） |
| 下游 | ⑦b 建单 |

## 输入 `input.json`

在 ⑤b 的基础上多了两个字段（由编排层从 check 的输出里取出）：

| 字段 | 说明 |
|---|---|
| `baseline` | 已知通过的基线（二分下界） |
| `change_window` | 怀疑区间 `a..b`（二分上界） |
| `reproduction` | ④b 输出，确认「确实能稳定复现」才有二分意义 |

## 输出 `output.json`

必填：`case_id` / `mode` / `is_regression`。

| 字段 | 说明 |
|---|---|
| `mode` | 固定 `bisect` |
| `bisected` | 是否收敛 |
| `guilty_patch` | 收敛时 `{commit, pr, author, confidence, rationale}`；**未收敛必须为 `null`** |
| `candidates` | 二分过程中剩下的候选（收敛后为空） |
| `notes` | 二分轮次与判断依据 |
| `evidence` | `regression_bisect` 证据 |

## Mock 剧本怎么演的

- `fixtures/regression/default.json` → 收敛到 `a1b2c3d`（`PR-1187` / `wang.lei` / 0.86），
  `rationale` 写明「在该提交上复现、父提交上通过」；
- `inconclusive.json` → `bisected=false`、`guilty_patch=null`，`notes` 里只把知识库
  `KB-PATCH-001` 作为**参考**列出，不当结论；
- 真实实现是「按区间中点构建 → 跑复现用例 → 折半」，受机器数与时间预算约束。

## 跑一下

```bash
python demos/harness.py 06c-bisect
```

## 实现要点

- **没收敛就别猜**：`guilty_patch=null` + `bisected=false` 是合法结论，
  硬给一个 commit 会把工单指错人（信誉成本极高）；
- `confidence` 要反映二分质量（测了几轮、结果是否一致），0.86 意味着「高但非绝对」；
- 二分要设**轮次/时间上限**，否则一个大区间能跑掉一整天的机位；
- 定罪的提交要带 `pr` 与 `author`，⑦b 靠它决定 assignee 和工单正文。
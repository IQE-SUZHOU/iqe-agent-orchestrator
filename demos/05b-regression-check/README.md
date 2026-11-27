# ⑤b 回归判定（regression : check）—— external 分支

**一句话**：回答一个问题——**这是新引入的回归吗？** 也就是「上次基线是好的，这次坏了」。
是回归才值得花机器去二分（⑥c），不是回归就走「环境问题 / 偶发」的结论直接提单。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.regression.check` |
| 队列 / 绑定 | `q.agent.regression` ← `ci.cmd.regression.#` |
| 上游 | ④b 受控复现（取证结论） |
| 下游 | `is_regression=true` → ⑥c 二分；`false` → 直接 ⑦b 建单 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座 |
| `rca` / `component` / `signature` | 分诊结论，`signature` 是查基线的键 |
| `reproduction` | **④b 的完整输出**（`reproducible` 决定能不能比） |
| `requested_focus` | 人工关注点 |

## 输出 `output.json`

必填：`case_id` / `mode` / `is_regression`。

| 字段 | 说明 |
|---|---|
| `mode` | 固定 `check` |
| `is_regression` | 是回归 `true`，否 `false`（环境/偶发） |
| `baseline` | 对比的基线标识（如 `staging-pass-2026-11-24`） |
| `change_window` | 怀疑区间 `a1b2c3d..f4e5d6a`，⑥c 直接用这两个端点 |
| `candidates` | 区间内的候选提交（`check` 只列，不定罪） |
| `guilty_patch` | **check 阶段必须为 `null`**，定罪是 ⑥c 的事 |
| `bisected` | check 阶段固定 `false` |
| `notes` | 判定理由，人读 |
| `evidence` | `regression_check` 证据 |

## Mock 剧本怎么演的

- 夹具 `fixtures/regression/default.json` → 是回归，3 个候选提交；
- `no_regression.json` → `is_regression=false`，`notes` 里给「环境问题」判断，编排层据此跳过 ⑥c；
- `inconclusive.json` → 基线缺失/不可比，交给 ⑥c 给「未收敛」结论；
- 基线本身来自历史「同签名最后一次全绿」的运行（真实实现要查基线库）。

## 跑一下

```bash
python demos/harness.py 05b-regression-check
```

## 实现要点

- **不要在这里定罪**：`guilty_patch` 留空，否则 ⑥c 的二分结果会和它冲突；
- `reproducible != yes` 时不该判回归（都复现不出来，谈不上「曾经好过」）；
- `change_window` 要找**能收敛**的区间：区间太大二分代价高，区间太小容易漏；
- 判 `false` 要给理由（环境变更 / 已知 flaky / 用例本身不稳定），否则下游无法解释「为什么不查了」。
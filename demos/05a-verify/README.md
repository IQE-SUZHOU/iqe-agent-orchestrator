# ⑤a 修复验证（reproduction : verify）—— internal 分支

**一句话**：把 ④a 的改动应用上去，用**原参数**重跑同一批用例，比对失败签名是否消失，
给出 `fix_verified`。这是闸门 2 唯一的「事实来源」。

注意：这**不是**复现取证（那是 ④b `evidence` 模式）。同一个 Agent、同一个队列，
靠 `payload.mode` 区分：`verify` 关心「修好了没有」，`evidence` 关心「能不能复现」。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.reproduction.verify` |
| 队列 / 绑定 | `q.agent.reproduction` ← `ci.cmd.reproduction.#` |
| 上游 | ④a 修复 |
| 下游 | ⑥a 闸门 2 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座（跑哪个 job 必须与失败时一致） |
| `rca` / `component` / `signature` / `key_frames` | 分诊结论；`key_frames` 是比对基准 |
| `remediation` | **④a 的完整输出**（`fixed` / `change_type` / `target`） |

## 输出 `output.json`

必填：`case_id` / `mode` / `reproducible`；关键可选：`fix_verified` / `attempts` / `matrix`。

| 字段 | 说明 |
|---|---|
| `mode` | 固定 `verify` |
| `reproducible` | `yes` / `no` / `timeout`（修复后是否仍能复现原问题） |
| `fix_verified` | **本阶段的核心结论**：`true` 才可能过闸门 2 |
| `attempts` | 重跑次数（Mock 为 3，防止单次抖动误判） |
| `matrix` | 每次重跑的 `{os, config, outcome}`，`config=patched` |
| `failing_tests` / `passing_tests` | 用例级结果，报告里会列 |
| `new_evidence` | 每次重跑一条 `repro_log` 证据，UI 可逐次追溯 |
| `log_excerpt` | 比对结论摘要（原错误簇是否再出现） |
| `blocked_reason` | 机位不可用等原因；非空表示这次没跑成 |

## Mock 剧本怎么演的

- 夹具 `fixtures/repro/default.json` 的 `verify` 段：3 次全 `pass` → `fix_verified=true`；
- `fixed_not_verified.json` 演示「改了但没修好」→ `fix_verified=false` → 闸门 2 `rework`；
- `not_reproducible.json` → 原问题都复现不出来，`reproducible=no`；
- `timeout.json` / `blocked_no_machine.json` 演示 `sleep_s` 超时与锁机失败（编排层重试 → DLQ）。

## 跑一下

```bash
python demos/harness.py 05a-verify
```

## 实现要点

- **必须用原参数重跑**：换机器、换参数、换用例集的「验证」等于没验证；
- **至少 3 次**：一次通过可能是 flaky，`attempts` 是可观测指标，别为了快改成 1；
- `fix_verified=false` 时不要自己改结论「大概修好了」，直接让闸门 2 打回 ④a；
- 锁机失败要明确 `blocked_reason`（可重试），不要伪装成「验证不通过」（不可重试）；
- 每次重跑都要落一条证据，事后要能回答「第几次开始通过的」。
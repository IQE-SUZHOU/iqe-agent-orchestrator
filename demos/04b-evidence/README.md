# ④b 受控复现取证（reproduction : evidence）—— external 分支

**一句话**：把失败在受控环境里**再打一遍**，证明「这不是偶发、确实能稳定重现」，
并留下可复核的取证材料（复现矩阵、dmesg、失败用例列表）。这是后续提单能站住脚的依据。

和 ⑤a `verify` 是同一个 Agent 的两个 mode：`evidence` 回答「能不能复现」，`verify` 回答「修好没有」。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.reproduction.evidence` |
| 队列 / 绑定 | `q.agent.reproduction` ← `ci.cmd.reproduction.#` |
| 上游 | ③ 闸门 1 `decision=external` |
| 下游 | ⑤b 回归判定 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座，`job` 决定复现哪个任务 |
| `rca` / `component` / `signature` | 分诊结论 |
| `key_frames` | **比对基准**：复现时的日志要能对上这些帧 |
| `requested_focus` | 人工关注点（如「补 8 卡机型复现」） |

注意这里**没有** `remediation`——外部路径不修，只取证。

## 输出 `output.json`

必填：`case_id` / `mode` / `reproducible`；关键可选：`attempts` / `matrix` / `fix_verified`。

| 字段 | 说明 |
|---|---|
| `mode` | 固定 `evidence` |
| `reproducible` | `yes` / `no` / `timeout`：能否稳定重现 |
| `fix_verified` | **本模式必须为 `null`**（没修过，无从验证） |
| `attempts` | 复现次数（Mock 为 2） |
| `matrix` | 每次 `{os, config, outcome}`，`config=default`、`outcome=fail` |
| `failing_tests` | 稳定失败的用例清单，会原样进工单 |
| `steps_summary` | 取证步骤（锁机 → 重建 → 跑用例 → 抓 dmesg → 释放机位） |
| `new_evidence` | 每次一条 `repro_log` 证据 |
| `blocked_reason` | 机位不可用 / 超时等原因；非空说明没跑成 |

## Mock 剧本怎么演的

- 夹具 `fixtures/repro/default.json` 的 `evidence` 段：2 次全 `fail` → `reproducible=yes`；
- `not_reproducible.json` → `reproducible=no`（提单前就知道站不住脚）；
- `timeout.json` / `blocked_no_machine.json` 用 `sleep_s` / `raise_blocked` 演示超时与锁机失败；
- `rca=infrastructure` 直接 `Blocked(disabled)`：基础设施类问题不占重跑机位。

## 跑一下

```bash
python demos/harness.py 04b-evidence
```

## 实现要点

- **同一台机器、同一参数**重建才叫复现；换环境复现不出不能写 `no`，要写 `blocked`；
- `fix_verified` 必须是 `null`：填 `true/false` 会让闸门 2/3 的逻辑读错模式；
- 复现矩阵要带 `os` 与 `config`：只报「复现了」无法定位环境相关性；
- 锁机是**外部资源**：一定要 `finally` 释放，否则机位泄漏比失败本身更麻烦；
- 取证证据要能离线复核（dmesg 尾部、console 片段），不能只留一句「复现成功」。
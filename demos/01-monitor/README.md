# ① Monitor 巡检发现（monitor : scan）

**一句话**：定期扫一遍 CI 最近的构建，把「值得调查的失败」挑出来，噪声与已知问题直接压掉，
压剩下的每条失败晋升成一个 Case。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.monitor.scan` |
| 队列 / 绑定 | `q.agent.monitor` ← `ci.cmd.monitor.#` |
| 触发方 | 编排层 `POST /api/scan`，或内置定时器（`MONITOR_SCAN_INTERVAL_S`） |
| 结果 | `ci.result.monitor` |

Monitor 是**唯一没有 `case_id`** 的阶段：Case 是它扫出来之后由编排层创建的。

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `scan_id` | 本次扫描 id，编排层生成；观测的 `obs_id` 由它派生，便于回溯 |
| `lookback` | 回看窗口，如 `15m` / `2h` / `1d` |
| `jobs_filter` | 只扫指定 job；空数组 = 全扫 |

## 输出 `output.json`

必填：`scan_id` / `observations` / `counts`；可选：`scan_window` / `source_status`。

`observations[]` 里下游真正依赖的字段：

| 字段 | 作用 |
|---|---|
| `signal` | `error` / `flaky` / `none`；**只有 `error` 才可能晋升** |
| `signature` | `组件:错误码:指纹`，查重、知识库匹配、回归基线的主键 |
| `noise_score` + `known_issue_ref` | 噪声判据：命中已知问题且 `noise_score >= 0.85` → 抑制 |
| `suppressed` + `context.suppressed_reason` | 被压掉的原因：`known_issue_noise` / `duplicate_signature_merge` |
| `promoted_reason` | 晋升原因：`new_failure_signature`（新签名）/ `unstable_result`（flaky、abort） |
| `context` | 自由扩展；Mock 用 `context.mock_scenario` 给后续阶段选剧本 |
| `log_excerpt` / `evidence_refs` | 失败段日志与证据引用（`db://evidence/N`） |

## Mock 剧本怎么演的

数据源是 `agents/src/iqe_agents/common/data/fixtures/ci_feed/*.json`，
**feed 名就是 `scenario`**（`new_failure` / `mixed` / `timeout` / `duplicate_merge` …）。判定顺序：

1. `result=SUCCESS` → `signal=none`，不晋升；
2. 签名族命中 `fixtures/known_issues.json` 且 `noise_score >= 0.85` → 抑制（`known_issue_noise`）；
3. 同一 feed 内重复签名 → 只留一条，其余 `duplicate_signature_merge`；
4. 其余 `signal=error` → 晋升；`UNSTABLE` / `ABORTED` 的 `promoted_reason=unstable_result`。

## 跑一下

```bash
python demos/harness.py 01-monitor                  # new_failure → driver:crash
python demos/harness.py 01-monitor --chain internal # mixed → toolchain:compile-error
```

## 实现要点

- **幂等**：同一 `scan_id` 重复投递不能造出重复观测，用 `obs_id` 去重；
- 别在这里做根因判断，判断是 ② Triage 的事；Monitor 只回答「哪条失败值得看」；
- `counts`（`observed/promoted/suppressed`）是运维指标，会在 `/ui/health` 展示，别少算；
- `context.mock_scenario` 是 Mock 专用开关。接真实 CI 时删掉，换成 `failed_stage`、
  `locked_resource`、`console_url` 这类真实上下文。
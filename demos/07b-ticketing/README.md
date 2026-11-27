# ⑦b 建单（ticketing : create）—— external 分支

**一句话**：把前面所有结论（根因、复现矩阵、回归区间、guilty patch）整理成一张**能被别人接手**的工单，
路由到责任团队，并按规则定优先级。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.ticketing.create` |
| 队列 / 绑定 | `q.agent.ticketing` ← `ci.cmd.ticketing.#` |
| 上游 | ④b 复现 + ⑤b/⑥c 回归 + ② 分诊（重复 Case 判定） |
| 下游 | ⑧b 闸门 3 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座 |
| `rca` / `component` / `signature` / `key_frames` | 分诊结论 |
| `signal` / `path` | 原始信号与 ③ 闸门 1 的决策（应恒为 `external`） |
| `reproduction` | ④b 输出：`reproducible` / `matrix` / `failing_tests` |
| `regression` | ⑤b 或 ⑥c 的输出（优先取 bisect）：`is_regression` / `guilty_patch` |
| `duplicate_of` | ② 判定的重复 Case（有则不另建单） |
| `requested_focus` | 人工关注点 |

## 输出 `output.json`

必填：`case_id` / `backend` / `ticket_status`。

| 字段 | 说明 |
|---|---|
| `backend` | `jira-mock`（真实实现换 Jira / ADO） |
| `ticket_status` | `created` / `drafted`（`TICKET_AUTOCREATE=false`）/ `linked_duplicate` |
| `external_key` / `url` | 工单号与链接；草稿态是 `DRAFT-xxxxxx` |
| `priority` | `P1` / `P2` / `P3`（规则见下） |
| `assignee_team` | 责任团队（如 `OS-Driver`），闸门 3 会检查非空 |
| `linked_patches` | 关联的 guilty commit 列表 |
| `duplicate_of` | 判重时指向原 Case |
| `sections[]` | 工单正文分段，**必须含 `rca` / `repro` / `evidence`** |
| `audit` | 建单审计信息（backend / autocreate / signature / scenario） |

### 优先级规则（Mock）

| 条件 | 优先级 |
|---|---|
| 是回归且已定位 guilty patch | `P1` |
| `rca=driver` 且可复现 | `P1` |
| `rca=test` / `environment` 且可复现 | `P2` |
| 不可复现 / flaky | `P3` |
| 其它 | `P2` |

## Mock 剧本怎么演的

- 后端是假的 Jira：`external_key=OST-<数字>`，正文由夹具 + 上游结论拼装；
- 完整 payload 会落一条证据（`ticket_payload`），闸门 3 与事后审计都从这里复核；
- 夹具 `fixtures/ticketing/duplicate.json` 演示判重 → `linked_duplicate`，不新建单。

## 跑一下

```bash
python demos/harness.py 07b-ticketing
```

## 实现要点

- 工单要**自解释**：接手人只看这张单就能开始查，别写「见 CI 链接」；
- `sections` 的 `key` 是契约：`rca` / `repro` / `evidence` 缺一段，闸门 3 直接 `needs_info` 打回；
- 建单是**有副作用**的操作，必须幂等（同 `case_id` 重复投递不能开两张单）——
  真实实现要用 `case_id` 做外部 id；
- `TICKET_AUTOCREATE=false` 时只产草稿，人工确认后再提交：这是生产默认的稳妥做法。
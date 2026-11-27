# ⑧b 闸门 3（reviewer : gate3）—— 建单验收

**一句话**：工单开出去之前最后一道关：**这张单能不能被别人直接接手？**
段落齐不齐、责任团队有没有、重复关系自不自洽。

过了 → ⑨ 出结案报告；不过 → `needs_info` 打回 ⑦b 补正文（不是打回 ④ 重查）。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.reviewer.gate3` |
| 队列 / 绑定 | `q.agent.reviewer` ← `ci.cmd.reviewer.#` |
| 上游 | ⑦b 建单（+ ④b 复现、⑤b/⑥c 回归，一起挂在输入里做交叉校验） |
| 下游 | approve → ⑨ reporting；`needs_info` → ⑦b |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `ticketing` | **⑦b 的完整输出**（`ticket_status` / `sections` / `assignee_team` / `duplicate_of`） |
| `reproduction` | ④b（`mode=evidence`）输出，用于核对工单里的复现结论 |
| `regression` | ⑤b/⑥c 输出，用于核对 guilty patch 与优先级 |
| `loop_seq` | 第几轮（上限 2） |

## 输出 `output.json`

必填：`case_id` / `gate` / `verdict`；关键可选：`reason_code` / `checks` / `requested_info`。

### gate3 的五条检查

| `id` | 通过条件 | 失败时的 `reason_code` |
|---|---|---|
| `ticket_status` | `ticket_status ∈ {created, drafted, linked_duplicate}` | `insufficient_evidence` |
| `ticket_sections` | `sections` 含 `rca` / `repro` / `evidence` 且内容非空 | `missing_ticket_sections` |
| `owner_assigned` | `assignee_team` 非空 | `wrong_owner` |
| `duplicate_consistent` | 有 `duplicate_of` 时状态必须是 `linked_duplicate` | `duplicate_ticket` |
| `loop_not_exhausted` | `loop_seq <= 2` | `loop_exhausted` |

任一条不过 → `needs_info` + `requested_info`（告诉 ⑦b 补哪一段），超过轮次则转人工。

## Mock 剧本怎么演的

- 全部机械校验，**不看内容质量**（质量是 ⑦b 的责任，闸门只管「齐不齐、对不对」）；
- 夹具 `fixtures/reviewer/gate3_needs_info.json` 演示缺段落 → `needs_info`；
- `quality_scores.checks_passed/checks_total`（5/5）用于看板展示。

## 跑一下

```bash
python demos/harness.py 08b-gate3
```

## 实现要点

- 闸门 3 **不重新判根因**：根因错了是 ②③ 的问题，这里只验「交付物完整」；
- `sections[].key` 是契约（`rca` / `repro` / `evidence`），改 key 名等于让闸门永远打回；
- `drafted` 也算通过：草稿态允许人工确认后提交，别把「没自动建单」当失败；
- 有 `duplicate_of` 却不是 `linked_duplicate` 是典型 bug（新建了单又标了重复），必须拦。
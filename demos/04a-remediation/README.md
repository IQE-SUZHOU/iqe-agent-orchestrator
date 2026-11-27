# ④a 内部修复（remediation : fix）—— internal 分支

**一句话**：既然是自家 CI / 环境 / 工具链的问题，就给出**可落地的改动**（改哪个文件、怎么改、风险多大），
再提一个 ADO 工作项留痕，交给 ⑤a 验证。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.remediation.fix` |
| 队列 / 绑定 | `q.agent.remediation` ← `ci.cmd.remediation.#` |
| 上游 | ③ 闸门 1 `decision=internal` |
| 下游 | ⑤a 修复验证 → ⑥a 闸门 2 |
| 回炉 | 闸门 2 判 `rework` 时回到这里（带上 `requested_focus`） |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座 |
| `rca` / `component` / `signature` / `key_frames` | 分诊结论 |
| `kb_fix_refs` | ② 命中过的知识库条目 id（有就优先采纳） |
| `requested_focus` | 闸门 2 打回时的重做要求；第一轮为 `null` |

## 输出 `output.json`

必填：`case_id` / `fixed` / `change_type` / `target` / `diff_summary`。

| 字段 | 说明 |
|---|---|
| `fixed` | 是否给出可用修复。`false` 时闸门 2 一定 `rework`（`missing_repro`） |
| `change_type` | `script` / `config` / `test` / `data` |
| `target` | 改哪个文件/配置（闸门 2 的 `change_target` 检查它非空） |
| `diff_summary` | 人读的改动摘要，写进结案报告与工单 |
| `risk` | `low` / `medium` / `high`，高风险的改动应当转人工 |
| `work_item_key` / `work_item_url` | 变更留痕（Mock 是 `ADO-xxxx`） |
| `adopted_from_kb` | 采纳的知识库修复条目；没有则为 `null` |
| `evidence` | `remediation_plan` 证据，⑥a 与 ⑨ 都会引用 |

## Mock 剧本怎么演的

1. **`rca=driver` 不自动修**（产品代码缺陷），直接 `fixed=false`，交人工/外部路径；
2. 修复形态按组件推导：`ci-infra` / `apt-repo` / `install` → `script`，
   `toolchain` / `network` / `image-build` → `config`，`test` → `test`，其余 → `data`；
3. 命中知识库 `fix` 条目则 `adopted_from_kb=KB-FIX-xxx`，并把来源写进 `notes`；
4. 夹具 `fixtures/remediation/<scenario>.json` 可整体覆盖（当前 demo 用 `default`，
   覆盖成 `script` + `ci/scripts/staging_install.sh` + `ADO-4821` + `risk=low`）；
   `rollback` 演示回退，`insufficient` 演示给不出方案。

## 跑一下

```bash
python demos/harness.py 04a-remediation
```

## 实现要点

- **不要改产品代码**：`rca=driver` 就该 `fixed=false` 转外部；越界自动改内核代码是事故来源；
- `target` 与 `diff_summary` 必须自洽，闸门 2 只看 `target` 非空，人看 `diff_summary`——
  两者打架时没人能审；
- 提工单（ADO work item）是**留痕**，不是审批：审批在闸门 2；
- 回炉重做时（`requested_focus` 非空）要在 `notes` 里显式回应上一轮要求，否则闸门 2 会再次打回。
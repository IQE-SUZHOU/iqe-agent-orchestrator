# ② Triage 分诊定级（triage : classify）

**一句话**：拿到一条失败观测，回答三个问题——**根因是什么（rca）**、**该谁修（internal / external）**、
**有多大把握（confidence）**，顺带查重并把证据落库。

这一阶段的输出是后面所有阶段的共同前提：④ 修复、⑤ 复现都拿它的 `rca / component / key_frames`。

## 消息

| 项 | 值 |
|---|---|
| 路由键 | `ci.cmd.triage.classify` |
| 队列 / 绑定 | `q.agent.triage` ← `ci.cmd.triage.#` |
| 上游 | ① Monitor 晋升出一条 Case 后由编排层派发 |

## 输入 `input.json`

| 字段 | 说明 |
|---|---|
| `case_id` / `build` / `source` / `job` | 公共底座（Case + 观测） |
| `signal` | 一定是 `error`（flaky 不建 Case） |
| `signature` | `组件:错误码:指纹`，用于匹配签名族与知识库 |
| `log_excerpt` | 失败段日志原文，剧本会先「蒸馏」再判定 |
| `context` | 观测上下文（`failed_stage` / `locked_resource` …） |
| `evidence_refs` | 上游已挂的证据引用 |
| `requested_focus` | 人工指定关注点；闸门 1 打回时带回来（如「补关键帧」） |

## 输出 `output.json`

必填：`case_id` / `rca` / `path` / `confidence` / `evidence`。

| 字段 | 说明 |
|---|---|
| `rca` | `driver` / `test` / `environment` / `infrastructure` / `unknown` |
| `component` | 更细的归属：`driver` / `test` / `toolchain` / `ci-infra` … |
| `path` | **预测**走哪条分支（`internal` / `external`）——只是建议，闸门 1 才有决定权 |
| `confidence` | 0~1；`< 0.70` 会被闸门 1 判 `needs_info` 打回重判 |
| `key_frames` | 关键日志帧，④⑤ 复现时要照着它比对 |
| `kb_refs` | 命中的知识库条目（如 `KB-FIX-002`），④a 修复会优先采纳 |
| `duplicate_of` | 命中历史重复 Case 时给出原 Case id |
| `evidence` | 蒸馏后日志 + 知识库查询记录（`distilled_log` / `kb_lookup`） |
| `skill_refs` / `llm_calls` | 用了哪张 SKILL 卡片 / 调了几次 LLM（Mock 为 0） |

## Mock 剧本怎么演的

- 规则表：`fixtures/triage/rules.json`（组件 → 根因、`path_by_component`、置信度）；
- **内外部归属**：`toolchain` / `test` / `ci-infra` → `internal`；`driver` 等产品侧 → `external`
  （`INTERNAL_COMPONENTS`，也可用 `force_path` 或 `scenario` 强制，演示「闸门 1 反转预判」）；
- `duplicate_of`：命中已存在的同签名 Case 时填；
- 日志先过 `LogDistiller`（Mock 版本按行过滤），再抽关键帧。

## 跑一下

```bash
python demos/harness.py 02-triage                   # driver → external / 0.82
python demos/harness.py 02-triage --chain internal   # toolchain → internal / 0.72
```

两份样例：`input.json`（外部，driver:crash）与 `input.internal.json`（内部，toolchain:compile-error）。

## 实现要点

- `path` **只做预判**，不要在这里「直接分流」——否则闸门 1 形同虚设；
- `confidence` 必须是可解释的数字：接 LLM 后要用「特征命中 + 相似历史案例」算，不要拍脑袋；
- `key_frames` 要能支撑复现：给的是能区分「同一失败」的日志行，不是随便截几行；
- 分诊结论会写进 DB（`case_runs` 的 `triage:classify` 输出），闸门 1 与报告都从这里取，
  **别只写日志**。
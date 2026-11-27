# 04 Mock Agent 设计（8 个消费者 × 13 个 mode）

图中的 12 个阶段由 **8 个部署单元 × 13 个执行单元（stage:mode）** 承载：

| 阶段 | 承载 | 阶段 | 承载 |
|---|---|---|---|
| 1 失败发现 | `monitor:scan` | 6a 修复复核 | `reviewer:gate2` |
| 2 分诊 | `triage:classify` | 6c 补丁定位 | `regression:bisect` |
| 3 分流闸门 | `reviewer:gate1` | 7b 建单 | `ticketing:create` |
| 4a 自动修复 | `remediation:fix` | 8b 工单闸门 | `reviewer:gate3` |
| 4b 受控复现 | `reproduction:evidence` | 9 报告 | `reporting:case` |
| 5a 修复验证 | `reproduction:verify` | 9 日报 | `reporting:daily` |
| 5b 回归判定 | `regression:check` | | |

---

## 1. 设计原则

1. **一个 Agent 只做一件事**：订阅自己的命令队列 → 按 `payload.mode` 跑对应剧本 → 把 result 发回总线。
   它不决定下游是谁（星型拓扑，编排层决定），也不暴露业务 HTTP，只有 `/healthz`。
2. **剧本是纯函数**：`async def handler(ctx: Ctx) -> dict`，全部输入来自 `ctx.env`、`ctx.db`、`ctx.fixtures`、
   `ctx.kb`，不直接碰总线；因此可以脱离 MQ/DB 单测（`ci-selfcheck` 就是这么跑的）。
3. **业务是 Mock，骨架是真的**：消息拓扑、契约校验、四层幂等、超时重试、回炉、DLQ、证据落库都是真实代码；
   只有"结论从哪来"换成了确定性夹具。
4. **写权最小化**：Agent 只写 `evidence` 与 `msg_dedup`；业务表（cases / reviews / tickets / kb_entries …）
   由编排层收到 result 后写。Reporting 想把经验写进知识库，只能放进输出的 `kb_writes`。
5. **结论词优先于夹具**：闸门的机械预检不过时恒为 `needs_info` / `rework`；夹具只能在"机械结论为 approve"时
   把结论改严（或 `force=true` 强制覆盖），**不能把该拒绝的改成通过**。

---

## 2. 一个 Agent 的运行骨架

```
consume(q.agent.<stage>, prefetch=N)
  ├─ schema 不合法 / mode 不属于本 stage / TTL 过期 ──→ nack(requeue=False) → ci.dlx → q.dlq
  ├─ msg_dedup claim(idempotency_key) 已存在 ────────→ ack（重复投递，不重跑）
  ├─ stage_runs 判 already_done（同 key 已有结果）──→ 复用结果直接发布
  ├─ ctx = Ctx(env, db, fixtures, kb, sleep, evidence)
  ├─ output = await playbook.handler(ctx)           # 剧本内按 payload.mode 分派
  │    ├─ raise Blocked(code, detail) → status=blocked + evidence("blocked", …)
  │    └─ 正常返回 → validate_stage_contract(stage, "output") 校验形状
  ├─ publish(ci.result.<stage>, status=ok, output, idempotency_key 原样带回)
  │    └─ 等 publisher confirm；失败则不 ack，让 broker 重投
  └─ ack
```

`ctx.sleep()` 会先 `sleep(min(MOCK_DELAY_MS 区间随机值, …))`，用来制造"真实耗时"；
剧本里显式写的 `sleep_s` 用于演示"编排层超时 → retryable → 重试耗尽 → blocked/DLQ"。

---

## 3. 8 个 Agent

### 3.1 monitor（阶段 1）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.monitor.scan` / `ci.result.monitor` |
| 输入 | `scan_id` `lookback` `jobs_filter[]`（可选 `source`） |
| 输出 | `scan_id` `observations[]` `counts` `scan_window` `source_status` |
| 剧本 | 读 Mock CI 流水 → 逐 build 算签名与签名族 → 去噪/合并 → 产出观测 |
| 结论 | 每 build：`signal=none|error`；error 再判 `suppressed` + `promoted_reason` |
| 证据 | `scan`（每条观测一条，含日志摘要；全文进 evidence 表） |
| 夹具 | `fixtures/ci_feed/*.json`（19 份） |

确定性规则：

```
SUCCESS                     → signal=none，signature=null（不占下游）
FAILURE/UNSTABLE/ABORTED    → signal=error，然后：
  a) 签名族命中 known_issues.json 且 noise_score >= 0.85 → suppressed(known_issue_noise)
  b) 同一次扫描内同签名族已有未抑制 error → 只保留 build 号最大者（duplicate_signature_merge）
  c) 其余 promoted；UNSTABLE/ABORTED 的 promoted_reason=unstable_result
场景文件缺失                → Blocked(source_unavailable)（只有 scenario=default 才允许回落 default.json）
```

`counts` 里固定带 `total/success/error/suppressed/promoted`，编排层用它判断"这轮要不要建 Case"。

### 3.2 triage（阶段 2）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.triage.classify` / `ci.result.triage` |
| 输入 | `signal` `signature` `log_excerpt` `context` `evidence_refs[]`（回炉时带 `requested_focus`） |
| 输出 | `rca` `path` `component` `os_stage` `signature` `duplicate_of` `confidence` `key_frames[]` `kb_refs[]` `evidence[]` |
| 剧本 | LogDistiller 蒸馏 → 组件 → 根因/置信度（`rules.json`）→ internal/external 预判 → 知识库检索 |
| 证据 | `triage`（关键帧与命中的知识条目） |
| 技能 | `triage/skills/*.md`（按指纹族引用的分类知识，`skill_refs` 回填） |

`path` 只是**预判**，最终由闸门 1 的 `decision` 覆盖。组件→路径的默认表：

| component | rca | path |
|---|---|---|
| `ci-infra` / `apt-repo` / `install` / `test` / `toolchain` | 自动化侧 | `internal` |
| `driver` / `image-build` / `network` / `timeout` / `unknown` | 产品/环境侧 | `external` |

置信度低于阈值（`triage` 夹具或规则给出的 `unknown_low_confidence` 场景）时，`rca=unknown`，
闸门 1 会判 `needs_info` 并回炉重分诊。

### 3.3 reviewer（阶段 3 / 6a / 8b）

一个 Agent 三道闸门，靠 `payload.mode` 区分；每道闸门只回答一个问题，输出统一为
`gate` `verdict` `reason_code` `requested_info[]` `checks[]` `quality_scores`（可选 `decision`）。

| 闸门 | 问题 | 合法结论 | 失败时 | 回炉目标 |
|---|---|---|---|---|
| `gate1` | 分诊结论能不能用来分流？ | `approve` + `decision=internal\|external` | `needs_info` | `triage:classify` |
| `gate2` | 修复是否真的被验证通过？ | `approve` | `rework` | `remediation:fix` |
| `gate3` | 工单是否具备可交付质量？ | `approve` | `needs_info` | `ticketing:create` |

`reject` 在任意闸门都可用（→ 计划里的 `end`，终态 `closed_rejected`）。

#### 机械预检（`checks[i]`，`id` 即契约编号）

| 闸门 | check id | 通过条件 | 失败 `reason_code` |
|---|---|---|---|
| gate1 | `evidence_present` | `evidence_count > 0` 或 `key_frames` 非空 | `insufficient_evidence` |
| gate1 | `triage_confident` | `rca != unknown` 且 `confidence >= 0.70` | `insufficient_evidence` |
| gate1 | `path_known` | `path ∈ {internal, external}` | `insufficient_evidence` |
| gate2 | `fix_applied` | `remediation.fixed = true` | `missing_repro` |
| gate2 | `change_target` | `remediation.target` 非空 | `insufficient_evidence` |
| gate2 | `verification_present` | `reproduction.mode = verify` 且已执行 | `missing_repro` |
| gate2 | `fix_verified` | `reproduction.fix_verified = true` | `fix_not_verified` |
| gate2 | `evidence_present` | `evidence_count >= 2` | `insufficient_evidence` |
| gate3 | `ticket_status` | `created` / `drafted` / `linked_duplicate` | `insufficient_evidence` |
| gate3 | `ticket_sections` | `rca` / `repro` / `evidence` 三段都在且非空 | `missing_ticket_sections` |
| gate3 | `owner_assigned` | `assignee_team` 非空 | `wrong_owner` |
| gate3 | `duplicate_consistent` | 有 `duplicate_of` 时状态必须是 `linked_duplicate` | `duplicate_ticket` |
| 全部 | `loop_not_exhausted` | `loop_seq <= MAX_REVIEW_LOOPS` | `loop_exhausted` |

`requested_info` 是给人看的中文补件说明（如"补一次带修复的完整重跑（verify 模式）"），
回炉时被编排层写进 `requested_focus` 传给上游，上游剧本会把它写进 notes/证据。

### 3.4 remediation（阶段 4a）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.remediation.fix` / `ci.result.remediation` |
| 输入 | `rca` `component` `signature` `key_frames[]` `kb_fix_refs[]` `requested_focus` |
| 输出 | `fixed` `change_type` `target` `diff_summary` `work_item_key` `work_item_url` `risk` `adopted_from_kb` |
| 剧本 | 按知识库修复方案生成改动 → 提 ADO 工作项 → 记风险 → 交 5a 验证 |

规则：

1. `rca=driver`（产品代码缺陷）不在自动化修复范围 → `fixed=false`，人跟进（闸门 2 会打回/结束）。
2. 修复形态由组件决定：`ci-infra`/`apt-repo`/`install` → `script`；`toolchain`/`network` → `config`；
   `test` → `test`；其余 → `data`。
3. 命中知识库 `fix` 条目则 `adopted_from_kb=<entry_id>`，否则按模板生成。
4. `requested_focus` 非空（闸门 2 打回）→ 记为"本轮重做"，notes 里带上要求原文。

### 3.5 reproduction（阶段 4b / 5a）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.reproduction.evidence\|verify` / `ci.result.reproduction` |
| 输入 | `rca` `component` `signature` `key_frames[]`；`verify` 额外带 `remediation` |
| 输出 | `mode` `reproducible` `attempts` `matrix[]` `steps_summary[]` `failing_tests[]` `passing_tests[]` `fix_verified` `blocked_reason` `log_excerpt` |
| 证据 | 每次重跑一条 `repro_log`（UI 可按 attempt 逐次追溯）+ 一条 `repro_plan` |

规则：

1. `evidence`：锁机 → 同参重建 → 复现矩阵（默认 3 次，`REPRO_MAX_RUNS`）→ `reproducible=yes|no|flaky`。
2. `verify`：应用 4a 改动 → 重跑 → `fix_verified=true|false`（失败时 `reproducible` 保留观测值）。
3. `rca=infrastructure` 在 `evidence` 模式直接 `Blocked(disabled)`：基础设施类不占重跑机位。
4. 段内 `raise_blocked` → `Blocked(code, detail)`（如 `source_unavailable` 机位被占用）。
5. 段内 `sleep_s` → 真 sleep，用于演示"超时 → retryable → 重试耗尽 → blocked"。

### 3.6 regression（阶段 5b / 6c）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.regression.check\|bisect` / `ci.result.regression` |
| 输入 | `check`：`reproduction` 的取证结论；`bisect`：额外 `baseline` `change_window` |
| 输出 | `check`：`is_regression` `baseline` `change_window` `candidates[]`；`bisect`：`bisected` `guilty_patch{commit,pr,author,confidence,rationale}` `notes[]` |
| 知识库 | `bisect` 未收敛时，把历史同类补丁作为参考写进 `notes` |

规则：

1. 同配置历史基线比对 → `is_regression=true` 则路由 6c（bisect）；`false` 直接建单，工单正文注明"长期环境问题"。
2. `bisect` 在变更区间内二分；未收敛时 `guilty_patch=null`（闸门 3 仍可过，但工单质量分会降）。
3. 夹具场景：`default`（回归）/ `no_regression`（跳过 6c）/ `inconclusive`（未收敛）。

### 3.7 ticketing（阶段 7b）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.ticketing.create` / `ci.result.ticketing` |
| 输入 | `rca` `path` `reproduction` `regression` `duplicate_of`（可带 `requested_focus`） |
| 输出 | `backend` `external_key` `url` `ticket_status` `duplicate_of` `priority` `assignee_team` `linked_patches[]` `sections[]` `audit` |
| 副作用 | 完整 payload 落 `evidence`，闸门 3 靠它复核 |

规则：

1. `TICKET_AUTOCREATE=false` → `ticket_status=drafted`，`external_key=DRAFT-<case前6位>`。
2. 夹具或上游 `duplicate_of` 非空 → `linked_duplicate`，不另建单。
3. 优先级：回归且有 guilty patch → `P1`；driver + 可复现 → `P1`；test/environment + 可复现 → `P2`；
   不可复现或 flaky → `P3`；其余 → `P2`。
4. `sections` 至少含 `rca` / `repro` / `evidence`（闸门 3 校验），另带 `regression` / `impact` / `next_steps`。

### 3.8 reporting（阶段 9）

| 项 | 内容 |
|---|---|
| 命令 / 结果 | `ci.cmd.reporting.case\|daily` / `ci.result.reporting` |
| 输入 | `case`：`case_id`；`daily`：`day` |
| 输出 | `case`：`report_md` `metrics` `kb_writes[]`；`daily`：`report_md` `metrics` |
| 数据源 | 全部是 Postgres（cases / stage_runs / evidence / scans / daily_reports），剧本不做推理 |

`kb_writes` 的生成条件（Agent 只出清单，编排层落 `kb_entries`）：

| 条件 | 条目 `kind` | 摘要内容 |
|---|---|---|
| `triage.rca != unknown` | `failure` | rca + 路径 + 置信度 + 分诊备注 |
| `remediation.fixed = true` | `fix` | change_type → target + diff 摘要 |
| `regression.guilty_patch.commit` 存在 | `guilty_patch` | commit / PR / author / 依据 |

---

## 4. 证据与写权

| 谁 | 写什么 | 表 |
|---|---|---|
| Agent | 动作证据、幂等 claim、阻塞原因 | `evidence`、`msg_dedup` |
| 编排层 | Case、阶段运行、闸门结论、修复、验证、回归、工单、知识库条目 | `cases` `stage_runs` `reviews` `remediations` `verifications` `regressions` `tickets` `kb_entries` |

证据 `kind`：`scan` / `triage` / `review` / `fix` / `repro_plan` / `repro_log` / `regression` / `ticket` / `report` / `blocked`。
一条动作一条证据，`payload` 里带夹具场景名，便于"这条结论是哪份夹具出的"。

---

## 5. 幂等与可靠性（四层）

| 层 | 机制 | 重复时 |
|---|---|---|
| 消息级 | `msg_dedup` 按 `idempotency_key` claim | 直接 ack，不重跑 |
| 阶段级 | `stage_runs` 判 `already_done` | 复用已有结果重新发布 |
| 副作用级 | 工单/复现动作先查 backref（`tickets.external_key`） | 返回已有键，不重复建 |
| 发布级 | 先 publish 拿 confirm，再 ack | 发布失败不 ack → broker redeliver |

**毒消息**：schema 不合法、`mode` 不属于本 stage、TTL 过期无法处理 → `nack(requeue=False)` →
`ci.dlx` → `q.dlq`（编排层 `_dlq_reason` 二次判定原因并落库）。
**业务阻塞**：`Blocked` 是正常结论，走 `status=blocked` 的 result，不进 DLQ。

---

## 6. 夹具清单

| 目录 | 场景 | 演示什么 |
|---|---|---|
| `ci_feed/` | `default` `new_failure` `many_errors` `mixed` `all_success` `known_issue_noise` `duplicate` `duplicate_merge` `flaky_unstable` `aborted_infra` `blocked_no_machine` `timeout` `not_reproducible` `needs_info` `reject` `insufficient` `unknown_low_confidence` `llm_warming` `llm_invalid` | Monitor 的签名/去噪/抑制 + 端到端各分支入口 |
| `triage/` | `rules.json`（组件→rca/路径/置信度表）、`scenarios.json`、`sample_failure.log` | 分类的确定性规则 |
| `reviewer/` | `default` `gate1_internal` `gate1_external` `gate1_reject` `gate2_rework` `gate3_needs_info` | 三道闸门的各结论与回炉 |
| `remediation/` | `default` `insufficient` `rollback` | 修复 / 无法修复 / 回退 |
| `repro/` | `default` `flaky` `not_reproducible` `fixed_not_verified` `blocked_no_machine` `timeout` | 复现矩阵、验证失败、机位占用、超时 |
| `regression/` | `default` `no_regression` `inconclusive` | 回归 / 跳过 6c / 二分未收敛 |
| `ticketing/` | `default` `duplicate` | 正常建单 / 关联重复单 |
| `reporting/` | `template.md` | 日报模板 |
| `plans/` | `internal_ok` `external_ok` `invalid_stage` `invalid_order` | LLM 兜底：合法两份 + 非法两份 |
| `kb/` | `entries.json` | 2 / 4a / 6c 三处检索、9 回写 |

---

## 7. 离线自检（`ci-selfcheck`）

不连 MQ/DB，用 `MemoryDB` + `Fixtures` 直接构造 `Ctx`，对 13 个 mode 逐一断言：

```
13 个 mode × 场景（默认 + 每个分支夹具）→ 输出形状 / 结论词 / 证据条数 / 阻塞码
19 份 CI feed                         → 观测条数、抑制原因、promoted_reason
计划契约与语义                        → V0–V9、workflow 命中、LLM 夹具、degraded 安全网
```

`selfcheck` 的 `MOCK_DELAY_MS` 固定为 `"0:0"`（只能是 `lo:hi` 形式，写成 `0-0` 会静默回落到默认 `150:500`），
所以 144 项断言总耗时 < 1s；超时用例靠夹具里的 `sleep_s=45` 制造真实超时。

---

## 8. 换真实实现要改哪里

| Agent | 替换点 |
|---|---|
| monitor | `_load_feed` → Jenkins REST（`builds` / `wfapi` + JUnit），签名算法不变 |
| triage | LogDistiller 之后接模型判定；知识库换向量检索 |
| reviewer | 机械预检保留（它是护栏），模型只填 `quality_scores` 与 `notes` |
| remediation | git 工作区改动 + ADO REST + 变更审批流 |
| reproduction | Jenkins Lockable Resources 锁机 + 触发重跑 + 抓 console + 指纹比对 |
| regression | 基线库查询 + 逐提交触发流水线二分 + 提交元数据关联 |
| ticketing | Jira REST（`POST /issue`、issue links） |
| reporting | 报告模板与推送渠道（邮件/IM），`kb_writes` 逻辑不变 |

剧本签名（`handler(ctx) -> dict`）与契约（`stage-contracts.json`）都不变 —— 换的是数据来源，不是骨架。

---

## 9. 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `SVC` | `unknown` | 单元名（日志/指标维度，如 `agent-triage`） |
| `MOCK_SCENARIO` | `default` | 剧本分支（命令可用 `scenario_hint` 覆盖） |
| `MOCK_DELAY_MS` | `150:500` | 模拟耗时区间（`lo:hi` 毫秒） |
| `MOCK_FAILURE_RATE` | `0.0` | 注入随机失败，验证重试与 DLQ |
| `NOISE_THRESHOLD` | `0.85` | triage/monitor 噪声抑制阈值 |
| `REPRO_ENABLED` / `REPRO_MAX_RUNS` | `true` / `3` | 复现开关与重跑上限 |
| `TICKET_BACKEND` / `TICKET_AUTOCREATE` | `jira-mock` / `true` | 工单后端与是否真建单 |
| `MAX_REVIEW_LOOPS` | `2` | 闸门回炉上限（与编排层同名） |

完整清单见 `common/config.py`。

---

相关文档：[01 架构总览](01-架构总览.md) · [02 消息总线与消息契约](02-消息总线与消息契约.md) ·
[03 编排层设计](03-编排层LangGraph设计.md) · [05 数据模型与部署演示](05-数据模型与部署演示.md)

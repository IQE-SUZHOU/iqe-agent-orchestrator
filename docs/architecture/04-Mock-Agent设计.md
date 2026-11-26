# 04 六个 Agent 设计（Mock 实现）

## 0. 共同骨架

每个 Agent 容器内部结构完全一致，只有「剧本」不同：

```
agent-<stage>/
 ├── main.py            # 启动：连 RabbitMQ → 声明/校验 queue+binding → consume → /healthz
 ├── contract.py        # 输入/输出校验（jsonschema，来自 schemas/）
 ├── mock_playbook.py   # 剧本：按 scenario 产出 Mock output
 ├── skills/            # SKILLs：*.md playbook（演示阶段只是被读取并回显 skill_id）
 └── tools/             # 工具适配层（演示阶段全部 mock：jenkins/jira/jarvis/logdistiller）
```

统一消费循环（示意）：

```python
async def handle(msg: aio_pika.IncomingMessage):
    env = decode(msg.body)
    if env["schema"] != "v1":             return await reject_to_dlq(msg, "schema_mismatch")  # nack(requeue=False)
    if env["ttl_ms"] < age(env):          return await ack_result(msg, status="expired")
    if await already_done(env):           return await msg.ack()          # 幂等层 2
    if not claim(env["idempotency_key"]): return await msg.ack()          # 幂等层 1
    await sleep(mock_delay(env))                                           # 模拟耗时
    if mock_should_fail(env):             return await publish_result(msg, status="retryable",
                                                  error={"code": "mock_fault"})
    out = playbook.produce(env)                                            # Mock 结论
    await save_evidence(env, out)                                          # 写 Postgres evidence
    await publish_result(msg, status="ok", output=out)                     # publisher confirm 成功后
    await msg.ack()                                                        # 才 ack，反之让其 redeliver
```

> 关键顺序：**先发布 result（等 confirm）再 ack 命令**。若 `publish_result` 抛错，不 ack，broker 会 redeliver，幂等层吸收重复。
> 消费者启动时用 `channel.set_qos(prefetch_count=<见 02 §2.2>)`，并断言自己的 queue/binding 已存在（漏声明直接退出）。

### 行为注入（演示控制面，全部 env 可配）

| env | 含义 | 演示默认 |
|---|---|---|
| `MOCK_SCENARIO` | `normal` / `error` / `flaky` / `needs_info` / `reject` / `fault` | 由触发脚本指定（见 05 §4） |
| `MOCK_DELAY_MS` | 模拟处理耗时（可 `min:max` 随机） | `200:900` |
| `MOCK_FAILURE_RATE` | 注入 `retryable` 失败的概率 | `0.0`（故障演示时 `0.6`） |
| `MOCK_LLM_ENABLED` | 是否真调模型（演示关闭，走剧本） | `false` |
| `MOCK_FEED_SOURCE` | Monitor 读取的假 CI 数据文件路径 | `/fixtures/ci_feed.json` |
| `MOCK_EVIDENCE_COUNT` | 每次产出的证据条数 | `2` |

### SKILLs / Jarvis CLI / LogDistiller 在 Mock 阶段的形态

| 组件 | Mock 形态 | 契约位置 |
|---|---|---|
| **SKILLs** | `skills/*.md` 诊断手册；Agent 按 `job→skill` 映射选中一个，output 里回显 `skills_used:[skill_id]` | Triage |
| **Jarvis CLI** | `tools/jarvis.py` 只读白名单桩：`device_status` / `log_fetch`，返回固定 JSON，调用记录写 `evidence` | Triage |
| **LogDistiller** | `tools/logdistiller.py`：把 Mock console 压成「错误簇 + 首尾上下文」，带硬预算（默认 6000 字符） | Triage / Reproduction / Reporting |
| **LLM 判定** | 由剧本直接给 `rca`+`confidence`（`MOCK_LLM_ENABLED=true` 时才真调模型） | Triage / Reviewer |

---

## 1. Monitor Agent（监控）

| 项 | 内容 |
|---|---|
| 订阅 | queue `q.agent.monitor` ← rk `ci.cmd.monitor.scan` |
| 发布 | exchange `ci.result` → rk `ci.result.monitor` |
| 外部依赖（Mock） | `MOCK_FEED_SOURCE` 假构建列表（替换点：真实 Jenkins REST 轮询） |

内部流程：
1. 读 feed → 得到构建列表（job、build、result、console 摘要、changeSet）。
2. **去噪**：`known_issue` 命中（本地缓存 signature，来自 `ci.event.case.*`）→ 标 `noise_score` 高；`result=SUCCESS` → `signal=none`；`ABORTED/UNSTABLE` → 按规则升为 `error`。
3. **签名**：`sha1(job + 归一化错误簇 + stage)` → `signature`。
4. **排优先级**：新增失败（历史无此 signature）= 高优先；历史复发 = 低优先；批量同 signature 合并为一条 observation。
5. 输出 observation 列表（每条含 `signal`），**编排层据此分支**。

Output（`ci.result.monitor`）：

```json
{
  "scan_window": {"from": "2026-11-25T09:00:00Z", "to": "2026-11-25T09:05:00Z"},
  "observations": [
    {
      "job": "staging-drv-gfx1151-smoke", "build": 128, "url": "http://jenkins/job/…/128",
      "signal": "none", "result": "SUCCESS",
      "signature": "sig-3f2a1c", "noise_score": 0.15, "priority": 3,
      "summary": "构建成功，12 stages / 8m32s，无告警"
    },
    {
      "job": "staging-drv-gfx1151-p1-regression", "build": 77, "url": "http://jenkins/job/…/77",
      "signal": "error", "result": "FAILURE", "failed_stage": "test-kernel-mode",
      "signature": "sig-9b71de", "first_seen": true, "noise_score": 0.05, "priority": 1,
      "summary": "新增失败：test-kernel-mode 12/12 FAIL，console 含 PAGE_FAULT 错误簇 ×47"
    }
  ],
  "counts": {"scanned": 10, "errors": 1, "suppressed": 2}
}
```

失败与降级：feed 不可读 → `status=blocked, code=source_unavailable`；解析异常单条 → 跳过该条并在 `counts.skipped` 计数。

---

## 2. Triage Agent（分类）

| 项 | 内容 |
|---|---|
| 订阅 | queue `q.agent.triage` ← rk `ci.cmd.triage.run` |
| 发布 | exchange `ci.result` → rk `ci.result.triage`（**不发布事件**：`ci.event.triage.classified` 在 V1 代码里没有任何发布点，已从事件登记移除，见 02 §3.1 / 06 §6） |
| 工具（Mock） | LogDistiller、Jarvis CLI 桩、SKILL Playbook、历史查询（Postgres） |

输入关键字段：`case`、`mode`（`full` / `classify_only`）、`outputs`（上游结论）、`extra_request`（Reviewer 回炉时要求补充的证据）。

内部流程：
1. **收集证据**：console（Mock）→ LogDistiller 蒸馏；job 配置摘要、changeSet、最近 N 次结果史、JUnit 摘要；按需 `jarvis device_status` / `jarvis log_fetch`。
2. **选 SKILL**：`job 前缀/failed_stage → skill_id`（如 `driver-hang.md`、`env-lock.md`）。
3. **分类根因**：`rca ∈ driver | test | environment | infrastructure | flaky | unknown` + `confidence`。
4. **复现资格判定**：`repro_eligible` + `repro_plan`（`environment/infrastructure` 通常不给复现资格）。
5. `classify_only` 模式：只做 1–3 步（蒸馏 + 分类 + 噪声标签），**不产出 repro_plan、不请求 Jarvis**，明确轻量。

Output（`mode=full`）：

```json
{
  "rca": "driver", "confidence": 0.78,
  "category_detail": "GPU page fault in kernel mode after resume",
  "skills_used": ["driver-hang.md"],
  "evidence_refs": [{"kind": "distilled_log", "uri": "db://evidence/1042"},
                    {"kind": "jarvis:device_status", "uri": "db://evidence/1043"}],
  "distilled_chars": 5120, "raw_chars": 240000,
  "history": {"same_signature_last_seen": "2026-11-21", "occurrences": 3},
  "suspected_change": {"commit": "a1b2c3", "file": "amdgpu/resume.c"},
  "repro_eligible": true,
  "repro_plan": {"action": "jenkins_rebuild_same_params", "job": "staging-drv-gfx1151-p1-regression",
                 "base_build": 77, "lock_resource": "sut-gfx1151-01", "max_runs": 2},
  "summary": "驱动 resume 路径回归，疑似 a1b2c3 引入；测试与环境证据不足"
}
```

Output（`mode=classify_only`，场景 S1）：

```json
{ "rca": "none", "confidence": 0.95, "noise_score": 0.12,
  "skills_used": [], "evidence_refs": [{"kind": "distilled_log", "uri": "db://evidence/1050"}],
  "repro_eligible": false, "summary": "构建成功，无需分诊；已计入观测台账" }
```

失败与降级：证据不足 → `rca=unknown, confidence≤0.4` 并照常返回（Reviewer 会驳回）；工具超时 → 丢弃该证据源并记 `evidence_gaps`。

---

## 3. Reproduction Agent（复现）

| 项 | 内容 |
|---|---|
| 订阅 | queue `q.agent.reproduction` ← rk `ci.cmd.reproduction.run` |
| 发布 | exchange `ci.result` → rk `ci.result.reproduction`（段名用全称，不是 `ci.result.reproduce`） |
| 工具（Mock） | Jenkins rebuild 桩、锁机桩、制品清单桩 |

输入：`case` + `outputs.triage.output.repro_plan` + `extra_request`。

内部流程：
1. **资格闸门**：`repro_eligible=false` 或 `rca ∈ {infrastructure}` → 返回 `reproducible=blocked`（不重跑）。
2. **受控重跑**：同参 rebuild ×`max_runs`（Mock：生成 `run_id` 列表 + 模拟结果），每次先取锁、跑完释放。
3. **指纹比对**：重跑 console 蒸馏后与原 signature 比对 → `match_score`。
4. **附加证据**：额外日志、JUnit、制品清单入 `evidence`。
5. **可复现性判定**：`yes`（全部命中）/ `partial`（部分）/ `no`（全部不复现）/ `blocked`（无资格或资源不可用）；`flaky_score` = 不稳定度。

Output：

```json
{
  "reproducible": "partial", "match_score": 0.81, "flaky_score": 0.33,
  "runs": [
    {"seq": 1, "build": 78, "result": "FAILURE", "signature_match": true,  "duration_s": 412},
    {"seq": 2, "build": 79, "result": "FAILURE", "signature_match": true,  "duration_s": 405},
    {"seq": 3, "build": 80, "result": "SUCCESS", "signature_match": false, "duration_s": 398}
  ],
  "lock": {"resource": "sut-gfx1151-01", "held_s": 1230},
  "artifacts_added": 3,
  "summary": "3 次重跑 2 次复现原错误簇，存在环境相关抖动"
}
```

护栏（Mock 也保留开关，便于后续接真实实现）：`REPRO_ENABLED=false` 时立即返回 `blocked(code=disabled)`；只允许 allowlist job 前缀 `staging-`；`max_runs ≤ 3`。

---

## 4. Ticket Agent（工单）

| 项 | 内容 |
|---|---|
| 订阅 | queue `q.agent.ticket` ← rk `ci.cmd.ticket.run` |
| 发布 | exchange `ci.result` → rk `ci.result.ticket`（**不发布事件**：人工批准回路需要的 `ci.event.ticket.drafted` 在 V1 没有任何发布点，见 02 §3.1 / 06 §6） |
| 工具（Mock） | Jira/Azure 桩（`JiraSearch` / `JiraCreate` / `AzureSearch`）返回假 key/URL |

内部流程：
1. **查重**：`signature` 精确匹配 → 工单台账；再按标题/错误簇关键词做关键词匹配（Mock：从 fixture 取一张已存在单）。命中 → `ticket_status=linked_duplicate`，不新建。
2. **组装草稿**：标题（job + 现象 + signature 短码）、RCA 摘要、复现步骤（来自 repro）、证据链接、优先级、影响面、指派建议。
3. **优先级与指派**：规则表 `(rca × reproducible × priority) → (ticket_priority, assignee_team)`。
4. **创建**：`TICKET_AUTOCREATE=true`（演示默认）→ 直接返回 mock 单号；`false` → 只回 `ticket_status="drafted"`（`external_key="DRAFT-<case前6位>"`，`playbook.py:105`），**不发事件**（`ci.event.ticket.drafted` V1 未实现），人工批准入口因此只能靠 Case 详情里的工单状态，见 06。

指派规则表（演示）：

| rca | reproducible | → priority | → assignee |
|---|---|---|---|
| driver | yes/partial | P1 | `DRV-CORE` |
| driver | no | P2 | `DRV-CORE` |
| test | any | P2 | `QA-IQE` |
| environment | any | P3 | `LAB-OPS` |
| infrastructure | any | P2 | `CI-INFRA` |
| flaky | yes | P3 | `QA-IQE` |
| unknown | any | P2 | `TRIAGE-LEAD` |

Output：

```json
{
  "ticket_status": "created", "backend": "jira", "external_key": "IQE-4821",
  "url": "https://jira.local/browse/IQE-4821", "ticket_priority": "P1",
  "assignee_team": "DRV-CORE", "duplicate_of": null,
  "title": "[staging-drv-gfx1151-p1-regression#77] kernel-mode page fault on resume (9b71de)",
  "sections": {"rca": true, "repro_steps": true, "evidence": 5, "history": true},
  "summary": "已建单 IQE-4821，附 RCA/复现步骤/5 条证据，查重未命中"
}
```

查重命中示例：

```json
{ "ticket_status": "linked_duplicate", "backend": "jira", "external_key": "IQE-4610",
  "url": "https://jira.local/browse/IQE-4610", "duplicate_of": "IQE-4610",
  "assignee_team": "DRV-CORE", "ticket_priority": "P1",
  "summary": "signature 命中已有单 IQE-4610，已关联并追加本次 occurrence" }
```

---

## 5. Reviewer Agent（审核）

| 项 | 内容 |
|---|---|
| 订阅 | queue `q.agent.review` ← rk `ci.cmd.review.run` |
| 发布 | exchange `ci.result` → rk `ci.result.review` |
| 独立性 | **不读** Triage 的推理文本，只读 `evidence_refs` + 各阶段结构化 output；独立 prompt / 可配独立模型 `REVIEW_MODEL` |

机械预检（实现对照 `agents/review/playbook.py`；**顺序与 `id` 即契约编号**，编排层与自测都按 `checks[i].id` 取值）：

| # | `checks[].id` | 检查内容 | 失败 `reason_code` | 失败动作 |
|---|---|---|---|---|
| 1 | `evidence_present` | Case 已有证据引用 | `insufficient_evidence` | `needs_info` |
| 2 | `evidence_count_ok` | 证据条数 ≥ `MIN_EVIDENCE`=2 | `insufficient_evidence` | `needs_info` |
| 3 | `triage_confidence_ok` | `rca ≠ unknown` 且 `confidence ≥ CONFIDENCE_MIN`=0.70 | `missing_triage` | `needs_info` |
| 4 | `repro_conclusion` | 复现段已给出 `reproducible` 结论（判据见下） | `missing_repro` | `needs_info` |
| 5 | `ticket_sections_nonempty` | 工单关键段落（`rca` / `repro` / `evidence`）齐且非空 | `missing_ticket_sections` | `needs_info` |
| 6 | `no_duplicate_ticket` | 不得既有重复工单又继续开单 | `duplicate_ticket` | 首轮 `needs_info`；**第 2 轮仍未澄清 → `reject`** |
| 7 | `signal_repro_consistent` | `signal` 与复现结论自洽（`none` 不得 `yes/flaky`；`error` 不得 `blocked`） | `inconsistent_evidence` | `needs_info` |
| 8 | `loop_not_exhausted` | 回炉轮次 ≤ `loop_max`（= `MAX_REVIEW_LOOPS`，默认 2） | `loop_exhausted` | `needs_info`（编排层据此把 Case 收尾为 `blocked`） |

**第 4 / 5 条的「跳过」语义（实现期定稿）**：

- 「不适用」**不是新字段、也不是新的 status**：该项照样写进 `checks[]`，`pass = true`，`detail` 里写明跳过原因，`reason_code = null`（`_check()` 在 `pass=true` 时把 `reason_code` 强制置 null，见 `playbook.py:63-65`）。
- **#4 跳过条件**：本 Case 的计划里没有 reproduction 段。判据是编排层注入的输入 `repro_attempted = ("reproduction" in outputs)`（`orchestrator/src/iqe_orchestrator/graph.py:455`）。跳过时 `detail = "计划未包含复现段，跳过"`。只有 `signal=error` 且 `repro_attempted=true` 而 `reproducible` 为空时才算真失败（`missing_repro`）。
- **#5 跳过条件**：没有工单段，判据是输入 `ticket_status is None`（取自库里 `tickets` 行，`graph.py:449-451`）。跳过时 `detail = "计划无 ticket，跳过"`。
- 所以 `checks[]` **恒定 8 项**（契约上限 20），`checks_passed + checks_failed = 8` 恒成立。要看"有没有做复现/开单"，请看 `detail` 里的「跳过」或 `stage_runs` 里有没有该 stage，**不要**用"检查条数"推断。

`checks[]` 元素的字段（权威形状 `playbook.py:63-65`）：

```json
{ "id": "evidence_count_ok",              // 稳定 id，编排层/自测/日报按它取值
  "name": "证据条数 >= 2",                 // 人读名称
  "pass": false,                          // 「跳过」也是 true
  "detail": "证据 1 条（要求 >= 2）",       // 截断 220 字；跳过时写「…，跳过」
  "reason_code": "insufficient_evidence" // pass=true（含跳过）时恒为 null
}
```

`verdict` 规则：全部 `pass` → `approve`（`reason_code="ok"`）；有失败 → `needs_info`，`reason_code` 取**第一个失败项**的 `reason_code`；#6 失败且 `loop_seq ≥ 2` → `reject`；#8 失败优先于夹具剧本，固定 `needs_info` + `loop_exhausted`。夹具 `fixtures/review/<scenario>.json` 的 `override` 可直接指定 `verdict / reason_code / requested_info`（演示 `reject`、永远 `needs_info`）。

Output（实现形状；`requested_info` 是**字符串数组**，最多 10 条，来自 `_REQUEST_INFO` 表）：

```json
{
  "case_id": "01J8ZQ3C0AWF4H7S5G1JB9KTX2",
  "verdict": "needs_info",
  "reason_code": "missing_repro",
  "requested_info": ["补充受控重跑记录与 reproducible 结论"],
  "checks": [
    {"id": "evidence_present", "name": "Case 已有证据引用", "pass": true,
     "detail": "证据 3 条", "reason_code": null},
    {"id": "repro_conclusion", "name": "复现段已给出 reproducible 结论", "pass": false,
     "detail": "reproduction 已执行但输出里没有 reproducible 结论", "reason_code": "missing_repro"},
    {"id": "ticket_sections_nonempty", "name": "工单关键段落非空", "pass": true,
     "detail": "计划无 ticket，跳过", "reason_code": null}
  ],
  "quality_scores": {"evidence": 0.75, "repro": 0.5, "consistency": 0.75,
                     "checks_passed": 6, "checks_total": 8, "overall": 0.68},
  "notes": "Mock 复核说明\n第 1 轮机械预检 6/8 通过\n结论 needs_info（missing_repro）"
}
```

`verdict` 只允许 `approve | needs_info | reject`；`approve` 必须全部机械检查通过。`reason_code` 的取值受 stage 契约枚举约束：`ok / insufficient_evidence / inconsistent_evidence / missing_repro / missing_ticket_sections / duplicate_ticket / missing_triage / mock_reject / loop_exhausted`（`orchestrator/src/iqe_orchestrator/common/data/schemas/stage-contracts.json` → `stages.review.output`）。

---

## 6. Reporting Agent（报告）

| 项 | 内容 |
|---|---|
| 订阅 | queue `q.agent.reporting` ← rk `ci.cmd.reporting.#`（`case` / `daily` 同队列） |
| 发布 | exchange `ci.result` → rk `ci.result.reporting`（段名用全称，不是 `ci.result.report`；**不发布事件**，`ci.event.report.published` 在 V1 没有发布点） |
| 数据源 | Postgres（`cases` + `stage_runs` + `reviews` + `tickets` + `evidence` + `scans`） |

- `mode=case`：读该 Case 的全部结构化结论 + `stage_runs` 时间线，产出**单 Case 结案报告**（`report_md` + `metrics`）；编排层把它写进 `cases.case_report`（`graph.py:299-302`）。返回形状 `{case_id, day, report_md, metrics}`。
- `mode=daily`：聚合当日 `cases` / `stage_runs` / `tickets` / `reviews`，生成 Markdown 日报正文，**直接写库** `daily_reports(day, report_md, metrics, cases)`（`reporting/playbook.py:214`），编排层再落一次索引。返回形状 `{case_id: null, day, report_md, metrics}`。
- 两种 mode 都不再产生"素材区文件 + `report_ref`"：日报的真相是 `daily_reports` 表里那一行，读它用 `GET /api/daily-report?day=`（见 05 §4.1）。

Output（`mode=daily` 的形状；`mode=case` 见上）：

```json
{
  "case_id": null,
  "day": "2026-11-25",
  "report_md": "# Staging-CI 日报 2026-11-25\n\n## 概览\n…（≤20000 字）",
  "metrics": {
    "day": "2026-11-25", "cases_total": 9, "errors": 4, "observed_only": 5, "suppressed": 7,
    "scans": 3, "tat_ms_avg": 61000, "open_cases": 1, "report_chars": 4120,
    "status_distribution": {"closed": 5, "blocked": 1, "closed_rejected": 3},
    "rca_distribution": {"driver": 2, "test": 1, "environment": 1},
    "plan_source_distribution": {"workflow": 8, "llm": 1},
    "top_signatures": [["sig-9b71de", 3]]
  }
}
```

---

## 7. Mock 剧本夹具（fixtures）

| 文件 | 内容 | 消费方 |
|---|---|---|
| `fixtures/ci_feed.json` | 10 条假构建（含 1 条 error、2 条历史复发噪声） | monitor |
| `fixtures/triage/<scenario>.json` | `error` / `normal` / `unknown` / `needs_info` 四种分类结论 | triage |
| `fixtures/repro/<scenario>.json` | `yes` / `partial` / `no` / `blocked` | reproduction |
| `fixtures/ticket/<scenario>.json` | `created` / `linked_duplicate` | ticket |
| `fixtures/review/<scenario>.json` | `approve` / `needs_info` / `reject` | reviewer |
| `fixtures/skills/*.md` | 2 个诊断 Playbook | triage |
| `fixtures/known_issues.json` | 已知问题 signature → 已有工单号 | monitor（降噪）、ticket（查重） |
| `fixtures/plans/<scenario>.yaml` | LLM 计划剧本：`warming_ok` / `skip_review`（V4 违规）/ `invalid_stage`（V2 违规） | orchestrator（`MOCK_LLM_ENABLED=true` 时整文件拷入 `PLANS_DIR`，走与人工 YAML 相同的校验路径） |

剧本选择优先级：`payload.scenario_hint`（编排层可透传 Reviewer 的 `requested_info`）→ env `MOCK_SCENARIO` → 默认。

> `fixtures/plans/*.yaml` 不参与上述优先级：它只模拟"LLM 写出来的文件"，整文件拷贝后**必须**再过 `yaml.safe_load` + JSON Schema + V1–V8，确保演示路径与真实模型路径完全一致。

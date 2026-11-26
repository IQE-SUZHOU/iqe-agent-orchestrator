"""Monitor 剧本：读 Mock CI 流水 → 逐 build 签名 → 去噪/合并 → observations + counts。

替换点（真实实现）：`_load_feed` → Jenkins REST（builds/wfapi + JUnit）轮询；签名算法保持一致。

剧本规则（确定性，同一输入同一输出）：
1. SUCCESS            → signal=none，signature=null，不占用下游。
2. FAILURE/UNSTABLE/ABORTED → signal=error，然后按序判抑制：
   a) 签名族命中 fixtures/known_issues.json 且 noise_score >= 0.85 → 抑制（known_issue_noise）；
   b) 同一次扫描内同签名族已有一条未抑制的 error → 保留 build 号最大者，其余抑制
      （duplicate_signature_merge）；
   c) 其余为未抑制 error（promoted），UNSTABLE/ABORTED 的 promoted_reason=unstable_result。
3. 场景文件缺失 → Blocked(source_unavailable)；仅 scenario == "default" 才允许回落 default.json。
"""
from __future__ import annotations

import logging
from typing import Any

from agentkit.agent_runtime import Blocked, Ctx
from agentkit.config import cfg
from agentkit.tools.fingerprint import component_of, signature_family, signature_of

log = logging.getLogger("agent.monitor.playbook")

FEED_DIR = "ci_feed"
SOURCE_NAME = "mock-jenkins"
MAX_EXCERPT = 1500            # 主观测里携带的日志截断长度（证据表存全文）
NOISE_SUPPRESS = 0.85         # 已知噪声抑制阈值
ERROR_RESULTS = ("FAILURE", "UNSTABLE", "ABORTED")
MERGE_KEEP = "max_build"      # 同签名族保留 build 号最大的一条


def _feed_file(ctx: Ctx, scenario: str) -> str:
    return f"{FEED_DIR}/{scenario}.json"


def fixture_names(scenario: str) -> list[str]:
    """剧本名兼容：编排层的 scenario_hint 可能用连字符，夹具文件用下划线（反之亦然）。"""
    out: list[str] = []
    for name in (scenario, scenario.replace("-", "_"), scenario.replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _load_feed(ctx: Ctx) -> dict[str, Any]:
    """取剧本流水夹具；缺失即 blocked(source_unavailable)。"""
    scenario = ctx.scenario or "default"
    for name in fixture_names(scenario):
        if ctx.fixtures.exists(FEED_DIR, f"{name}.json"):
            return ctx.fixtures.json(FEED_DIR, f"{name}.json")
    if scenario == "default" and ctx.fixtures.exists(FEED_DIR, "default.json"):
        return ctx.fixtures.json(FEED_DIR, "default.json")
    hint = getattr(cfg, "mock_feed_source", "") or ""
    path = f"{ctx.fixtures.path(FEED_DIR, f'{scenario}.json')}"
    log.warning("ci_feed_missing", extra={"ctx": {"scenario": scenario, "path": path}})
    raise Blocked("source_unavailable", f"Mock CI feed 不可读: {path}" + (f" (hint={hint})" if hint else ""))


def _known_families(ctx: Ctx) -> dict[str, dict]:
    if not ctx.fixtures.exists("known_issues.json"):
        return {}
    doc = ctx.fixtures.json("known_issues.json") or {}
    out: dict[str, dict] = {}
    for row in doc.get("families", []):
        if row.get("family"):
            out[row["family"]] = row
    return out


def _job_allowed(job: str, jobs_filter: list[str]) -> bool:
    if not jobs_filter:
        return True
    for item in jobs_filter:
        if not item:
            continue
        if item.endswith("*"):
            if job.startswith(item[:-1]):
                return True
        elif job == item or item in job:
            return True
    return False


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    src = payload.get("input", {}) or {}
    scan_id = src.get("scan_id") or ctx.env.get("run_id") or "scan-unknown"
    lookback = src.get("lookback") or "15m"
    jobs_filter = list(src.get("jobs_filter") or [])

    feed = _load_feed(ctx)
    builds = list(feed.get("builds") or [])
    job_default = feed.get("job_default") or SOURCE_NAME
    known = _known_families(ctx)
    threshold = float(getattr(cfg, "noise_threshold", NOISE_SUPPRESS) or NOISE_SUPPRESS)

    # ---- 第一遍：逐 build 计算签名与初始判定 ----
    rows: list[dict[str, Any]] = []
    for b in builds:
        job = str(b.get("job") or job_default)
        number = b.get("number")
        result = str(b.get("result") or "UNKNOWN")
        if not _job_allowed(job, jobs_filter):
            continue
        log_excerpt = str(b.get("log_excerpt") or "")
        if result == "SUCCESS":
            rows.append({
                "obs_id": f"obs-{scan_id}-{number}", "source": SOURCE_NAME, "job": job,
                "build": number, "result": result, "signal": "none", "signature": None,
                "noise_score": 0.0, "known_issue_ref": None, "promoted_reason": None,
                "suppressed": False,
                # 成功构建也带上 console 尾部：轻量分诊（classify_only）仍然需要日志做蒸馏归档
                "log_excerpt": log_excerpt[:MAX_EXCERPT], "evidence_refs": [],
                "context": dict(b.get("context") or {}),
            })
            continue

        sig = signature_of(log_excerpt)
        family = signature_family(sig)
        entry = known.get(family)
        noise = float(entry.get("noise_score", 0.0)) if entry else 0.0
        context = dict(b.get("context") or {})
        context.update({"family": family, "component": component_of(log_excerpt),
                        "scan_id": scan_id, "branch": b.get("branch"),
                        "duration_s": b.get("duration_s"), "started_at": b.get("started_at")})
        row = {
            "obs_id": f"obs-{scan_id}-{number}", "source": SOURCE_NAME, "job": job,
            "build": number, "result": result, "signal": "error", "signature": sig,
            "noise_score": noise, "known_issue_ref": family if entry else None,
            "promoted_reason": None, "suppressed": False,
            "log_excerpt": log_excerpt[:MAX_EXCERPT], "evidence_refs": [], "context": context,
        }
        if entry and noise >= threshold:
            row["suppressed"] = True
            context["suppressed_reason"] = "known_issue_noise"
        rows.append(row)

    # ---- 第二遍：同签名族合并（保留 build 号最大者） ----
    promoted_by_family: dict[str, dict[str, Any]] = {}
    for row in sorted([r for r in rows if r["signal"] == "error" and not r["suppressed"]],
                      key=lambda r: (r["build"] if isinstance(r["build"], int) else 0)):
        fam = row["context"]["family"]
        keep = promoted_by_family.get(fam)
        if keep is None:
            promoted_by_family[fam] = row
            continue
        # 后来的 build 号更大 → 换主观测，旧的改判为抑制
        keep["suppressed"] = True
        keep["context"]["suppressed_reason"] = "duplicate_signature_merge"
        keep["evidence_refs"] = []
        row["context"]["duplicate_merge_count"] = int(row["context"].get("duplicate_merge_count", 1)) + 1
        row["context"].setdefault("duplicate_builds", []).append(keep["build"])
        promoted_by_family[fam] = row

    for row in rows:
        if row["signal"] != "error" or row["suppressed"]:
            continue
        if row["result"] in ("UNSTABLE", "ABORTED"):
            row["promoted_reason"] = "unstable_result"
        else:
            row["promoted_reason"] = "new_failure_signature"

    # ---- 第三遍：为主观测写证据（UI 侧看得到证据链） ----
    for row in rows:
        if row["signal"] != "error" or row["suppressed"]:
            continue
        uri = await ctx.evidence(
            "log_excerpt", row["log_excerpt"] or "",
            {"job": row["job"], "build": row["build"],
             "family": row["context"]["family"], "signature": row["signature"],
             "note": f"{row['job']}#{row['build']} {row['result']} 签名族 {row['context']['family']}"})
        row["evidence_refs"] = [{"kind": "log_excerpt", "uri": uri,
                                 "note": f"build {row['build']} 失败段"}]
    # 被抑制的观测同样落证据：UI 上能回答「这条为什么被降噪」
    for row in [r for r in rows if r["signal"] == "error" and r["suppressed"]]:
        uri = await ctx.evidence(
            "log_excerpt", row["log_excerpt"] or "",
            {"job": row["job"], "build": row["build"], "family": row["context"].get("family"),
             "signature": row["signature"], "suppressed": True,
             "suppressed_reason": row["context"].get("suppressed_reason"),
             "noise_score": row.get("noise_score"),
             "note": f"{row['job']}#{row['build']} 已抑制："
                     f"{row['context'].get('suppressed_reason')}（噪声族 {row['context'].get('family')}）"})
        row["evidence_refs"] = [{"kind": "log_excerpt", "uri": uri,
                                 "note": f"build {row['build']} 抑制证据"}]

    if not promoted_by_family:
        await ctx.evidence("log_excerpt",
                           f"scan {scan_id} lookback={lookback}：{len(rows)} 条构建观测，无新增失败。",
                           {"note": "扫描摘要（无 promoted error）", "scan_id": scan_id})

    promoted = [r for r in rows if r["signal"] == "error" and not r["suppressed"]]
    suppressed = [r for r in rows if r["suppressed"]]
    counts = {"observed": len(rows), "promoted": len(promoted), "suppressed": len(suppressed)}
    log.info("monitor_scan", extra={"ctx": {"scan_id": scan_id, "scenario": ctx.scenario, **counts}})
    return {
        "scan_id": scan_id,
        "observations": rows,
        "counts": counts,
        "scan_window": lookback,
        "source_status": [{"name": SOURCE_NAME, "ok": True, "feed": _feed_file(ctx, ctx.scenario or "default"),
                           "job_default": job_default}],
    }

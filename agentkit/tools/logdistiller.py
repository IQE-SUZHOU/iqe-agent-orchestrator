"""LogDistiller 的 Mock 版：接口与真实组件一致（build_job → log_text + distill）。

真实实现见 LogDistiller（Jenkins 日志获取 + 失败段抽取 + 去噪 + 限长 + 指纹）。
这里只做同样的形状：抽失败段、去噪、限长、算指纹，不产生任何新结论。
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

FAILURE_MARKERS = ("error:", "fatal:", "failed", "Traceback", "AssertionError", "FAILED", "exit status",
                   "编译失败", "断言失败", "超时", "TIMEOUT", "core dumped", "kernel panic")
NOISE_PATTERNS = (
    r"^INFO:\s+Fetching (origin|upstream)",
    r"^From http",
    r"^Receiving objects",
    r"Resolving deltas",
    r"^Updating files",
    r"^\s*$",
)


@dataclass
class Distilled:
    text: str
    original_bytes: int
    distilled_bytes: int
    fingerprint: str
    distiller_version: str = "logdistiller-mock/v1"
    lines_kept: int = 0
    notes: list[str] = field(default_factory=list)


def fetch_log(fixture_text: str, *, source: str) -> str:
    """Mock：真实实现会 Jenkins API + blob 重试（失败段优先）。"""
    return fixture_text


def distill(log_text: str, *, max_chars: int = 6000, context_lines: int = 25) -> Distilled:
    lines = log_text.splitlines()
    original = len(log_text.encode("utf-8"))
    hits: list[int] = []
    for i, line in enumerate(lines):
        if any(m.lower() in line.lower() for m in FAILURE_MARKERS):
            hits.append(i)
    keep: set[int] = set()
    for i in hits:
        for j in range(max(0, i - 2), min(len(lines), i + context_lines)):
            keep.add(j)
    noise = re.compile("|".join(NOISE_PATTERNS))
    out_lines = [lines[i] for i in sorted(keep) if not noise.match(lines[i].strip())]
    if not out_lines:  # 无失败段：退化为尾部窗口
        out_lines = lines[-40:]
    text = "\n".join(out_lines)[:max_chars]
    fp = hashlib.sha256(re.sub(r"\s+", " ", text).encode("utf-8")).hexdigest()[:16]
    return Distilled(text=text, original_bytes=original, distilled_bytes=len(text.encode("utf-8")),
                     fingerprint=fp, lines_kept=len(out_lines))


def build_job(job: str, build: int, fixture_text: str, *, max_chars: int = 6000) -> tuple[str, Distilled]:
    log_text = fetch_log(fixture_text, source=f"{job}#{build}")
    return log_text, distill(log_text, max_chars=max_chars)

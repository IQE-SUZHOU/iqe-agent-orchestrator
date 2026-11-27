"""共享知识库（Mock）：日志/历史问题/修复/回归与 guilty patch/最佳实践。

真实实现是向量库 + 工单系统检索；演示版是 ``data/fixtures/kb/entries.json`` 的确定性打分：

- 组件命中 +3，签名族前缀命中 +2，kind 命中 +1，``noise`` 类条目降权 -2；
- 三处检索：triage（2）判 internal/external 与查重、regression（6c）找历史 guilty patch、
  remediation（4a）找可复用的修复方案；
- 回写不在这里做：Reporting（9）把结论放进 result 的 ``kb_writes``，由编排层落库
  （保持「Agent 只写 evidence / msg_dedup」的写权分工）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("ci_triage.common.kb")

KB_FILE = ("kb", "entries.json")


@dataclass
class KnowledgeBase:
    fixtures: Any  # common.mock.Fixtures（鸭子类型：exists/json）

    def entries(self) -> list[dict]:
        if not self.fixtures.exists(*KB_FILE):
            log.warning("kb_missing_using_empty")
            return []
        doc = self.fixtures.json(*KB_FILE) or {}
        return list(doc.get("entries") or [])

    def search(
        self,
        *,
        component: str | None = None,
        signature: str | None = None,
        kind: str | None = None,
        top_k: int = 3,
    ) -> list[dict]:
        family = (signature or "").split(":")[0]
        scored: list[tuple[int, dict]] = []
        for entry in self.entries():
            code = int(entry.get("_score", 0))
            if component and entry.get("component") == component:
                code += 3
            if family and str(entry.get("signature_family") or "") == family:
                code += 2
            if kind and entry.get("kind") == kind:
                code += 1
            if entry.get("kind") == "noise":
                code -= 2
            # 至少要命中组件或签名族才算相关：单靠 kind 加分（+1）会误召回别的组件的修复方案
            if code >= 2:
                scored.append((code, entry))
        scored.sort(key=lambda kv: (-kv[0], str(kv[1].get("id"))))
        return [{**e, "score": s} for s, e in scored[:top_k]]

    def refs(self, **kw) -> list[str]:
        """只回条目 id，用作输出里的 ``kb_refs``（保持契约小、可对账）。"""
        return [str(e.get("id")) for e in self.search(**kw)]

    def fixes_for(self, component: str | None, signature: str | None) -> list[dict]:
        rows = self.search(component=component, signature=signature, kind="fix", top_k=3)
        return [r for r in rows if r.get("kind") == "fix"]

    def guilty_patch_for(self, component: str | None, signature: str | None) -> dict | None:
        rows = self.search(component=component, signature=signature, kind="guilty_patch", top_k=1)
        return rows[0] if rows else None


def normalize_write(raw: dict) -> dict:
    """Reporting 回写的知识条目归一化（编排层落库前调用）。"""
    return {
        "kind": str(raw.get("kind") or "failure")[:32],
        "component": str(raw.get("component") or "unknown")[:128],
        "signature": (str(raw["signature"])[:200] if raw.get("signature") else None),
        "summary": str(raw.get("summary") or "")[:500],
        "case_id": raw.get("case_id"),
    }

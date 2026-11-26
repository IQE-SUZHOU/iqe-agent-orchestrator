"""六个 Mock Agent：monitor / triage / reproduction / ticket / review / reporting。

每个包结构一致：
    main.py      启动 AgentRuntime（连总线 → 校验契约 → 执行剧本 → 发布 result）
    playbook.py  剧本 handler(ctx) -> dict（确定性 Mock 结论）
    skills/      仅 triage 有：SKILL 卡片（诊断手册，被读取并回显）
"""

STAGES = ("monitor", "triage", "reproduction", "ticket", "review", "reporting")

__all__ = ["STAGES"]

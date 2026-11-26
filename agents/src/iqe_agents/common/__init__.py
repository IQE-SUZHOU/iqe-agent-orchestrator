"""共享底座（每个子项目各带一份，两份必须字节一致）。

包含消息总线、数据库访问、信封与幂等、配置、路径、Mock 夹具、健康探针、日志与计数器；
不含领域逻辑（编排层的 plan/graph 在 orchestrator，六剧本在 agents）。
"""

from __future__ import annotations

"""专业结构调整治理后端。

只追加事件日志 + 哈希链是系统的事实来源；审计记录独立提交，
业务事务回滚不影响审计留痕；签署互斥在数据库事务内最终裁决。
"""
from __future__ import annotations

from .engine import GovernanceEngine, ConflictError, WorkflowError
from .storage import EventStore, tampered_events
from .projection import GovernanceState

__all__ = [
    "GovernanceEngine",
    "ConflictError",
    "WorkflowError",
    "EventStore",
    "GovernanceState",
    "tampered_events",
]

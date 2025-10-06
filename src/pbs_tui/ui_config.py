"""UI configuration for table layouts and column metadata."""

from __future__ import annotations

JOB_TABLE_COLUMNS: list[tuple[str, str]] = [
    ("#JobId", "left"),
    ("User", "left"),
    ("Account", "left"),
    ("JobName", "left"),
    ("Queue", "left"),
    ("WallTime", "right"),
    ("QueuedTime", "right"),
    ("EstStart", "left"),
    ("RunTime", "right"),
    ("TimeRemaining", "right"),
    ("Nodes", "right"),
    ("State", "left"),
]

__all__ = ["JOB_TABLE_COLUMNS"]

"""UI configuration for table layouts and column metadata."""

from __future__ import annotations

JOB_TABLE_COLUMNS: list[tuple[str, str]] = [
    ("#JobId", "left"),
    ("User", "left"),
    ("Account", "left"),
    ("Queue", "left"),
    ("Nodes", "right"),
    ("State", "left"),
    ("WallTime", "right"),
    ("RunTime", "right"),
    ("TimeRemaining", "right"),
    ("QueuedTime", "right"),
    ("EstStart", "left"),
    ("JobName", "left"),
]

__all__ = ["JOB_TABLE_COLUMNS"]

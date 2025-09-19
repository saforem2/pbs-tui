"""UI configuration for table layouts and column metadata."""

from __future__ import annotations

JOB_TABLE_COLUMNS: list[tuple[str, str]] = [
    ("#JobId", "left"),
    ("User", "left"),
    ("Account", "left"),
    ("Score", "right"),
    ("WallTime", "left"),
    ("QueuedTime", "left"),
    ("EstStart", "left"),
    ("RunTime", "left"),
    ("TimeRemaining", "left"),
    ("Nodes", "right"),
    ("State", "left"),
    ("Queue", "left"),
    ("JobName", "left"),
    ("Location/Comments", "left"),
]

__all__ = ["JOB_TABLE_COLUMNS"]

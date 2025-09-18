"""UI configuration for table layouts and column metadata."""

from __future__ import annotations

JOB_TABLE_COLUMNS: list[tuple[str, str]] = [
    ("Job ID", "left"),
    ("Name", "left"),
    ("User", "left"),
    ("Queue", "left"),
    ("State", "left"),
    ("Nodes", "left"),
    ("Node Count", "right"),
    ("First Node", "left"),
    ("Walltime", "left"),
    ("Runtime", "left"),
]

__all__ = ["JOB_TABLE_COLUMNS"]

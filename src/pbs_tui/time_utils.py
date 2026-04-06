"""Shared time/duration parsing utilities for PBS TUI.

Centralises duration parsing and remaining-time calculations so that all
views (job table, cluster grid, exports) interpret walltime strings
identically.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from .data import Job


def parse_duration_spec(value: Optional[str]) -> Optional[timedelta]:
    """Parse a PBS duration string into a :class:`timedelta`.

    Accepted formats (with optional ``DAYS-`` prefix):
    ``DD:HH:MM:SS``, ``HH:MM:SS``, ``MM:SS``, ``SS``.
    Returns *None* for invalid or empty input.
    """
    if value is None:
        return None
    spec = value.strip()
    if not spec:
        return None
    days = 0
    time_part = spec
    if "-" in spec:
        day_part, remainder = spec.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            return None
        if days < 0:
            return None
        time_part = remainder
    parts = time_part.split(":")
    try:
        units = [int(part) for part in parts]
    except ValueError:
        return None
    if len(units) > 4:
        return None
    if any(unit < 0 for unit in units):
        return None
    if len(units) == 4:
        days += units[0]
        hours, minutes, seconds = units[1:]
    elif len(units) == 3:
        hours, minutes, seconds = units
    elif len(units) == 2:
        hours, minutes, seconds = 0, units[0], units[1]
    elif len(units) == 1:
        hours, minutes, seconds = 0, 0, units[0]
    else:
        return None
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def time_remaining(job: Job, ref: datetime) -> Optional[timedelta]:
    """Return estimated time remaining for a running *job*."""
    walltime = parse_duration_spec(job.walltime)
    if walltime is None:
        return None
    if job.start_time is None:
        return walltime
    start = job.start_time
    end = job.end_time or ref
    if start.tzinfo is not None and end.tzinfo is None:
        end = end.replace(tzinfo=start.tzinfo)
    elif start.tzinfo is None and end.tzinfo is not None:
        start = start.replace(tzinfo=end.tzinfo)
    elapsed = end - start
    remaining = walltime - elapsed
    return timedelta(seconds=0) if remaining.total_seconds() < 0 else remaining


def format_remaining(td: Optional[timedelta]) -> str:
    """Format a remaining-time delta as a compact human-readable string."""
    if td is None:
        return ""
    total = int(td.total_seconds())
    if total <= 0:
        return "done"
    hours, rem = divmod(total, 3600)
    minutes = rem // 60
    if hours > 0:
        return f"{hours}h{minutes:02d}m"
    return f"{minutes}m"

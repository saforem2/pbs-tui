"""Data structures for PBS Pro scheduler state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional


@dataclass(slots=True)
class Job:
    """Representation of a PBS Pro job."""

    id: str
    name: str
    user: str
    queue: str
    state: str
    exec_host: Optional[str] = None
    create_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    walltime: Optional[str] = None
    nodes: Optional[str] = None
    resources_requested: Dict[str, str] = field(default_factory=dict)
    resources_used: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None
    exit_status: Optional[str] = None

    def runtime(self, reference: Optional[datetime] = None) -> Optional[timedelta]:
        """Return the runtime of the job using *reference* as the end time."""

        if self.start_time is None:
            return None
        reference = reference or datetime.now(timezone.utc)
        start = self.start_time
        if start.tzinfo is not None and reference.tzinfo is None:
            reference = reference.replace(tzinfo=start.tzinfo)
        elif start.tzinfo is None and reference.tzinfo is not None:
            start = start.replace(tzinfo=reference.tzinfo)
        delta = reference - start
        if delta.total_seconds() < 0:
            return timedelta(seconds=0)
        return delta


@dataclass(slots=True)
class Node:
    """Representation of a PBS compute node."""

    name: str
    state: str
    np: Optional[int] = None
    ncpus: Optional[int] = None
    properties: List[str] = field(default_factory=list)
    jobs: List[str] = field(default_factory=list)
    resources_available: Dict[str, str] = field(default_factory=dict)
    resources_assigned: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None

    def primary_state(self) -> str:
        """Return the primary node state, collapsing comma separated values."""

        return self.state.split(",", 1)[0] if self.state else "unknown"


@dataclass(slots=True)
class Queue:
    """Representation of a PBS queue."""

    name: str
    state: Optional[str] = None
    enabled: Optional[bool] = None
    started: Optional[bool] = None
    total_jobs: Optional[int] = None
    job_states: Dict[str, int] = field(default_factory=dict)
    resources_default: Dict[str, str] = field(default_factory=dict)
    resources_max: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None


@dataclass(slots=True)
class SchedulerSnapshot:
    """A snapshot of the PBS scheduler state."""

    jobs: List[Job] = field(default_factory=list)
    nodes: List[Node] = field(default_factory=list)
    queues: List[Queue] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "pbs"
    errors: List[str] = field(default_factory=list)

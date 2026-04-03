"""Cluster-wide node utilization grid widget."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from rich.table import Table
from rich.text import Text
from textual.widgets import Static

from .data import Job, SchedulerSnapshot
from .nodes import parse_node_count_spec, extract_exec_host_nodes, extract_requested_nodes

# ── colour palette ──────────────────────────────────────────────────────

# Individual job colours (Rich style strings) — high contrast, ordered
JOB_STYLES = [
    "on blue",
    "on red",
    "on magenta",
    "on bright_yellow",
    "on bright_red",
    "on bright_blue",
    "on white",
    "on bright_magenta",
]

# Aggregated-queue colours
AGGREGATED_QUEUE_STYLES: Dict[str, str] = {
    "debug": "on yellow",
    "debug-scaling": "on bright_yellow",
    "preemptable": "on color(240)",
    "demand": "on bright_magenta",
    "tiny": "on bright_cyan",
    "small": "on cyan",
    "medium": "on bright_green",
}

_AGGREGATED_QUEUE_DEFAULT_STYLE = "on color(236)"
_EMPTY_STYLE = "on color(236)"
_BORDER_STYLE = "bold"
_BLOCK_CHAR = " "

# Queues whose running jobs are always merged into a single coloured block
AGGREGATED_QUEUES = frozenset(
    {"debug", "debug-scaling", "preemptable", "demand", "tiny", "small", "medium"}
)


# ── helpers ─────────────────────────────────────────────────────────────


def _job_node_count(job: Job) -> int:
    """Return the best-effort node count for *job*."""
    if exec_nodes := extract_exec_host_nodes(job.exec_host):
        return len(exec_nodes)
    if (count := parse_node_count_spec(job.nodes)) is not None:
        return count
    if requested_nodes := extract_requested_nodes(job.nodes):
        return len(requested_nodes)
    for key in ("select", "nodes", "nodect"):
        if (count := parse_node_count_spec(job.resources_requested.get(key))) is not None:
            return count
    return 1


def _parse_duration_spec(value: Optional[str]) -> Optional[timedelta]:
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
        time_part = remainder
    parts = time_part.split(":")
    try:
        units = [int(p) for p in parts]
    except ValueError:
        return None
    if len(units) == 3:
        hours, minutes, seconds = units
    elif len(units) == 2:
        hours, minutes, seconds = 0, units[0], units[1]
    elif len(units) == 1:
        hours, minutes, seconds = 0, 0, units[0]
    else:
        return None
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def _time_remaining(job: Job, ref: datetime) -> Optional[timedelta]:
    """Return estimated time remaining for a running job."""
    walltime = _parse_duration_spec(job.walltime)
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


def _format_remaining(td: Optional[timedelta]) -> str:
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


# ── grid building ───────────────────────────────────────────────────────


def _build_grid(
    snapshot: SchedulerSnapshot,
    grid_width: int = 100,
    grid_height: int = 12,
) -> Tuple[Text, Text]:
    """Return ``(grid_text, legend_text)`` Rich Text objects."""

    total_nodes = max(len(snapshot.nodes), 1)
    total_cells = grid_width * grid_height
    nodes_per_cell = total_nodes / total_cells

    ref = snapshot.timestamp

    running_jobs = [j for j in snapshot.jobs if j.state == "R"]

    # ── partition jobs ──────────────────────────────────────────────
    large_queue_jobs: List[Tuple[Job, int]] = []  # (job, node_count)
    agg_queue_nodes: Dict[str, int] = defaultdict(int)

    for job in running_jobs:
        nc = _job_node_count(job)
        if job.queue in AGGREGATED_QUEUES:
            agg_queue_nodes[job.queue] += nc
        else:
            large_queue_jobs.append((job, nc))

    # Sort largest first
    large_queue_jobs.sort(key=lambda x: x[1], reverse=True)

    # ── assign cells ────────────────────────────────────────────────
    cell_styles: List[str] = [_EMPTY_STYLE] * total_cells
    current = 0

    # Assign colours to individual large-queue jobs
    job_style_map: Dict[str, str] = {}
    legend_entries: List[Tuple[str, str, str, int, str]] = []  # (style, user, queue, nodes, time_str)

    for idx, (job, nc) in enumerate(large_queue_jobs):
        style = JOB_STYLES[idx % len(JOB_STYLES)]
        job_style_map[job.id] = style
        cells_needed = max(1, int(nc / nodes_per_cell))
        for _ in range(cells_needed):
            if current < total_cells:
                cell_styles[current] = style
                current += 1
        remaining = _time_remaining(job, ref)
        time_str = _format_remaining(remaining)
        legend_entries.append((style, job.user, job.queue, nc, time_str))

    # Aggregated queues
    agg_legend: List[Tuple[str, str, int]] = []  # (style, queue, nodes)
    for queue_name, nodes in agg_queue_nodes.items():
        style = AGGREGATED_QUEUE_STYLES.get(queue_name, _AGGREGATED_QUEUE_DEFAULT_STYLE)
        cells_needed = max(1, int(nodes / nodes_per_cell))
        for _ in range(cells_needed):
            if current < total_cells:
                cell_styles[current] = style
                current += 1
        agg_legend.append((style, queue_name, nodes))

    # Count available
    available_nodes = total_nodes - sum(nc for _, nc in large_queue_jobs) - sum(agg_queue_nodes.values())
    if available_nodes < 0:
        available_nodes = 0

    # ── render grid ─────────────────────────────────────────────────
    grid = Text()
    border_line = "=" * (grid_width + 2)
    grid.append(border_line + "\n", style=_BORDER_STYLE)

    for row in range(grid_height):
        grid.append("|", style=_BORDER_STYLE)
        for col in range(grid_width):
            idx = row * grid_width + col
            grid.append(_BLOCK_CHAR, style=cell_styles[idx])
        grid.append("|", style=_BORDER_STYLE)
        grid.append("\n")

    grid.append(border_line, style=_BORDER_STYLE)

    # ── render legend ───────────────────────────────────────────────
    legend = Text()
    legend.append("Legend:\n", style="bold")

    for style, user, queue, nodes, time_str in legend_entries:
        legend.append("  ")
        legend.append("  ", style=style)
        legend.append(f" {user:<8} {nodes:>5}n {queue:<12}")
        if time_str:
            legend.append(f" [{time_str}]")
        legend.append("\n")

    for style, queue_name, nodes in agg_legend:
        legend.append("  ")
        legend.append("  ", style=style)
        legend.append(f" {queue_name} ({nodes} nodes)\n")

    legend.append("  ")
    legend.append("  ", style=_EMPTY_STYLE)
    legend.append(f" Available ({available_nodes} nodes)\n")

    # ── summary header ──────────────────────────────────────────────
    running_nodes = sum(nc for _, nc in large_queue_jobs) + sum(agg_queue_nodes.values())
    queued_jobs = sum(1 for j in snapshot.jobs if j.state == "Q")
    header = Text()
    header.append("Cluster Status", style="bold")
    ts = ref.astimezone().strftime("%H:%M:%S") if ref.tzinfo else ref.strftime("%H:%M:%S")
    header.append(f" [{ts}]")
    header.append(f" | Nodes: {total_nodes:,}")
    header.append(f" | Running: {running_nodes:,}")
    header.append(f" | Available: {available_nodes:,}")
    header.append(f" | Jobs: {len(running_jobs)}R/{queued_jobs}Q")

    # Combine header + grid
    full_grid = Text()
    full_grid.append_text(header)
    full_grid.append("\n\n")
    full_grid.append_text(grid)

    return full_grid, legend


# ── widget ──────────────────────────────────────────────────────────────


class ClusterGridWidget(Static):
    """Cluster-wide node utilization grid with legend.

    Call :meth:`update_from_snapshot` whenever the scheduler snapshot refreshes.
    """

    DEFAULT_CSS = """
    ClusterGridWidget {
        height: 1fr;
        padding: 1;
    }
    """

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        grid_text, legend_text = _build_grid(snapshot)
        layout = Table.grid(padding=(0, 2))
        layout.add_column(ratio=3)
        layout.add_column(ratio=1)
        layout.add_row(grid_text, legend_text)
        self.update(layout)

"""Cluster-wide node utilization grid widget."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from rich.console import Group
from rich.table import Table
from rich.text import Text
from textual.widgets import Static

from .data import Job, SchedulerSnapshot
from .nodes import parse_node_count_spec, extract_exec_host_nodes, extract_requested_nodes

# ── colour palette ──────────────────────────────────────────────────────

# Individual job colours — saturated, high-contrast, no overlap with queue palette
JOB_STYLES = [
    "on dodger_blue2",
    "on red3",
    "on dark_magenta",
    "on dark_orange",
    "on chartreuse3",
    "on deep_sky_blue1",
    "on medium_violet_red",
    "on gold1",
    "on turquoise2",
    "on indian_red",
    "on orchid",
    "on spring_green2",
    "on steel_blue",
    "on salmon1",
    "on pale_green1",
    "on hot_pink",
]

# Aggregated-queue colours — deliberately muted/pastel to not clash with jobs.
# Each uses a 256-colour index chosen to be visually distinct from the job palette.
AGGREGATED_QUEUE_STYLES: Dict[str, str] = {
    "debug": "on color(215)",        # sandy orange
    "debug-scaling": "on color(222)", # pale gold
    "preemptable": "on color(102)",   # muted olive
    "demand": "on color(132)",        # dusty rose
    "tiny": "on color(109)",          # muted teal
    "small": "on color(110)",         # soft steel blue
    "medium": "on color(150)",        # sage green
}

_AGGREGATED_QUEUE_DEFAULT_STYLE = "on color(59)"  # dark olive grey
_EMPTY_STYLE = "on grey15"
_BLOCK_CHAR = " "         # solid fill (space with background) for jobs
_QUEUE_GRID_CHAR = "░"    # textured fill for aggregated queues in the grid

# Legend swatches: jobs get solid ██, queues get striped ░░ to visually separate
_JOB_SWATCH = "██"
_QUEUE_SWATCH = "░░"
_EMPTY_SWATCH = "▒▒"

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
    grid_height: int = 14,
) -> Group:
    """Return a Rich renderable with header, grid, and legend."""

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
    # Each cell is (bg_style, char) — jobs use solid blocks, queues use patterns
    cell_data: List[Tuple[str, str]] = [(_EMPTY_STYLE, _BLOCK_CHAR)] * total_cells
    current = 0

    legend_entries: List[Tuple[str, str, str, int, str]] = []

    for idx, (job, nc) in enumerate(large_queue_jobs):
        style = JOB_STYLES[idx % len(JOB_STYLES)]
        cells_needed = max(1, int(nc / nodes_per_cell))
        for _ in range(cells_needed):
            if current < total_cells:
                cell_data[current] = (style, _BLOCK_CHAR)
                current += 1
        remaining = _time_remaining(job, ref)
        time_str = _format_remaining(remaining)
        legend_entries.append((style, job.user, job.queue, nc, time_str))

    # Aggregated queues — use pattern character in the grid
    agg_legend: List[Tuple[str, str, int]] = []
    for queue_name, nodes in agg_queue_nodes.items():
        style = AGGREGATED_QUEUE_STYLES.get(queue_name, _AGGREGATED_QUEUE_DEFAULT_STYLE)
        cells_needed = max(1, int(nodes / nodes_per_cell))
        for _ in range(cells_needed):
            if current < total_cells:
                cell_data[current] = (style, _QUEUE_GRID_CHAR)
                current += 1
        agg_legend.append((style, queue_name, nodes))

    # Stats
    running_nodes = sum(nc for _, nc in large_queue_jobs) + sum(agg_queue_nodes.values())
    available_nodes = max(0, total_nodes - running_nodes)
    queued_jobs = sum(1 for j in snapshot.jobs if j.state == "Q")
    utilisation = running_nodes / total_nodes * 100

    # ── header ──────────────────────────────────────────────────────
    ts = ref.astimezone().strftime("%H:%M:%S") if ref.tzinfo else ref.strftime("%H:%M:%S")

    header = Text()
    header.append("Cluster Status ", style="bold")
    header.append(f"[{ts}]", style="dim")
    header.append("  ")
    header.append(f"{utilisation:.0f}% ", style="bold green" if utilisation < 80 else "bold yellow" if utilisation < 95 else "bold red")
    header.append("utilised", style="dim")
    header.append("  ")
    header.append(f"Nodes: ", style="dim")
    header.append(f"{total_nodes:,}", style="bold")
    header.append(f"  Running: ", style="dim")
    header.append(f"{running_nodes:,}", style="bold cyan")
    header.append(f"  Available: ", style="dim")
    header.append(f"{available_nodes:,}", style="bold green")
    header.append(f"  Jobs: ", style="dim")
    header.append(f"{len(running_jobs)}", style="bold")
    header.append("R", style="dim green")
    header.append("/", style="dim")
    header.append(f"{queued_jobs}", style="bold")
    header.append("Q", style="dim yellow")

    # ── utilisation bar ─────────────────────────────────────────────
    bar_width = min(grid_width, 50)
    filled = int(utilisation / 100 * bar_width)
    bar = Text()
    bar.append("  [", style="dim")
    if filled > 0:
        bar.append("━" * filled, style="bold cyan")
    if bar_width - filled > 0:
        bar.append("━" * (bar_width - filled), style="grey30")
    bar.append("]", style="dim")

    # ── render grid ─────────────────────────────────────────────────
    grid = Text()
    for row in range(grid_height):
        for col in range(grid_width):
            idx = row * grid_width + col
            style, char = cell_data[idx]
            grid.append(char, style=style)
        if row < grid_height - 1:
            grid.append("\n")

    # ── render legend (grid below the chart) ──────────────────────────
    def _fg(bg_style: str) -> str:
        """Convert 'on <color>' background style to foreground."""
        return bg_style.replace("on ", "", 1) if bg_style.startswith("on ") else bg_style

    def _entry(swatch: str, bg_style: str, label: str, detail: str = "") -> Text:
        """Build a single legend entry: swatch label detail."""
        t = Text()
        t.append(swatch, style=_fg(bg_style))
        t.append(f" {label}", style="bold")
        if detail:
            t.append(f" {detail}", style="dim")
        return t

    # Build all legend entries as Text objects
    all_entries: List[Text] = []

    for style, user, queue, nodes, time_str in legend_entries:
        detail_parts = [f"{nodes:,}n {queue}"]
        if time_str:
            detail_parts.append(f"[{time_str}]")
        all_entries.append(_entry(_JOB_SWATCH, style, user, " ".join(detail_parts)))

    for style, queue_name, nodes in agg_legend:
        all_entries.append(_entry(_QUEUE_SWATCH, style, queue_name, f"({nodes:,}n)"))

    all_entries.append(_entry(_EMPTY_SWATCH, _EMPTY_STYLE, "Available", f"({available_nodes:,}n)"))

    # Lay entries into a grid with up to 4 columns
    n_cols = min(4, len(all_entries)) if all_entries else 1
    legend_grid = Table.grid(padding=(0, 3), expand=True)
    for _ in range(n_cols):
        legend_grid.add_column()

    for i in range(0, len(all_entries), n_cols):
        row = all_entries[i : i + n_cols]
        while len(row) < n_cols:
            row.append(Text())
        legend_grid.add_row(*row)

    return Group(header, bar), grid, legend_grid


# ── widget ──────────────────────────────────────────────────────────────


class _GridPanel(Static):
    """The coloured grid area — gets a Textual border."""


class _InfoPanel(Static):
    """Header + legend area outside the border."""


class ClusterGridWidget(Static):
    """Cluster-wide node utilization grid with legend.

    Call :meth:`update_from_snapshot` whenever the scheduler snapshot refreshes.
    """

    DEFAULT_CSS = """
    ClusterGridWidget {
        layout: vertical;
        height: 1fr;
        padding: 1 2;
    }
    ClusterGridWidget _InfoPanel {
        height: auto;
    }
    ClusterGridWidget _GridPanel {
        border: round $surface-lighten-2;
        height: 1fr;
        padding: 0 1;
    }
    """

    def compose(self):
        yield _InfoPanel(id="cluster_info")
        yield _GridPanel(id="cluster_grid_panel")

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        header, grid, legend = _build_grid(snapshot)
        self.query_one(_InfoPanel).update(Group(header, Text(), legend))
        self.query_one(_GridPanel).update(grid)

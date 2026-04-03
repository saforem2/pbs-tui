"""Cluster-wide node utilization grid widget."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text
from textual.containers import VerticalScroll
from textual.message import Message
from textual.widget import Widget
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
AGGREGATED_QUEUE_STYLES: Dict[str, str] = {
    "debug": "on color(215)",        # sandy orange
    "debug-scaling": "on color(222)", # pale gold
    "preemptable": "on color(102)",   # muted olive
    "demand": "on color(132)",        # dusty rose
    "tiny": "on color(109)",          # muted teal
    "small": "on color(110)",         # soft steel blue
    "medium": "on color(150)",        # sage green
}

_AGGREGATED_QUEUE_DEFAULT_STYLE = "on color(59)"
_EMPTY_STYLE = "on grey15"

# Legend swatches
_JOB_SWATCH = "██"
_QUEUE_SWATCH = "░░"
_EMPTY_SWATCH = "▒▒"

# Queues whose running jobs are always merged into a single coloured block
AGGREGATED_QUEUES = frozenset(
    {"debug", "debug-scaling", "preemptable", "demand", "tiny", "small", "medium"}
)


# ── helpers ─────────────────────────────────────────────────────────────


def _fg(bg_style: str) -> str:
    """Convert ``'on <color>'`` background style to a foreground style."""
    return bg_style.replace("on ", "", 1) if bg_style.startswith("on ") else bg_style


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


# ── grid data structure ─────────────────────────────────────────────────


class GridData:
    """Holds the result of building the cluster grid."""

    __slots__ = (
        "header", "grid_text", "legend", "cell_owners",
        "grid_width", "grid_height",
    )

    def __init__(
        self,
        header: RenderableType,
        grid_text: Text,
        legend: RenderableType,
        cell_owners: List[Optional[str]],
        grid_width: int,
        grid_height: int,
    ) -> None:
        self.header = header
        self.grid_text = grid_text
        self.legend = legend
        self.cell_owners = cell_owners
        self.grid_width = grid_width
        self.grid_height = grid_height


# ── grid building ───────────────────────────────────────────────────────


def _build_grid(
    snapshot: SchedulerSnapshot,
    grid_width: int = 100,
    grid_height: int = 28,
) -> GridData:
    """Build the cluster grid data.

    *grid_height* must be even (each text row encodes two logical rows via
    half-block characters).
    """

    grid_height = grid_height if grid_height % 2 == 0 else grid_height + 1

    total_nodes = max(len(snapshot.nodes), 1)
    total_cells = grid_width * grid_height
    nodes_per_cell = total_nodes / total_cells

    ref = snapshot.timestamp

    running_jobs = [j for j in snapshot.jobs if j.state == "R"]

    # ── partition jobs ──────────────────────────────────────────────
    large_queue_jobs: List[Tuple[Job, int]] = []
    agg_queue_nodes: Dict[str, int] = defaultdict(int)

    for job in running_jobs:
        nc = _job_node_count(job)
        if job.queue in AGGREGATED_QUEUES:
            agg_queue_nodes[job.queue] += nc
        else:
            large_queue_jobs.append((job, nc))

    large_queue_jobs.sort(key=lambda x: x[1], reverse=True)

    # ── assign cells ────────────────────────────────────────────────
    cell_styles: List[str] = [_EMPTY_STYLE] * total_cells
    cell_owners: List[Optional[str]] = [None] * total_cells
    current = 0

    legend_entries: List[Tuple[str, str, str, int, str]] = []

    for idx, (job, nc) in enumerate(large_queue_jobs):
        style = JOB_STYLES[idx % len(JOB_STYLES)]
        cells_needed = max(1, int(nc / nodes_per_cell))
        for _ in range(cells_needed):
            if current < total_cells:
                cell_styles[current] = style
                cell_owners[current] = job.id
                current += 1
        remaining = _time_remaining(job, ref)
        time_str = _format_remaining(remaining)
        legend_entries.append((style, job.user, job.queue, nc, time_str))

    # Aggregated queues
    agg_legend: List[Tuple[str, str, int]] = []
    for queue_name, nodes in agg_queue_nodes.items():
        style = AGGREGATED_QUEUE_STYLES.get(queue_name, _AGGREGATED_QUEUE_DEFAULT_STYLE)
        cells_needed = max(1, int(nodes / nodes_per_cell))
        for _ in range(cells_needed):
            if current < total_cells:
                cell_styles[current] = style
                cell_owners[current] = f"queue:{queue_name}"
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
    header.append(
        f"{utilisation:.0f}% ",
        style="bold green" if utilisation < 80 else "bold yellow" if utilisation < 95 else "bold red",
    )
    header.append("utilised", style="dim")
    header.append("  ")
    header.append("Nodes: ", style="dim")
    header.append(f"{total_nodes:,}", style="bold")
    header.append("  Running: ", style="dim")
    header.append(f"{running_nodes:,}", style="bold cyan")
    header.append("  Available: ", style="dim")
    header.append(f"{available_nodes:,}", style="bold green")
    header.append("  Jobs: ", style="dim")
    header.append(f"{len(running_jobs)}", style="bold")
    header.append("R", style="dim green")
    header.append("/", style="dim")
    header.append(f"{queued_jobs}", style="bold")
    header.append("Q", style="dim yellow")

    # utilisation bar
    bar_width = min(grid_width, 50)
    filled = int(utilisation / 100 * bar_width)
    bar = Text()
    bar.append("  [", style="dim")
    if filled > 0:
        bar.append("━" * filled, style="bold cyan")
    if bar_width - filled > 0:
        bar.append("━" * (bar_width - filled), style="grey30")
    bar.append("]", style="dim")

    # ── render grid (half-block) ────────────────────────────────────
    grid = Text()
    text_rows = grid_height // 2
    for text_row in range(text_rows):
        top_row = text_row * 2
        bot_row = text_row * 2 + 1
        for col in range(grid_width):
            top_idx = top_row * grid_width + col
            bot_idx = bot_row * grid_width + col
            top_fg = _fg(cell_styles[top_idx])
            bot_bg = cell_styles[bot_idx]
            grid.append("▀", style=f"{top_fg} {bot_bg}")
        if text_row < text_rows - 1:
            grid.append("\n")

    # ── legend ──────────────────────────────────────────────────────
    def _entry(swatch: str, bg_style: str, label: str, detail: str = "") -> Text:
        t = Text()
        t.append(swatch, style=_fg(bg_style))
        t.append(f" {label}", style="bold")
        if detail:
            t.append(f" {detail}", style="dim")
        return t

    all_entries: List[Text] = []
    for style, user, queue, nodes, time_str in legend_entries:
        detail_parts = [f"{nodes:,}n {queue}"]
        if time_str:
            detail_parts.append(f"[{time_str}]")
        all_entries.append(_entry(_JOB_SWATCH, style, user, " ".join(detail_parts)))

    for style, queue_name, nodes in agg_legend:
        all_entries.append(_entry(_QUEUE_SWATCH, style, queue_name, f"({nodes:,}n)"))

    all_entries.append(_entry(_EMPTY_SWATCH, _EMPTY_STYLE, "Available", f"({available_nodes:,}n)"))

    n_cols = min(4, len(all_entries)) if all_entries else 1
    legend_grid = Table.grid(padding=(0, 3), expand=True)
    for _ in range(n_cols):
        legend_grid.add_column()

    for i in range(0, len(all_entries), n_cols):
        row = all_entries[i : i + n_cols]
        while len(row) < n_cols:
            row.append(Text())
        legend_grid.add_row(*row)

    return GridData(
        header=Group(header, bar),
        grid_text=grid,
        legend=legend_grid,
        cell_owners=cell_owners,
        grid_width=grid_width,
        grid_height=grid_height,
    )


# ── widgets ─────────────────────────────────────────────────────────────


class _GridPanel(Widget, can_focus=True):
    """Interactive coloured grid — supports click to inspect a job."""

    DEFAULT_CSS = """
    _GridPanel {
        border: round $surface-lighten-2;
        height: auto;
        padding: 0 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._content: RenderableType = Text()
        self._cell_owners: List[Optional[str]] = []
        self._grid_width: int = 0
        self._grid_height: int = 0

    def render(self) -> RenderableType:
        return self._content

    def set_grid(self, data: GridData) -> None:
        self._content = data.grid_text
        self._cell_owners = data.cell_owners
        self._grid_width = data.grid_width
        self._grid_height = data.grid_height
        self.refresh()

    def on_click(self, event) -> None:
        if not self._cell_owners or self._grid_width == 0:
            return
        col = int(event.x)
        # Each text row = 2 logical rows (half-block rendering)
        text_row = int(event.y)
        logical_row = text_row * 2  # top half of clicked row
        if col < 0 or col >= self._grid_width:
            return
        if logical_row < 0 or logical_row >= self._grid_height:
            return
        idx = logical_row * self._grid_width + col
        if idx < 0 or idx >= len(self._cell_owners):
            return
        owner = self._cell_owners[idx]
        if owner:
            self.post_message(ClusterGridWidget.CellClicked(owner))


class _InfoPanel(Static):
    """Header + legend area outside the border."""


class ClusterGridWidget(VerticalScroll):
    """Cluster-wide node utilization grid with legend.

    Scrollable container. Call :meth:`update_from_snapshot` on each refresh.
    """

    class CellClicked(Message):
        """Posted when a grid cell is clicked."""

        def __init__(self, owner: str) -> None:
            super().__init__()
            self.owner = owner

    DEFAULT_CSS = """
    ClusterGridWidget {
        height: 1fr;
        padding: 1 2;
    }
    ClusterGridWidget _InfoPanel {
        height: auto;
    }
    """

    def compose(self):
        yield _InfoPanel(id="cluster_info")
        yield _GridPanel(id="cluster_grid_panel")

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        data = _build_grid(snapshot)
        self.query_one(_InfoPanel).update(Group(data.header, Text(), data.legend))
        self.query_one(_GridPanel).set_grid(data)

"""Cluster-wide node utilization grid widget."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from rich.console import Group, RenderableType
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
_SEP_STYLE = "on grey7"  # dark separator between job blocks

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

    prev_owner: Optional[str] = None

    def _insert_sep() -> None:
        """Insert a thin dark separator cell between blocks."""
        nonlocal current
        if current > 0 and current < total_cells:
            cell_styles[current] = _SEP_STYLE
            cell_owners[current] = None
            current += 1

    for idx, (job, nc) in enumerate(large_queue_jobs):
        style = JOB_STYLES[idx % len(JOB_STYLES)]
        cells_needed = max(1, int(nc / nodes_per_cell))
        if prev_owner is not None:
            _insert_sep()
        for _ in range(cells_needed):
            if current < total_cells:
                cell_styles[current] = style
                cell_owners[current] = job.id
                current += 1
        prev_owner = job.id
        remaining = _time_remaining(job, ref)
        time_str = _format_remaining(remaining)
        legend_entries.append((style, job.user, job.queue, nc, time_str))

    # Aggregated queues
    agg_legend: List[Tuple[str, str, int]] = []
    for queue_name, nodes in agg_queue_nodes.items():
        style = AGGREGATED_QUEUE_STYLES.get(queue_name, _AGGREGATED_QUEUE_DEFAULT_STYLE)
        cells_needed = max(1, int(nodes / nodes_per_cell))
        if prev_owner is not None:
            _insert_sep()
        for _ in range(cells_needed):
            if current < total_cells:
                cell_styles[current] = style
                cell_owners[current] = f"queue:{queue_name}"
                current += 1
        prev_owner = f"queue:{queue_name}"
        agg_legend.append((style, queue_name, nodes))

    # Separator before empty/available region
    if prev_owner is not None and current < total_cells:
        _insert_sep()

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

    # ── proportional legend bar ─────────────────────────────────────
    # Each segment's width ∝ its node count.  When a segment is too
    # narrow for its label, a circled number is shown instead and a
    # numbered key is printed below.
    bar_width = grid_width

    # Collect segments: (style, full_label, node_count)
    segments: List[Tuple[str, str, int]] = []
    for style, user, queue, nodes, time_str in legend_entries:
        detail = f"{user} {nodes:,}n {queue}"
        if time_str:
            detail += f" [{time_str}]"
        segments.append((style, detail, nodes))
    for style, queue_name, nodes in agg_legend:
        segments.append((style, f"{queue_name} ({nodes:,}n)", nodes))
    segments.append((_EMPTY_STYLE, f"Available ({available_nodes:,}n)", available_nodes))

    # Calculate proportional widths
    remaining_width = bar_width
    remaining_nodes = total_nodes
    seg_widths: List[int] = []
    for _, _, nodes in segments:
        if remaining_nodes <= 0:
            seg_widths.append(0)
            continue
        w = max(1, round(nodes / remaining_nodes * remaining_width)) if nodes > 0 else 0
        seg_widths.append(w)
        remaining_width -= w
        remaining_nodes -= nodes

    # Circled number glyphs for compact references
    _CIRCLED = "❶❷❸❹❺❻❼❽❾❿⓫⓬⓭⓮⓯⓰⓱⓲⓳⓴"

    legend_bar = Text()
    label_row = Text()
    footnotes: List[Tuple[str, str, str]] = []  # (number_str, style, full_label)

    for i, ((style, label, _), w) in enumerate(zip(segments, seg_widths)):
        if w <= 0:
            continue
        # Decide what text to show in the bar segment
        if len(label) + 2 <= w:
            # Label fits with padding
            bar_label = label
            pad_left = (w - len(bar_label)) // 2
            pad_right = w - len(bar_label) - pad_left
            legend_bar.append(" " * pad_left, style=style)
            legend_bar.append(bar_label, style=f"bold {style}")
            legend_bar.append(" " * pad_right, style=style)
            # Label row mirrors
            label_row.append(" " * pad_left)
            label_row.append(bar_label, style="bold")
            label_row.append(" " * pad_right)
        else:
            # Too narrow — show a number in the bar, add a footnote
            num_str = _CIRCLED[i] if i < len(_CIRCLED) else f"{i + 1}"
            footnotes.append((num_str, style, label))
            # Center the number in the segment
            pad_left = (w - 1) // 2
            pad_right = w - 1 - pad_left
            legend_bar.append(" " * pad_left, style=style)
            legend_bar.append(num_str, style=f"bold {style}")
            legend_bar.append(" " * pad_right, style=style)
            # Label row: just the number
            label_row.append(" " * pad_left)
            label_row.append(num_str, style="bold")
            label_row.append(" " * pad_right)

    # Footnotes row — numbered key for segments that didn't fit
    footnote_text = Text()
    if footnotes:
        for num_str, style, full_label in footnotes:
            footnote_text.append(f"  {num_str} ", style=_fg(style))
            footnote_text.append(full_label, style="dim")

    return GridData(
        header=header,
        grid_text=grid,
        legend=Group(legend_bar, label_row, footnote_text) if footnotes else Group(legend_bar, label_row),
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


class _HeaderPanel(Static):
    """Status header above the grid."""


class _LegendBar(Static):
    """Proportional legend bar below the grid."""


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
    ClusterGridWidget _HeaderPanel {
        height: auto;
    }
    ClusterGridWidget _LegendBar {
        height: auto;
        margin-top: 1;
    }
    """

    def compose(self):
        yield _HeaderPanel(id="cluster_header")
        yield _GridPanel(id="cluster_grid_panel")
        yield _LegendBar(id="cluster_legend_bar")

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        data = _build_grid(snapshot)
        self.query_one(_HeaderPanel).update(data.header)
        self.query_one(_GridPanel).set_grid(data)
        self.query_one(_LegendBar).update(data.legend)

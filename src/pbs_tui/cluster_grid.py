"""Cluster-wide node utilization grid widget."""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import colorsys

from rich.console import Group, RenderableType
from rich.text import Text
from textual.containers import VerticalScroll
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Static

from .data import Job, SchedulerSnapshot
from .nodes import job_node_summary
from .time_utils import time_remaining, format_remaining

# ── colour palette ──────────────────────────────────────────────────────

# Theme variable names used as seed colours for job blocks.
_THEME_SEED_KEYS = [
    "primary", "secondary", "accent", "warning", "error", "success", "boost",
]

# Fallback hex colours when theme variables are unavailable
_FALLBACK_SEEDS = [
    "#4DB2FF", "#89DDFF", "#F8BD96", "#F9E2AF", "#F38BA8", "#94E2D5", "#CBA6F7",
]

# Theme names that should use ANSI named colors instead of hex
_ANSI_THEME_NAMES = frozenset({"ansi-dark", "ansi-light"})


def _strip_alpha(h: str) -> str:
    """Normalise a hex colour to 6-digit RGB, stripping any alpha channel."""
    bare = h.lstrip("#")
    if len(bare) == 8:
        bare = bare[:6]
    return f"#{bare}"


def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
    bare = h.lstrip("#")[:6]
    return int(bare[0:2], 16), int(bare[2:4], 16), int(bare[4:6], 16)


def _rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{r:02x}{g:02x}{b:02x}"


def _darken(hex_color: str, factor: float = 0.35) -> str:
    """Return a darker variant of *hex_color*."""
    r, g, b = _hex_to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    l = max(0.0, l * (1 - factor))
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return _rgb_to_hex(int(r2 * 255), int(g2 * 255), int(b2 * 255))


def _lighten(hex_color: str, factor: float = 0.2) -> str:
    """Return a lighter variant of *hex_color*."""
    r, g, b = _hex_to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    l = min(1.0, l + factor)
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return _rgb_to_hex(int(r2 * 255), int(g2 * 255), int(b2 * 255))


def _desaturate(hex_color: str, factor: float = 0.5) -> str:
    """Return a muted/desaturated variant of *hex_color*."""
    r, g, b = _hex_to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    s = s * (1 - factor)
    l = min(1.0, l * 0.7 + 0.15)  # push toward mid-range
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return _rgb_to_hex(int(r2 * 255), int(g2 * 255), int(b2 * 255))


def _shift_hue(hex_color: str, offset: float) -> str:
    """Rotate the hue of *hex_color* by *offset* (0.0–1.0)."""
    r, g, b = _hex_to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    h = (h + offset) % 1.0
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return _rgb_to_hex(int(r2 * 255), int(g2 * 255), int(b2 * 255))


def _generate_job_colors(seeds: List[str], count: int) -> List[str]:
    """Generate *count* distinct hex colours from *seeds* via hue-shifting.

    Uses the seed colours directly first, then produces additional variants
    by rotating hue in evenly-spaced increments.
    """
    if count <= len(seeds):
        return seeds[:count]
    colors = list(seeds)
    needed = count - len(seeds)
    shifts_per_seed = (needed // len(seeds)) + 1
    for shift_idx in range(1, shifts_per_seed + 1):
        offset = shift_idx / (shifts_per_seed + 1)
        for seed in seeds:
            colors.append(_shift_hue(seed, offset))
            if len(colors) >= count:
                return colors[:count]
    return colors[:count]


# ANSI colors used only for ansi-dark/ansi-light themes
_ANSI_JOB_STYLES = [
    "on blue",
    "on red",
    "on green",
    "on magenta",
    "on cyan",
    "on yellow",
    "on bright_blue",
    "on bright_red",
    "on bright_green",
    "on bright_magenta",
    "on bright_cyan",
    "on bright_yellow",
    "on white",
    "on bright_black",
]


def _build_palette(
    theme_vars: Dict[str, str],
    theme_name: str = "",
    job_count: int = 0,
) -> "Palette":
    """Build a colour palette appropriate for the current theme.

    For ANSI themes (ansi-dark, ansi-light): uses the 14 named ANSI colors.
    For other themes: generates unique hex colours from the theme seed colours,
    sized to match the number of running jobs.
    """
    surface = _strip_alpha(theme_vars.get("surface", "#141B2D").strip())
    if not surface.startswith("#"):
        surface = "#141B2D"

    # Resolve seed colours from theme variables (strip alpha channels)
    seeds: List[str] = []
    for key in _THEME_SEED_KEYS:
        val = theme_vars.get(key, "").strip()
        if val and val.startswith("#"):
            seeds.append(_strip_alpha(val))
    if not seeds:
        seeds = list(_FALLBACK_SEEDS)

    # Aggregated queue colours: desaturated seeds
    agg_colors = [_desaturate(s, 0.6) for s in seeds]

    # Job colours: ANSI names for ANSI themes, hex for everything else
    if theme_name in _ANSI_THEME_NAMES:
        job_styles = list(_ANSI_JOB_STYLES)
    else:
        n = max(job_count, len(seeds))
        hex_colors = _generate_job_colors(seeds, n)
        job_styles = [f"on {c}" for c in hex_colors]

    empty_bg = _darken(surface, 0.05)
    # Foreground for textured empty cells — slightly lighter than background
    empty_fg = _lighten(empty_bg, 0.08)

    return Palette(
        job_styles=job_styles,
        agg_colors=agg_colors,
        empty_style=f"{empty_fg} on {empty_bg}",
    )


class Palette:
    """Resolved colour palette for the cluster grid."""

    __slots__ = ("job_styles", "agg_colors", "empty_style")

    def __init__(
        self,
        job_styles: List[str],
        agg_colors: List[str],
        empty_style: str,
    ) -> None:
        self.job_styles = job_styles
        self.agg_colors = agg_colors
        self.empty_style = empty_style

    def job_style(self, idx: int) -> str:
        return self.job_styles[idx % len(self.job_styles)]

    def agg_style(self, queue_name: str) -> str:
        """Return a deterministic style for an aggregated queue."""
        i = int(hashlib.md5(queue_name.encode()).hexdigest(), 16) % len(self.agg_colors)
        bg = self.agg_colors[i]
        fg = _lighten(bg, 0.15)
        return f"{fg} on {bg}"

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
    count, _ = job_node_summary(job)
    return count if count is not None else 1


_time_remaining = time_remaining
_format_remaining = format_remaining


# ── grid data structure ─────────────────────────────────────────────────


class GridData:
    """Holds the result of building the cluster grid."""

    __slots__ = (
        "header", "grid_text", "legend", "cell_owners", "cell_styles",
        "grid_width", "grid_height",
    )

    def __init__(
        self,
        header: RenderableType,
        grid_text: Text,
        legend: RenderableType,
        cell_owners: List[Optional[str]],
        cell_styles: List[str],
        grid_width: int,
        grid_height: int,
    ) -> None:
        self.header = header
        self.grid_text = grid_text
        self.legend = legend
        self.cell_owners = cell_owners
        self.cell_styles = cell_styles
        self.grid_width = grid_width
        self.grid_height = grid_height


# ── grid building ───────────────────────────────────────────────────────


def _build_grid(
    snapshot: SchedulerSnapshot,
    palette: Palette,
    grid_width: int = 100,
    grid_height: int = 28,
) -> GridData:
    """Build the cluster grid data."""

    # Guard: no node data → placeholder
    if not snapshot.nodes:
        placeholder = Text()
        placeholder.append("Cluster-wide node utilization\n", style="bold")
        placeholder.append("Node data unavailable; unable to render utilization grid.", style="dim")
        return GridData(
            header=placeholder,
            grid_text=Text(),
            legend=Text(),
            cell_owners=[],
            cell_styles=[],
            grid_width=0,
            grid_height=0,
        )

    total_nodes = len(snapshot.nodes)
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
    cell_styles: List[str] = [palette.empty_style] * total_cells
    cell_chars: List[str] = ["▒"] * total_cells  # default: textured empty
    cell_owners: List[Optional[str]] = [None] * total_cells
    current = 0

    legend_entries: List[Tuple[str, str, str, int, str]] = []

    for idx, (job, nc) in enumerate(large_queue_jobs):
        if current >= total_cells:
            break
        style = palette.job_style(idx)
        cells_needed = min(max(1, math.ceil(nc / nodes_per_cell)), total_cells - current)
        cells_assigned = 0
        for _ in range(cells_needed):
            cell_styles[current] = style
            cell_chars[current] = " "  # solid fill for jobs
            cell_owners[current] = job.id
            current += 1
            cells_assigned += 1
        if cells_assigned > 0:
            remaining = _time_remaining(job, ref)
            time_str = _format_remaining(remaining)
            legend_entries.append((style, job.user, job.queue, nc, time_str))

    # Aggregated queues — textured with ░ (sorted for stable ordering)
    agg_legend: List[Tuple[str, str, int]] = []
    for queue_name, nodes in sorted(agg_queue_nodes.items()):
        if current >= total_cells:
            break
        style = palette.agg_style(queue_name)
        cells_needed = min(max(1, math.ceil(nodes / nodes_per_cell)), total_cells - current)
        cells_assigned = 0
        for _ in range(cells_needed):
            cell_styles[current] = style
            cell_chars[current] = "░"
            cell_owners[current] = f"queue:{queue_name}"
            current += 1
            cells_assigned += 1
        if cells_assigned > 0:
            agg_legend.append((style, queue_name, nodes))

    # Stats — clamp running_nodes to total_nodes for display
    raw_running_nodes = sum(nc for _, nc in large_queue_jobs) + sum(agg_queue_nodes.values())
    running_nodes = min(raw_running_nodes, total_nodes)
    overcommit = raw_running_nodes - total_nodes
    available_nodes = max(0, total_nodes - running_nodes)
    queued_jobs = sum(1 for j in snapshot.jobs if j.state == "Q")
    utilisation = min(running_nodes / total_nodes * 100, 100.0)

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
    if overcommit > 0:
        header.append(f" (+{overcommit:,} over)", style="dim")
    header.append("  Available: ", style="dim")
    header.append(f"{available_nodes:,}", style="bold green")
    header.append("  Jobs: ", style="dim")
    header.append(f"{len(running_jobs)}", style="bold")
    header.append("R", style="dim green")
    header.append("/", style="dim")
    header.append(f"{queued_jobs}", style="bold")
    header.append("Q", style="dim yellow")

    # ── render grid ─────────────────────────────────────────────────
    grid = Text()
    for row in range(grid_height):
        for col in range(grid_width):
            idx = row * grid_width + col
            grid.append(cell_chars[idx], style=cell_styles[idx])
        if row < grid_height - 1:
            grid.append("\n")

    # ── proportional legend bar ─────────────────────────────────────
    # Each segment's width is proportional to its node count.  When a
    # segment is too narrow for its label, details are listed below.
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
    segments.append((palette.empty_style, f"Available ({available_nodes:,}n)", available_nodes))

    # Calculate proportional widths — clamp to remaining space
    remaining_width = bar_width
    remaining_nodes = total_nodes
    seg_widths: List[int] = []
    for _, _, nodes in segments:
        if remaining_nodes <= 0 or remaining_width <= 0:
            seg_widths.append(0)
            remaining_nodes -= nodes
            continue
        if nodes > 0:
            w = min(remaining_width, max(1, round(nodes / remaining_nodes * remaining_width)))
        else:
            w = 0
        seg_widths.append(w)
        remaining_width -= w
        remaining_nodes -= nodes

    # Render the proportional bar — labels inside when they fit, else blank
    legend_bar = Text()
    footnotes: List[Tuple[str, str]] = []  # (style, full_label) for narrow segments

    for (style, label, _), w in zip(segments, seg_widths):
        if w <= 0:
            continue
        if len(label) + 2 <= w:
            pad_left = (w - len(label)) // 2
            pad_right = w - len(label) - pad_left
            legend_bar.append(" " * pad_left, style=style)
            legend_bar.append(label, style=f"bold {style}")
            legend_bar.append(" " * pad_right, style=style)
        else:
            legend_bar.append(" " * w, style=style)
            footnotes.append((style, label))

    # Footnote list for segments too narrow for inline labels
    footnote_text = Text(no_wrap=False)
    if footnotes:
        for style, full_label in footnotes:
            footnote_text.append("  ██", style=_fg(style))
            footnote_text.append(f" {full_label}\n", style="dim")

    return GridData(
        header=header,
        grid_text=grid,
        legend=Group(legend_bar, footnote_text) if footnotes else legend_bar,
        cell_owners=cell_owners,
        cell_styles=cell_styles,
        grid_width=grid_width,
        grid_height=grid_height,
    )


# ── widgets ─────────────────────────────────────────────────────────────


class _GridPanel(Widget):
    """Interactive coloured grid — supports click to inspect a job."""

    DEFAULT_CSS = """
    _GridPanel {
        border: round $surface-lighten-2;
        height: 1fr;
        min-height: 10;
        padding: 0 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._content: RenderableType = Text()
        self._cell_owners: List[Optional[str]] = []
        self._cell_styles: List[str] = []
        self._grid_width: int = 0
        self._grid_height: int = 0

    def render(self) -> RenderableType:
        return self._content

    def set_grid(self, data: GridData) -> None:
        self._content = data.grid_text
        self._cell_owners = data.cell_owners
        self._cell_styles = data.cell_styles
        self._grid_width = data.grid_width
        self._grid_height = data.grid_height
        self.refresh()

    def _owner_at(self, logical_row: int, col: int) -> Optional[str]:
        """Return the cell owner at a logical grid position."""
        if logical_row < 0 or logical_row >= self._grid_height:
            return None
        idx = logical_row * self._grid_width + col
        if idx < 0 or idx >= len(self._cell_owners):
            return None
        return self._cell_owners[idx]

    def _style_at(self, logical_row: int, col: int) -> str:
        idx = logical_row * self._grid_width + col
        if 0 <= idx < len(self._cell_styles):
            return self._cell_styles[idx]
        return ""

    def on_click(self, event) -> None:
        if not self._cell_owners or self._grid_width == 0:
            return
        col = int(event.x)
        text_row = int(event.y)
        if col < 0 or col >= self._grid_width:
            return
        # The grid renders one character per cell, so text_row maps directly
        # to a logical row.  Check the clicked row first.
        owner = self._owner_at(text_row, col)
        if owner:
            style = self._style_at(text_row, col)
            self.post_message(ClusterGridWidget.CellClicked(owner, style))


class _HeaderPanel(Static):
    """Status header above the grid."""


class _LegendBar(Static):
    """Proportional legend bar below the grid."""


class ClusterGridWidget(VerticalScroll):
    """Cluster-wide node utilization grid with legend.

    Scrollable container. Call :meth:`update_from_snapshot` on each refresh.
    The grid auto-sizes to fill available width; height scales to match.
    """

    class CellClicked(Message):
        """Posted when a grid cell is clicked."""

        def __init__(self, owner: str, color_style: str = "") -> None:
            super().__init__()
            self.owner = owner
            self.color_style = color_style

    DEFAULT_CSS = """
    ClusterGridWidget {
        height: 1fr;
        padding: 1 2;
    }
    ClusterGridWidget _HeaderPanel {
        height: auto;
    }
    ClusterGridWidget _GridPanel {
        height: 1fr;
    }
    ClusterGridWidget _LegendBar {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snapshot: Optional[SchedulerSnapshot] = None

    def compose(self):
        yield _HeaderPanel(id="cluster_header")
        yield _GridPanel(id="cluster_grid_panel")
        yield _LegendBar(id="cluster_legend_bar")

    def _rebuild(self) -> None:
        if self._snapshot is None:
            return
        panel = self.query_one(_GridPanel)
        # Use the panel's content area for grid dimensions.
        content_size = getattr(panel, "content_size", None) or panel.size
        w = content_size.width if content_size.width > 0 else 100
        h = content_size.height if content_size.height > 0 else 14

        # Build palette from current theme
        try:
            theme_vars = self.app.get_css_variables()
        except Exception:
            theme_vars = {}
        try:
            theme_name = self.app.theme or ""
        except Exception:
            theme_name = ""
        # Count non-aggregated running jobs for dynamic palette sizing
        running_job_count = sum(
            1 for j in self._snapshot.jobs
            if j.state == "R" and j.queue not in AGGREGATED_QUEUES
        )
        pal = _build_palette(theme_vars, theme_name=theme_name, job_count=running_job_count)

        data = _build_grid(self._snapshot, pal, grid_width=w, grid_height=h)
        self.query_one(_HeaderPanel).update(data.header)
        panel.set_grid(data)
        self.query_one(_LegendBar).update(data.legend)

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        self._snapshot = snapshot
        self._rebuild()

    def on_resize(self, event) -> None:
        self._rebuild()

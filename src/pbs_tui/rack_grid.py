"""Rack-organised cluster grid widget.

Renders each rack as a labelled mini-grid of node cells and supports
selecting a job (via the sidebar list, by clicking a colored cell, or via
arrow-key navigation) to highlight every node it occupies.

The rendering pipeline keeps three things separate so each can be tested in
isolation:

* ``classify_cell`` decides what a single cell represents (job owner, free,
  down, etc.) — a pure function of ``Node`` + owning ``job_id``.
* ``build_render_model`` walks a :class:`MachineLayout` and produces a
  ``RenderModel`` with a 2-D grid of cells plus reverse lookups used by the
  click handler.
* ``render_to_text`` turns a ``RenderModel`` into a Rich ``Text``.

The widget itself is a thin Textual layer over those building blocks.
"""

from __future__ import annotations

import enum
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.containers import (
    Horizontal,
    ScrollableContainer,
    Vertical,
    VerticalScroll,
)
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Static

from .cluster_grid import Palette, _build_palette
from .data import Job, Node, SchedulerSnapshot
from .nodes import job_node_assignments
from .rack_layout import MachineLayout, RackSpec, parse_node_id, detect_layout
from .time_utils import format_remaining, time_remaining


__all__ = [
    "CellState",
    "classify_cell",
    "Cell",
    "RackPlacement",
    "RenderModel",
    "build_render_model",
    "CELL_GLYPHS",
    "render_to_text",
    "build_legend_text",
    "build_header_text",
    "JobListEntry",
    "build_job_list_entries",
    "render_job_list_entry",
    "RackGridWidget",
]


class CellState(enum.Enum):
    OCCUPIED = "occupied"
    FREE = "free"
    DOWN = "down"
    UNKNOWN = "unknown"
    RESERVATION = "reservation"
    MISSING = "missing"  # slot in layout has no corresponding Node in snapshot


def classify_cell(node: Optional[Node], owner_job_id: Optional[str]) -> CellState:
    if node is None:
        return CellState.MISSING

    primary = node.primary_state()

    # job-exclusive trumps everything else
    if "job-exclusive" in node.state or primary == "job-exclusive":
        return CellState.OCCUPIED

    # The full state string can be comma-joined ("offline,down") — split.
    flags = {flag.strip() for flag in node.state.split(",") if flag.strip()}
    if flags & {"offline", "down"}:
        return CellState.DOWN
    if "state-unknown" in flags:
        return CellState.UNKNOWN
    if "resv-exclusive" in flags:
        return CellState.RESERVATION
    if owner_job_id is not None:
        return CellState.OCCUPIED
    return CellState.FREE


# ---------------------------------------------------------------------------
# Layout constants (tweakable later)
# ---------------------------------------------------------------------------
RACK_LABEL_LINES = 1      # rack-name label
RACK_UTIL_LINES = 1       # "n/N" utilization line
RACK_TRAILING_LINES = 1   # blank padding under each rack
RACK_HORIZONTAL_PAD = 1   # spaces between racks within a row
RACK_ROW_GAP = 1          # blank lines between rack rows


@dataclass(frozen=True)
class Cell:
    """One renderable cell — either a node slot or a structural element."""
    row: int
    col: int
    state: CellState
    node_name: Optional[str] = None   # None for structural cells (labels, padding)
    rack_name: Optional[str] = None
    owner_job_id: Optional[str] = None


@dataclass
class RackPlacement:
    """Where a single rack lives within the rendered grid (top-left corner)."""
    rack: str
    row: int
    col: int
    width: int
    height: int


@dataclass
class RenderModel:
    layout: MachineLayout
    width: int
    height: int
    cells_by_node: Dict[str, Cell]
    rack_placements: Dict[str, RackPlacement]
    # Reverse lookup for click handling — keyed (text_row, text_col)
    _coord_to_node: Dict[Tuple[int, int], str]
    _job_to_nodes: Dict[str, List[str]]

    def cell_at(self, row: int, col: int) -> Optional[str]:
        """Return the node name at grid (row, col), or ``None``."""
        return self._coord_to_node.get((row, col))

    def nodes_for_job(self, job_id: str) -> List[str]:
        return list(self._job_to_nodes.get(job_id, []))

    def rack_at(self, row: int, col: int) -> Optional[str]:
        """Return the rack-name label at (row, col), or ``None``.

        Only the label-line cells return a rack name — used by the click
        handler to detect rack-name clicks.
        """
        for placement in self.rack_placements.values():
            if (placement.row == row
                    and placement.col <= col < placement.col + placement.width):
                return placement.rack
        return None


def _rack_box_size(spec: RackSpec) -> Tuple[int, int]:
    """Return (width, height) of a single rack's rendered box."""
    cap = spec.capacity()
    # The util string "n/N" can be wider than either the rack name or the
    # column count (e.g. rack "r1" col=2, util "10/14" is 5 chars wide).
    util_width = len(f"{cap}/{cap}")
    label_width = max(spec.cols, len(spec.name), util_width)  # ensure all fit
    height = RACK_LABEL_LINES + RACK_UTIL_LINES + spec.rows + RACK_TRAILING_LINES
    return label_width, height


def build_render_model(
    layout: MachineLayout,
    snapshot: SchedulerSnapshot,
    job_assignments: Dict[str, List[str]],
) -> RenderModel:
    """Walk *layout* and *snapshot* to produce a :class:`RenderModel`."""
    node_by_name: Dict[str, Node] = {n.name: n for n in snapshot.nodes}
    node_to_job: Dict[str, str] = {}
    for job_id, node_names in job_assignments.items():
        for n in node_names:
            node_to_job[n] = job_id

    cells_by_node: Dict[str, Cell] = {}
    rack_placements: Dict[str, RackPlacement] = {}
    coord_to_node: Dict[Tuple[int, int], str] = {}

    cursor_y = 0
    total_width = 0
    # Use the layout's largest rack-box width so empty placeholders advance the
    # cursor by the same step as a real rack (keeps right-aligned rows aligned).
    max_box_width = max(
        (_rack_box_size(s)[0] for s in layout.rack_specs.values()),
        default=0,
    )
    for rack_row in layout.rack_rows:
        cursor_x = 0
        row_height = 0
        for rack in rack_row:
            if not rack:
                # Empty placeholder — reserve space, no rack rendered here.
                cursor_x += max_box_width + RACK_HORIZONTAL_PAD
                continue
            spec = layout.rack_specs[rack]
            box_w, box_h = _rack_box_size(spec)
            placement = RackPlacement(
                rack=rack, row=cursor_y, col=cursor_x,
                width=box_w, height=box_h,
            )
            rack_placements[rack] = placement

            slots = layout.rack_slots[rack]
            inner_top = cursor_y + RACK_LABEL_LINES + RACK_UTIL_LINES
            for idx, node_name in enumerate(slots):
                if idx >= spec.capacity():
                    break
                rr = inner_top + idx // spec.cols
                cc = cursor_x + idx % spec.cols
                node = node_by_name.get(node_name)
                owner = node_to_job.get(node_name)
                cell = Cell(
                    row=rr, col=cc,
                    state=classify_cell(node, owner),
                    node_name=node_name,
                    rack_name=rack,
                    owner_job_id=owner,
                )
                cells_by_node[node_name] = cell
                coord_to_node[(rr, cc)] = node_name

            cursor_x += box_w + RACK_HORIZONTAL_PAD
            row_height = max(row_height, box_h)

        total_width = max(total_width, cursor_x)
        cursor_y += row_height + RACK_ROW_GAP

    job_to_nodes: Dict[str, List[str]] = defaultdict(list)
    for node_name, job_id in node_to_job.items():
        if node_name in cells_by_node:
            job_to_nodes[job_id].append(node_name)

    return RenderModel(
        layout=layout,
        width=total_width,
        height=cursor_y,
        cells_by_node=cells_by_node,
        rack_placements=rack_placements,
        _coord_to_node=coord_to_node,
        _job_to_nodes=dict(job_to_nodes),
    )


# ---------------------------------------------------------------------------
# Cell glyphs and renderer
# ---------------------------------------------------------------------------

# Cell glyphs use plain ASCII so every terminal renders them at 1 cell wide.
# Down/unknown/reservation use distinct ASCII markers that read clearly
# alongside the colored x/o.
CELL_GLYPHS: Dict[CellState, str] = {
    CellState.OCCUPIED: "x",   # colored per job
    CellState.FREE: "o",       # muted outline
    CellState.DOWN: "-",       # struck-through marker
    CellState.UNKNOWN: "?",
    CellState.RESERVATION: "%",
    CellState.MISSING: " ",
}


def _bg_to_fg(style: str) -> str:
    """Convert an ``on <color>`` background style to a foreground style.

    The job palette is built as ``on <hex>`` styles for the cluster grid's
    block-fill rendering.  The rack grid colours the foreground glyph
    instead, so we strip the leading ``on``.
    """
    return style.replace("on ", "", 1) if style.startswith("on ") else style


# Foreground style for free / down / unknown / reservation cells.  These need
# a fixed muted color that is visible against the surface background; the
# cluster_grid palette's empty_style is a "<near-bg> on <bg>" pair tuned for
# block-fill rendering and would be invisible if used directly here.
_FREE_FG = "color(244)"
_DOWN_FG = "color(248)"
_UNKNOWN_FG = "color(248)"
_RESERVATION_FG = "color(140)"


def _state_style(state: CellState, palette: Palette) -> str:
    """Foreground style for a non-occupied cell.

    *palette* is accepted for symmetry with the occupied-cell branch but
    intentionally not used; the cluster_grid palette's empty colour is a
    background-fill pair that doesn't translate to a foreground glyph.
    """
    if state == CellState.FREE:
        return _FREE_FG
    if state == CellState.DOWN:
        return _DOWN_FG
    if state == CellState.UNKNOWN:
        return _UNKNOWN_FG
    if state == CellState.RESERVATION:
        return _RESERVATION_FG
    return _FREE_FG


def _highlight_style(base_style: str) -> str:
    """Build a highlight style for a selected job's cells.

    The base style is a foreground colour; we add a bold underline to make
    selected cells visually pop without needing a background fill.
    """
    return f"bold underline {base_style}"


def render_to_text(
    model: RenderModel,
    *,
    palette: Palette,
    running_jobs: Dict[str, int],
    selected_job_id: Optional[str],
) -> Text:
    """Render *model* as a Rich Text grid.

    *running_jobs* maps job_id → palette index so cell colors stay consistent
    with the legend.  *selected_job_id*, if not None, marks every cell of that
    job with a highlight style (bold underline).
    """
    # Build a 2-D char grid + style grid initialised to spaces.
    chars: List[List[str]] = [[" "] * model.width for _ in range(model.height)]
    styles: List[List[str]] = [[""] * model.width for _ in range(model.height)]

    # Place rack labels.
    for placement in model.rack_placements.values():
        label = placement.rack
        label_pad = max(0, (placement.width - len(label)) // 2)
        for i, ch in enumerate(label):
            c = placement.col + label_pad + i
            if 0 <= c < model.width:
                chars[placement.row][c] = ch
                styles[placement.row][c] = "bold"

    # Count occupied per rack for the utilization line.
    occupied_per_rack: Dict[str, int] = defaultdict(int)
    # Use actual slot count rather than spec capacity so the denominator
    # reflects real slots (Aurora geometry rounding means len(slots) can be
    # less than spec.capacity()).
    total_per_rack: Dict[str, int] = {
        rack: len(model.layout.rack_slots.get(rack, [])) or model.layout.rack_specs[rack].capacity()
        for rack in model.rack_placements
    }
    for cell in model.cells_by_node.values():
        if cell.rack_name and cell.state == CellState.OCCUPIED:
            occupied_per_rack[cell.rack_name] += 1

    # Write utilization lines (row = placement.row + RACK_LABEL_LINES).
    for placement in model.rack_placements.values():
        util = f"{occupied_per_rack[placement.rack]}/{total_per_rack[placement.rack]}"
        util_pad = max(0, (placement.width - len(util)) // 2)
        util_row = placement.row + RACK_LABEL_LINES
        for i, ch in enumerate(util):
            c = placement.col + util_pad + i
            if 0 <= c < model.width and util_row < model.height:
                chars[util_row][c] = ch
                styles[util_row][c] = "dim"

    # Place node cells.
    for cell in model.cells_by_node.values():
        glyph = CELL_GLYPHS[cell.state]
        if cell.state == CellState.OCCUPIED and cell.owner_job_id in running_jobs:
            # Job style is "on <hex>"; we want the colour as foreground.
            base_style = _bg_to_fg(palette.job_style(running_jobs[cell.owner_job_id]))
        else:
            base_style = _state_style(cell.state, palette)
        if selected_job_id is not None:
            if cell.owner_job_id == selected_job_id:
                base_style = _highlight_style(base_style)
            else:
                # Dim every non-selected cell so the selection stands out.
                base_style = f"dim {base_style}"
        if 0 <= cell.row < model.height and 0 <= cell.col < model.width:
            chars[cell.row][cell.col] = glyph
            styles[cell.row][cell.col] = base_style

    # Assemble Rich Text — append per-cell to keep style runs accurate.
    out = Text()
    for r in range(model.height):
        for c in range(model.width):
            out.append(chars[r][c], style=styles[r][c] or None)
        if r < model.height - 1:
            out.append("\n")
    return out


# ---------------------------------------------------------------------------
# Header and legend widgets
# ---------------------------------------------------------------------------

class _RackHeader(Static):
    """One-line header summarising the cluster: machine name + counts."""


class _RackLegend(Static):
    """One-line legend mapping glyphs to states."""


def build_legend_text() -> Text:
    legend = Text()
    legend.append(CELL_GLYPHS[CellState.OCCUPIED], style="bold")
    legend.append(" job  ")
    legend.append(CELL_GLYPHS[CellState.FREE], style=_FREE_FG)
    legend.append(" free  ")
    legend.append(CELL_GLYPHS[CellState.DOWN], style=_DOWN_FG)
    legend.append(" down  ")
    legend.append(CELL_GLYPHS[CellState.UNKNOWN], style=_UNKNOWN_FG)
    legend.append(" unknown  ")
    legend.append(CELL_GLYPHS[CellState.RESERVATION], style=_RESERVATION_FG)
    legend.append(" reservation  ")
    legend.append("(click a cell to select its job)", style="dim")
    return legend


def build_header_text(layout: MachineLayout, snapshot: SchedulerSnapshot,
                      assignments: Dict[str, List[str]]) -> Text:
    total = len(snapshot.nodes)
    free = sum(1 for n in snapshot.nodes if n.primary_state() == "free")
    down = sum(1 for n in snapshot.nodes if n.primary_state() in {"offline", "down"})
    running_node_count = sum(len(v) for v in assignments.values())
    header = Text()
    header.append(layout.name.title(), style="bold")
    header.append("  ")
    header.append(f"{total:,}", style="bold")
    header.append(" nodes  ")
    header.append(f"{running_node_count:,}", style="bold cyan")
    header.append(" running  ")
    header.append(f"{free:,}", style="bold green")
    header.append(" free  ")
    if down:
        header.append(f"{down:,}", style="bold red")
        header.append(" down")
    return header


# ---------------------------------------------------------------------------
# Sidebar job list — data model and renderer
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JobListEntry:
    job_id: str
    user: str
    queue: str
    node_count: int
    palette_index: int
    time_remaining_str: str
    nodes: List[str]


def build_job_list_entries(
    snapshot: SchedulerSnapshot,
    assignments: Dict[str, List[str]],
    *,
    palette_index: Dict[str, int],
    rack_filter: Optional[str] = None,
) -> List[JobListEntry]:
    """Build the sidebar entry list (running jobs only).

    Jobs are returned sorted by node count descending.  When *rack_filter* is
    provided, only jobs with at least one node whose rack matches the filter
    string are included.
    """
    entries: List[JobListEntry] = []
    ref = snapshot.timestamp
    by_id: Dict[str, Job] = {j.id: j for j in snapshot.jobs}
    for job_id, nodes in assignments.items():
        job = by_id.get(job_id)
        if job is None or job.state != "R":
            continue
        if rack_filter is not None:
            if not any(_node_matches_rack(n, rack_filter) for n in nodes):
                continue
        remaining = time_remaining(job, ref)
        entries.append(JobListEntry(
            job_id=job.id,
            user=job.user or "?",
            queue=job.queue or "",
            node_count=len(nodes),
            palette_index=palette_index.get(job.id, 0),
            time_remaining_str=format_remaining(remaining),
            nodes=list(nodes),
        ))
    entries.sort(key=lambda e: e.node_count, reverse=True)
    return entries


def _node_matches_rack(node_name: str, rack: str) -> bool:
    """Return True when *node_name* belongs to *rack*."""
    parsed = parse_node_id(node_name)
    return parsed is not None and parsed.rack == rack


def render_job_list_entry(entry: JobListEntry, palette: Palette,
                          *, selected: bool) -> Text:
    """Render one sidebar row as Rich Text."""
    style = palette.job_style(entry.palette_index)
    fg = style.replace("on ", "", 1) if style.startswith("on ") else style
    text = Text()
    text.append("x ", style=fg)
    label = f"{entry.user} {entry.node_count}n {entry.queue}"
    if entry.time_remaining_str:
        label += f" [{entry.time_remaining_str}]"
    if selected:
        text.append(label, style="bold")
    else:
        text.append(label)
    return text


# ---------------------------------------------------------------------------
# Rack panel widget — rendering + click handling
# ---------------------------------------------------------------------------


class _RackPanel(ScrollableContainer):
    """Renders the rack grid and emits clicks as messages.

    Uses a ScrollableContainer wrapping a child Static so the rack grid can
    extend beyond the viewport horizontally and vertically without wrapping
    the rack rows.
    """

    DEFAULT_CSS = """
    _RackPanel {
        width: 1fr;
        height: 1fr;
        overflow-x: scroll;
        overflow-y: scroll;
        padding: 0 1;
    }
    _RackPanel > Static {
        width: auto;
        height: auto;
    }
    _RackPanel:focus {
        border: tall $accent;
    }
    """

    BINDINGS = [
        ("up", "scroll_up", "Up"),
        ("down", "scroll_down", "Down"),
        ("left", "scroll_left", "Left"),
        ("right", "scroll_right", "Right"),
        ("pageup", "page_up", "PgUp"),
        ("pagedown", "page_down", "PgDn"),
        ("home", "scroll_home", "Top"),
        ("end", "scroll_end", "Bottom"),
        ("escape", "clear_selection", "Clear"),
    ]

    can_focus = True

    class CellClicked(Message):
        def __init__(
            self,
            node_name: Optional[str],
            rack_name: Optional[str],
            owner_job_id: Optional[str] = None,
        ) -> None:
            super().__init__()
            self.node_name = node_name
            self.rack_name = rack_name
            self.owner_job_id = owner_job_id

    class SelectionCleared(Message):
        pass

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._inner = Static("", id="rack_panel_inner", expand=False)
        self._model: Optional[RenderModel] = None

    def compose(self):
        yield self._inner

    def update(self, content: Text, model: RenderModel) -> None:
        # no_wrap on the Text would still let Rich measure; the inner Static
        # being width: auto makes the container respect the actual rendered
        # width and surface horizontal scrolling.
        content.no_wrap = True
        content.overflow = "ignore"
        self._inner.update(content)
        self._model = model

    def action_clear_selection(self) -> None:
        self.post_message(self.SelectionCleared())

    def on_click(self, event) -> None:
        if self._model is None:
            return
        # event.x / event.y are container-local; add scroll offsets to map
        # back to the underlying RenderModel's absolute coordinates.
        col = int(event.x) + int(self.scroll_offset.x)
        row = int(event.y) + int(self.scroll_offset.y)
        node_name = self._model.cell_at(row, col)
        if node_name is not None:
            # Look up the owner directly from the pre-computed cell data.
            owner_job_id = self._model.cells_by_node[node_name].owner_job_id
            self.post_message(
                self.CellClicked(
                    node_name=node_name,
                    rack_name=None,
                    owner_job_id=owner_job_id,
                )
            )
            return
        rack_name = self._model.rack_at(row, col)
        if rack_name is not None:
            self.post_message(self.CellClicked(node_name=None, rack_name=rack_name))


class _JobRow(Static):
    """A single clickable job entry inside the sidebar list.

    One widget per entry means Textual routes clicks straight to the right row
    regardless of scroll position or line-wrapping, and the cursor can be
    scrolled into view with ``scroll_to_widget`` — no manual coordinate math.
    """

    DEFAULT_CSS = """
    _JobRow {
        width: 1fr;
        height: auto;
    }
    """

    def __init__(self, index: int, job_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.index = index
        self.job_id = job_id

    def on_click(self, event) -> None:
        # Stop the click bubbling to the container's filter-chip handler.
        event.stop()
        self.post_message(_JobListWidget.RowClicked(self.index))


class _JobListFilterChip(Static):
    """Header chip shown when a rack filter is active; clicking it clears."""

    DEFAULT_CSS = """
    _JobListFilterChip {
        width: 1fr;
        height: auto;
        margin-bottom: 1;
    }
    """

    def on_click(self, event) -> None:
        event.stop()
        self.post_message(_JobListWidget.FilterCleared())


class _JobListWidget(VerticalScroll):
    """Scrollable sidebar listing running jobs; emits JobChosen on selection.

    Implemented as a :class:`VerticalScroll` holding one :class:`_JobRow` child
    per entry (plus an optional filter chip).  This makes the list scroll when
    it overflows the viewport — a plain ``Widget`` that renders its own ``Text``
    is clamped to the viewport height and never scrolls.
    """

    DEFAULT_CSS = """
    _JobListWidget {
        width: 32;
        min-width: 32;
        height: 1fr;
        border-left: tall $surface-lighten-1;
        padding: 0 1;
    }
    _JobListWidget:focus {
        border-left: tall $accent;
    }
    """

    BINDINGS = [
        ("up", "move(-1)", "Up"),
        ("down", "move(1)", "Down"),
        ("enter", "choose", "Select"),
        ("escape", "clear", "Clear"),
    ]

    can_focus = True

    class JobChosen(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class FilterCleared(Message):
        pass

    class RowClicked(Message):
        def __init__(self, index: int) -> None:
            super().__init__()
            self.index = index

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._entries: List[JobListEntry] = []
        self._cursor: int = 0
        self._palette: Optional[Palette] = None
        self._selected_id: Optional[str] = None
        self._rack_filter: Optional[str] = None

    def on_mount(self) -> None:
        # The parent may call update() before this widget is mounted (during its
        # own compose/rebuild). Render whatever state we captured once mounted.
        self._rebuild_children()

    def update(self, entries: List[JobListEntry], palette: Palette,
               selected_id: Optional[str], rack_filter: Optional[str]) -> None:
        self._entries = entries
        self._palette = palette
        self._selected_id = selected_id
        self._rack_filter = rack_filter
        self._cursor = max(0, min(self._cursor, len(entries) - 1))
        self._rebuild_children()

    def _rebuild_children(self) -> None:
        # Only reconcile once mounted; before mount there is no place to put
        # children (update() may be called by the parent during its own build).
        if not self.is_mounted:
            return
        self.remove_children()
        widgets: List[Widget] = []
        if self._rack_filter:
            chip = Text()
            chip.append("Filtered: ", style="dim")
            chip.append(self._rack_filter, style="bold")
            chip.append(f"  ({len(self._entries)} jobs)\n", style="dim")
            chip.append("[clear: esc or click chip]", style="dim")
            widgets.append(_JobListFilterChip(chip))
        if not self._entries:
            widgets.append(Static(Text("(no running jobs)", style="dim")))
        elif self._palette is None:
            widgets.append(Static(Text("(loading…)", style="dim")))
        else:
            for i, entry in enumerate(self._entries):
                row = _JobRow(i, entry.job_id)
                row.update(self._render_row(i, entry))
                widgets.append(row)
        self.mount_all(widgets)

    def _render_row(self, index: int, entry: JobListEntry) -> Text:
        assert self._palette is not None
        prefix = "▶ " if index == self._cursor else "  "
        text = Text(prefix)
        text.append_text(render_job_list_entry(
            entry, self._palette,
            selected=(self._selected_id == entry.job_id),
        ))
        return text

    def _refresh_row(self, index: int) -> None:
        """Re-render a single row's content in place (cheap cursor repaint)."""
        if not (0 <= index < len(self._entries)) or self._palette is None:
            return
        rows = self.query(_JobRow)
        for row in rows:
            if row.index == index:
                row.update(self._render_row(index, self._entries[index]))
                return

    def _scroll_cursor_into_view(self) -> None:
        for row in self.query(_JobRow):
            if row.index == self._cursor:
                self.scroll_to_widget(row, animate=False)
                return

    def action_move(self, delta: int) -> None:
        if not self._entries:
            return
        previous = self._cursor
        self._cursor = (self._cursor + delta) % len(self._entries)
        # Repaint only the two affected rows instead of rebuilding the list.
        self._refresh_row(previous)
        self._refresh_row(self._cursor)
        self._scroll_cursor_into_view()

    def action_choose(self) -> None:
        if not self._entries:
            return
        chosen = self._entries[self._cursor]
        self.post_message(self.JobChosen(chosen.job_id))

    def action_clear(self) -> None:
        self.post_message(self.FilterCleared())

    def on__job_list_widget_row_clicked(self, event: "RowClicked") -> None:
        event.stop()
        index = event.index
        if not (0 <= index < len(self._entries)):
            return
        previous = self._cursor
        self._cursor = index
        self._refresh_row(previous)
        self._refresh_row(index)
        self.post_message(self.JobChosen(self._entries[index].job_id))


# ---------------------------------------------------------------------------
# Public composite widget
# ---------------------------------------------------------------------------


class RackGridWidget(Vertical):
    """Public Racks-tab widget."""

    DEFAULT_CSS = """
    RackGridWidget {
        height: 1fr;
        padding: 1 2;
    }
    RackGridWidget _RackHeader { height: auto; }
    RackGridWidget _RackLegend { height: auto; margin-top: 1; }
    RackGridWidget Horizontal { height: 1fr; }
    """

    class JobSelected(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class NodeSelected(Message):
        def __init__(self, node_name: str) -> None:
            super().__init__()
            self.node_name = node_name

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snapshot: Optional[SchedulerSnapshot] = None
        self._selected_job_id: Optional[str] = None
        self._rack_filter: Optional[str] = None

    def compose(self):
        yield _RackHeader(id="rack_header")
        with Horizontal():
            yield _RackPanel(id="rack_panel")
            yield _JobListWidget(id="rack_job_list")
        yield _RackLegend(id="rack_legend")

    # ── public API ───────────────────────────────────────────────

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        self._snapshot = snapshot
        # Drop selection if the job is no longer running
        assignments = job_node_assignments(snapshot)
        if self._selected_job_id and self._selected_job_id not in assignments:
            self._selected_job_id = None
        self._rebuild()

    # ── internal ─────────────────────────────────────────────────

    def _rebuild(self) -> None:
        if self._snapshot is None:
            return
        snap = self._snapshot
        node_names = [n.name for n in snap.nodes]
        layout = detect_layout(node_names)
        assignments = job_node_assignments(snap)
        # Palette index per job — sort by job_id for stable assignment
        sorted_running = sorted(j for j in assignments.keys())
        palette_index = {jid: i for i, jid in enumerate(sorted_running)}

        # Build palette using the same job_count we used in cluster_grid
        try:
            theme_vars = self.app.get_css_variables()
            theme_name = self.app.theme or ""
        except Exception:
            theme_vars, theme_name = {}, ""
        palette = _build_palette(
            theme_vars, theme_name=theme_name, job_count=len(sorted_running)
        )

        model = build_render_model(layout, snap, job_assignments=assignments)
        text = render_to_text(
            model,
            palette=palette,
            running_jobs=palette_index,
            selected_job_id=self._selected_job_id,
        )

        self.query_one(_RackHeader).update(build_header_text(layout, snap, assignments))
        self.query_one(_RackPanel).update(text, model)

        entries = build_job_list_entries(
            snap, assignments,
            palette_index=palette_index,
            rack_filter=self._rack_filter,
        )
        self.query_one(_JobListWidget).update(
            entries, palette,
            selected_id=self._selected_job_id,
            rack_filter=self._rack_filter,
        )
        self.query_one(_RackLegend).update(build_legend_text())

    # ── message handlers ────────────────────────────────────────

    def on__rack_panel_selection_cleared(
        self, event: "_RackPanel.SelectionCleared"
    ) -> None:
        self._selected_job_id = None
        self._rack_filter = None
        self._rebuild()

    def on__rack_panel_cell_clicked(self, event: "_RackPanel.CellClicked") -> None:
        if event.node_name:
            # owner_job_id is pre-computed by the click handler from cells_by_node.
            owning_job = event.owner_job_id
            if owning_job:
                self._selected_job_id = owning_job
                self.post_message(self.JobSelected(owning_job))
            else:
                self.post_message(self.NodeSelected(event.node_name))
        elif event.rack_name:
            # Toggle / switch the rack filter
            self._rack_filter = (
                None if self._rack_filter == event.rack_name else event.rack_name
            )
        self._rebuild()

    def on__job_list_widget_job_chosen(
        self, event: "_JobListWidget.JobChosen"
    ) -> None:
        self._selected_job_id = event.job_id
        self.post_message(self.JobSelected(event.job_id))
        self._rebuild()

    def on__job_list_widget_filter_cleared(
        self, event: "_JobListWidget.FilterCleared"
    ) -> None:
        self._rack_filter = None
        self._selected_job_id = None
        self._rebuild()

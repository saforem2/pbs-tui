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

from .cluster_grid import Palette  # re-use job-color palette
from .data import Node, SchedulerSnapshot
from .rack_layout import MachineLayout, RackSpec


__all__ = [
    "CellState",
    "classify_cell",
    "Cell",
    "RackPlacement",
    "RenderModel",
    "build_render_model",
    "CELL_GLYPHS",
    "render_to_text",
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
    label_width = max(spec.cols, len(spec.name))  # ensure label fits
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
    for rack_row in layout.rack_rows:
        cursor_x = 0
        row_height = 0
        for rack in rack_row:
            spec = layout.rack_specs[rack]
            box_w, box_h = _rack_box_size(spec)
            placement = RackPlacement(
                rack=rack, row=cursor_y, col=cursor_x,
                width=box_w, height=box_h,
            )
            rack_placements[rack] = placement

            slots = layout.rack_slots[rack]
            inner_top = cursor_y + RACK_LABEL_LINES + RACK_UTIL_LINES
            for idx, slot in enumerate(slots):
                if idx >= spec.capacity():
                    break
                rr = inner_top + idx // spec.cols
                cc = cursor_x + idx % spec.cols
                node_name = f"{rack}-{slot}"
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

CELL_GLYPHS: Dict[CellState, str] = {
    CellState.OCCUPIED: " ",
    CellState.FREE: "░",
    CellState.DOWN: "×",
    CellState.UNKNOWN: "?",
    CellState.RESERVATION: "▒",
    CellState.MISSING: " ",
}


def _state_style(state: CellState, palette: Palette) -> str:
    """Style for a non-occupied cell."""
    if state == CellState.FREE:
        return palette.empty_style
    if state == CellState.DOWN:
        return "on color(235)"
    if state == CellState.UNKNOWN:
        return "dim"
    if state == CellState.RESERVATION:
        return "on color(60)"
    return palette.empty_style


def _invert_style(base_style: str) -> str:
    """Build a highlight style for a selected job's cells."""
    fg = base_style.replace("on ", "", 1) if base_style.startswith("on ") else base_style
    return f"reverse {fg}"


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
    job with an inverted style.
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
    total_per_rack: Dict[str, int] = {
        rack: model.layout.rack_specs[rack].capacity()
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
            base_style = palette.job_style(running_jobs[cell.owner_job_id])
        else:
            base_style = _state_style(cell.state, palette)
        if selected_job_id and cell.owner_job_id == selected_job_id:
            base_style = _invert_style(base_style)
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

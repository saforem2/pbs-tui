"""Tests for pbs_tui.rack_grid — cell classification, rendering, interactions."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pbs_tui.data import Node
from pbs_tui.rack_grid import CellState, classify_cell


def _node(state: str, name: str = "x4702-b00") -> Node:
    return Node(name=name, state=state, ncpus=208)


@pytest.mark.parametrize(
    "node_state, owner_job_id, expected",
    [
        ("free", None, CellState.FREE),
        ("job-exclusive", "job-1", CellState.OCCUPIED),
        ("job-exclusive", None, CellState.OCCUPIED),  # Owner unknown but busy
        ("offline", None, CellState.DOWN),
        ("down", None, CellState.DOWN),
        ("offline,down", None, CellState.DOWN),
        ("state-unknown", None, CellState.UNKNOWN),
        ("resv-exclusive", None, CellState.RESERVATION),
    ],
)
def test_classify_cell(node_state, owner_job_id, expected):
    cell = classify_cell(_node(node_state), owner_job_id)
    assert cell == expected


def test_classify_cell_missing_node():
    """When no Node is supplied (slot exists in layout but not in snapshot)."""
    assert classify_cell(None, None) == CellState.MISSING


from pbs_tui.data import SchedulerSnapshot
from pbs_tui.rack_grid import build_render_model
from pbs_tui.rack_layout import MachineLayout, RackSpec


def _two_rack_layout() -> MachineLayout:
    spec = RackSpec(name="r1", rows=2, cols=2)
    return MachineLayout(
        name="test",
        rack_rows=[["r1", "r2"]],
        rack_specs={"r1": spec, "r2": RackSpec(name="r2", rows=2, cols=2)},
        rack_slots={
            "r1": ["r1-a", "r1-b", "r1-c", "r1-d"],
            "r2": ["r2-a", "r2-b", "r2-c", "r2-d"],
        },
    )


def test_build_render_model_assigns_one_cell_per_slot():
    layout = _two_rack_layout()
    snap = SchedulerSnapshot(
        nodes=[
            Node(name="r1-a", state="free"),
            Node(name="r1-b", state="job-exclusive"),
            Node(name="r1-c", state="free"),
            Node(name="r1-d", state="offline"),
            Node(name="r2-a", state="free"),
            Node(name="r2-b", state="free"),
            Node(name="r2-c", state="free"),
            Node(name="r2-d", state="free"),
        ],
        jobs=[],
    )
    model = build_render_model(layout, snap, job_assignments={"j1": ["r1-b"]})
    # One cell per slot in the layout
    assert len(model.cells_by_node) == 8
    assert model.cells_by_node["r1-b"].state == CellState.OCCUPIED
    assert model.cells_by_node["r1-d"].state == CellState.DOWN
    # Job lookup
    assert model.nodes_for_job("j1") == ["r1-b"]
    # Empty job-id returns []
    assert model.nodes_for_job("does-not-exist") == []


def test_build_render_model_assigns_text_coordinates_within_grid():
    layout = _two_rack_layout()
    snap = SchedulerSnapshot(
        nodes=[Node(name=f"r{r}-{s}", state="free")
               for r in (1, 2) for s in ("a", "b", "c", "d")],
        jobs=[],
    )
    model = build_render_model(layout, snap, job_assignments={})
    # Every node should have a non-negative (row, col) within the grid bounds
    for cell in model.cells_by_node.values():
        assert 0 <= cell.row < model.height
        assert 0 <= cell.col < model.width
    # cell_at on a node's coordinates returns that node's id
    sample = next(iter(model.cells_by_node.values()))
    assert model.cell_at(sample.row, sample.col) == sample.node_name


from pbs_tui.cluster_grid import Palette
from pbs_tui.rack_grid import render_to_text


def _palette() -> Palette:
    return Palette(
        job_styles=["on blue", "on red", "on green"],
        agg_colors=["#445566"],
        empty_style="on color(236)",
    )


def test_render_to_text_produces_expected_dimensions():
    layout = _two_rack_layout()
    snap = SchedulerSnapshot(
        nodes=[Node(name=f"r{r}-{s}", state="free")
               for r in (1, 2) for s in ("a", "b", "c", "d")],
        jobs=[],
    )
    model = build_render_model(layout, snap, job_assignments={})
    text = render_to_text(model, palette=_palette(), running_jobs={}, selected_job_id=None)
    plain = text.plain
    lines = plain.splitlines()
    assert len(lines) >= model.height - 1  # trailing newline may be omitted
    # Rack labels appear on the first line
    assert "r1" in lines[0]
    assert "r2" in lines[0]


def test_render_to_text_uses_glyph_for_each_state():
    layout = _two_rack_layout()
    snap = SchedulerSnapshot(
        nodes=[
            Node(name="r1-a", state="free"),
            Node(name="r1-b", state="job-exclusive"),
            Node(name="r1-c", state="offline"),
            Node(name="r1-d", state="state-unknown"),
            Node(name="r2-a", state="resv-exclusive"),
            Node(name="r2-b", state="free"),
            Node(name="r2-c", state="free"),
            Node(name="r2-d", state="free"),
        ],
        jobs=[],
    )
    model = build_render_model(
        layout, snap, job_assignments={"j1": ["r1-b"]}
    )
    text = render_to_text(
        model,
        palette=_palette(),
        running_jobs={"j1": 0},  # palette index 0
        selected_job_id=None,
    )
    plain = text.plain
    # Glyphs from CELL_GLYPHS should appear
    from pbs_tui.rack_grid import CELL_GLYPHS
    assert CELL_GLYPHS[CellState.FREE] in plain
    assert CELL_GLYPHS[CellState.DOWN] in plain
    assert CELL_GLYPHS[CellState.UNKNOWN] in plain
    assert CELL_GLYPHS[CellState.RESERVATION] in plain


def test_render_to_text_inverts_only_selected_job_cells():
    layout = _two_rack_layout()
    snap = SchedulerSnapshot(
        nodes=[
            Node(name="r1-a", state="job-exclusive"),
            Node(name="r1-b", state="job-exclusive"),
            Node(name="r1-c", state="job-exclusive"),
            Node(name="r1-d", state="free"),
            Node(name="r2-a", state="free"),
            Node(name="r2-b", state="free"),
            Node(name="r2-c", state="free"),
            Node(name="r2-d", state="free"),
        ],
        jobs=[],
    )
    assignments = {"j1": ["r1-a", "r1-b"], "j2": ["r1-c"]}
    model = build_render_model(layout, snap, job_assignments=assignments)
    text = render_to_text(
        model,
        palette=_palette(),
        running_jobs={"j1": 0, "j2": 1},
        selected_job_id="j1",
    )
    # Walk Text spans and verify only j1 cells got the highlight style
    # (bold underline) — j2's r1-c cell must NOT be highlighted.
    selected_coords = {
        (model.cells_by_node["r1-a"].row, model.cells_by_node["r1-a"].col),
        (model.cells_by_node["r1-b"].row, model.cells_by_node["r1-b"].col),
    }
    highlighted: list[tuple[int, int]] = []
    rendered = text.plain.splitlines()
    assert len(rendered) > 0
    cursor_row = 0
    cursor_col = 0
    for span_text, span_style in zip(*_walk_text_spans(text)):
        if "\n" in span_text:
            cursor_row += span_text.count("\n")
            cursor_col = 0
            continue
        for offset in range(len(span_text)):
            style_str = str(span_style) if span_style else ""
            if "underline" in style_str and "bold" in style_str:
                highlighted.append((cursor_row, cursor_col + offset))
        cursor_col += len(span_text)
    assert set(highlighted) == selected_coords


def _walk_text_spans(text):
    """Yield (segment_text, style) pairs for the rich Text in render order."""
    plain = text.plain
    spans = list(text.spans)
    # Build segment list by walking spans (which carry start/end indices)
    boundaries = sorted({0, len(plain)} | {s.start for s in spans} | {s.end for s in spans})
    seg_texts: list[str] = []
    seg_styles: list[str] = []
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        chunk = plain[start:end]
        # Find a span that covers this chunk; later spans win
        active_style = ""
        for s in spans:
            if s.start <= start and s.end >= end:
                active_style = s.style or active_style
        seg_texts.append(chunk)
        seg_styles.append(active_style)
    return seg_texts, seg_styles


from pbs_tui.rack_grid import build_job_list_entries, JobListEntry
from tests.util import make_job


def test_build_job_list_entries_sorts_by_node_count_descending():
    snap = SchedulerSnapshot(
        jobs=[
            make_job(id="small", state="R", queue="capacity", user="alice",
                     exec_host="r1-a/0"),
            make_job(id="big", state="R", queue="large", user="bob",
                     exec_host="r1-a/0+r1-b/0+r1-c/0"),
            make_job(id="queued", state="Q", queue="capacity", user="bob",
                     exec_host=None),
        ],
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    assignments = {"big": ["r1-a", "r1-b", "r1-c"], "small": ["r1-a"]}
    entries = build_job_list_entries(snap, assignments, palette_index={"big": 0, "small": 1})
    ids = [e.job_id for e in entries]
    assert ids == ["big", "small"]
    assert entries[0].node_count == 3
    assert entries[1].node_count == 1


def test_build_job_list_entries_filter_by_rack():
    """Filtering keeps only jobs that touch the requested rack."""
    snap = SchedulerSnapshot(
        jobs=[
            make_job(id="j1", state="R", queue="x", user="a",
                     exec_host="r1-a/0+r1-b/0"),
            make_job(id="j2", state="R", queue="x", user="b",
                     exec_host="r2-a/0"),
        ],
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    assignments = {"j1": ["r1-a", "r1-b"], "j2": ["r2-a"]}
    palette_index = {"j1": 0, "j2": 1}
    entries = build_job_list_entries(snap, assignments, palette_index=palette_index,
                                     rack_filter="r1")
    assert [e.job_id for e in entries] == ["j1"]

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
            "r1": ["a", "b", "c", "d"],
            "r2": ["a", "b", "c", "d"],
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

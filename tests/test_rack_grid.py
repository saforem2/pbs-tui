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

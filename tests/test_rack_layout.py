"""Tests for pbs_tui.rack_layout — node-name parsing and layout detection."""

from __future__ import annotations

import pytest

from pbs_tui.rack_layout import NodeId, parse_node_id


@pytest.mark.parametrize(
    "name, expected",
    [
        ("x4702-b07", NodeId(rack="x4702", slot="b07", raw="x4702-b07")),
        ("x3001-b00", NodeId(rack="x3001", slot="b00", raw="x3001-b00")),
        ("x4720-b13", NodeId(rack="x4720", slot="b13", raw="x4720-b13")),
        # Generic fallback — last "-" splits rack/slot
        ("nodeA-01", NodeId(rack="nodeA", slot="01", raw="nodeA-01")),
        # Pure-name fallback — single token becomes its own rack
        ("nid000001", NodeId(rack="nid000001", slot="0", raw="nid000001")),
        # Empty / whitespace
        ("", None),
        ("   ", None),
    ],
)
def test_parse_node_id(name, expected):
    assert parse_node_id(name) == expected


from pbs_tui.rack_layout import MachineLayout, RackSpec


def test_rack_spec_construction():
    spec = RackSpec(name="x4702", rows=7, cols=2)
    assert spec.name == "x4702"
    assert spec.rows == 7
    assert spec.cols == 2


def test_machine_layout_construction():
    spec = RackSpec(name="x4702", rows=7, cols=2)
    layout = MachineLayout(
        name="aurora",
        rack_rows=[["x4702", "x4703"]],
        rack_specs={"x4702": spec, "x4703": spec},
        rack_slots={
            "x4702": [f"b{i:02d}" for i in range(14)],
            "x4703": [f"b{i:02d}" for i in range(14)],
        },
    )
    assert layout.name == "aurora"
    assert layout.rack_rows == [["x4702", "x4703"]]
    assert layout.rack_specs["x4702"].rows == 7
    assert len(layout.rack_slots["x4702"]) == 14

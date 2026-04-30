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


from pbs_tui.rack_layout import detect_layout


def test_detect_aurora_layout_from_node_names():
    # 14 Aurora-pattern names + 0 others — well above the 80% threshold
    names = [f"x4702-b{i:02d}" for i in range(14)]
    layout = detect_layout(names)
    assert layout.name == "aurora"
    # Aurora curated rack rows are listed top-down with descending row prefix
    # (x47XX above x46XX above ...). x4702 is in the top row.
    top_row = layout.rack_rows[0]
    assert "x4702" in top_row
    # Each Aurora rack has 14 slots arranged 2 cols x 7 rows
    assert layout.rack_specs["x4702"].cols == 2
    assert layout.rack_specs["x4702"].rows == 7
    assert layout.rack_slots["x4702"] == [f"b{i:02d}" for i in range(14)]


def test_detect_aurora_layout_includes_empty_racks():
    # Single observed rack — curated layout should still enumerate every rack
    names = ["x4702-b00"]
    layout = detect_layout(names)
    assert layout.name == "aurora"
    # Layout must contain more than the one observed rack
    assert len(layout.all_racks()) > 1
    # The observed rack must be present in the curated rows
    assert "x4702" in layout.all_racks()


def test_detect_generic_layout_for_unknown_names():
    # No Aurora-pattern names — should fall to generic
    names = ["nodeA-01", "nodeA-02", "nodeA-03", "nodeA-04",
             "nodeB-01", "nodeB-02"]
    layout = detect_layout(names)
    assert layout.name == "generic"
    racks = set(layout.all_racks())
    assert racks == {"nodeA", "nodeB"}
    # nodeA has 4 slots — square shape: cols=2, rows=2
    assert layout.rack_specs["nodeA"].cols == 2
    assert layout.rack_specs["nodeA"].rows == 2
    # nodeB has 2 slots — square-ish: cols=2, rows=1
    assert layout.rack_specs["nodeB"].cols == 2
    assert layout.rack_specs["nodeB"].rows == 1


def test_detect_layout_below_aurora_threshold_falls_to_generic():
    # 5 Aurora + 5 unknown = 50% < 80% threshold
    names = [f"x4702-b{i:02d}" for i in range(5)] + [f"node-{i}" for i in range(5)]
    layout = detect_layout(names)
    assert layout.name == "generic"


def test_detect_empty_layout_returns_generic_with_no_racks():
    layout = detect_layout([])
    assert layout.name == "generic"
    assert layout.all_racks() == []


def test_generic_caps_rack_rows_at_sixteen():
    # 1000 slots in one rack → cols = 32, rows capped at 16
    names = [f"big-{i:04d}" for i in range(1000)]
    layout = detect_layout(names)
    spec = layout.rack_specs["big"]
    assert spec.rows == 16
    assert spec.cols >= 32

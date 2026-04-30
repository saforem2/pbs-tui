"""Tests for pbs_tui.rack_layout — node-name parsing and layout detection."""

from __future__ import annotations

import pytest

from pbs_tui.rack_layout import NodeId, parse_node_id


@pytest.mark.parametrize(
    "name, expected",
    [
        # ALCF cabinet hostnames (Aurora + Polaris share the format)
        (
            "x4702c0s7b0n0",
            NodeId(rack="x4702", slot="c0s07b0n0", raw="x4702c0s7b0n0"),
        ),
        (
            "x3001c0s0b0n0",
            NodeId(rack="x3001", slot="c0s00b0n0", raw="x3001c0s0b0n0"),
        ),
        (
            "x4720c0s13b1n0",
            NodeId(rack="x4720", slot="c0s13b1n0", raw="x4720c0s13b1n0"),
        ),
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
    # 64 Aurora-pattern names in a single rack — well above the 80% threshold
    names = [
        f"x4702c{c}s{s}b{b}n0"
        for c in range(8) for s in range(8) for b in range(1)
    ]
    layout = detect_layout(names)
    assert layout.name == "aurora"
    # Aurora curated rack rows are listed top-down with descending row prefix
    # (x47XX above x46XX above ...). x4702 is in the top row.
    top_row = layout.rack_rows[0]
    assert "x4702" in top_row
    # 8 rack rows, 21 racks per row
    assert len(layout.rack_rows) == 8
    assert all(len(row) == 21 for row in layout.rack_rows)
    # rack_slots stores the full observed node names for x4702
    assert len(layout.rack_slots["x4702"]) == 64


def test_detect_aurora_layout_includes_empty_racks():
    # Single observed rack — curated layout should still enumerate every rack
    names = ["x4702c0s0b0n0"]
    layout = detect_layout(names)
    assert layout.name == "aurora"
    # Layout must contain all 8 * 21 = 168 racks
    assert len(layout.all_racks()) == 168
    # The observed rack must be present in the curated rows
    assert "x4702" in layout.all_racks()
    # Empty racks render with no observed nodes (renderer treats them as MISSING)
    assert layout.rack_slots["x4000"] == []


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


def test_parse_node_id_polaris():
    """Polaris hostnames look like x3005c0s7b0n0."""
    from pbs_tui.rack_layout import parse_node_id, NodeId
    # Slot id encodes (chassis, slot, blade, node) so adjacent blades are
    # distinguishable and sortable.
    assert parse_node_id("x3005c0s7b0n0") == NodeId(
        rack="x3005", slot="c0s07b0n0", raw="x3005c0s7b0n0"
    )
    assert parse_node_id("x3005c0s7b1n0") == NodeId(
        rack="x3005", slot="c0s07b1n0", raw="x3005c0s7b1n0"
    )
    # Two-digit chassis slot
    assert parse_node_id("x3112c0s13b0n0") == NodeId(
        rack="x3112", slot="c0s13b0n0", raw="x3112c0s13b0n0"
    )


def test_detect_polaris_layout_from_node_names():
    """A snapshot of Polaris-pattern names returns the curated polaris layout."""
    # 14 nodes per rack: 7 chassis slots x 2 blades.
    names = [f"x3001c0s{s}b{b}n0" for s in range(7) for b in range(2)]
    layout = detect_layout(names)
    assert layout.name == "polaris"
    # Three rack rows: 16 / 12 / 12
    assert len(layout.rack_rows) == 3
    real_per_row = [sum(1 for r in row if r) for row in layout.rack_rows]
    assert real_per_row == [16, 12, 12]
    # Rows 2 and 3 are right-aligned with 4 leading blanks
    blanks_per_row = [sum(1 for r in row if not r) for row in layout.rack_rows]
    assert blanks_per_row == [0, 4, 4]
    # Display order is right-to-left within each row
    assert layout.rack_rows[0][0] == "x3016"   # leftmost is highest
    assert layout.rack_rows[0][-1] == "x3001"  # rightmost is lowest
    # rack_slots is populated from the observed names, sorted by canonical
    # slot id, so all 14 nodes appear in chassis-then-blade order.
    assert layout.rack_slots["x3001"] == [
        f"x3001c0s{s}b{b}n0" for s in range(7) for b in range(2)
    ]
    # Racks not present in the snapshot get an empty slot list — the renderer
    # treats missing slots as MISSING cells, which is correct.
    assert layout.rack_slots["x3016"] == []


def test_polaris_takes_precedence_over_aurora_when_majority():
    """Mixed snapshot: Polaris majority should win even with some Aurora names."""
    names = [
        f"x3001c0s{s}b{b}n0" for s in range(7) for b in range(2)
    ] + [f"x4702-b{i:02d}" for i in range(2)]
    layout = detect_layout(names)
    assert layout.name == "polaris"


def test_generic_caps_rack_rows_at_sixteen():
    # 1000 slots in one rack → rows capped at 16, cols widened so capacity >= 1000
    names = [f"big-{i:04d}" for i in range(1000)]
    layout = detect_layout(names)
    spec = layout.rack_specs["big"]
    assert spec.rows == 16
    assert spec.cols >= 32


# ---------------------------------------------------------------------------
# Bug-fix regression tests
# ---------------------------------------------------------------------------

import re as _re
from pbs_tui.rack_layout import _AURORA_RACK_PATTERN


def test_aurora_regex_no_longer_matches_x4900():
    """Tightened regex must not match racks outside the x40-x47 curated grid."""
    # x4900 is row-prefix x49, which is outside the curated x40-x47 range.
    assert not _AURORA_RACK_PATTERN.match("x4900")
    # x4800 likewise is outside x40-x47.
    assert not _AURORA_RACK_PATTERN.match("x4800")
    # Original curated racks must still match.
    assert _AURORA_RACK_PATTERN.match("x4000")
    assert _AURORA_RACK_PATTERN.match("x4720")
    assert _AURORA_RACK_PATTERN.match("x4720")
    assert _AURORA_RACK_PATTERN.match("x4020")


def test_detect_aurora_includes_unmapped_racks_in_overflow_row():
    """Aurora-pattern racks outside the curated 00-20 range go to an overflow row.

    x4721 is col 21 — one past the curated x4700..x4720 range — so it must
    appear in an overflow row appended after the 8 curated rows.
    """
    # Mostly curated Aurora names (one curated rack with 64 nodes)
    curated_names = [
        f"x4702c{c}s{s}b0n0" for c in range(8) for s in range(8)
    ]
    # One uncurated rack: col 21 of the x47 row
    uncurated_names = ["x4721c0s0b0n0"]
    layout = detect_layout(curated_names + uncurated_names)

    assert layout.name == "aurora"
    # The curated layout has 8 row-prefixes; the overflow row makes 9.
    assert len(layout.rack_rows) == 9
    overflow_row = layout.rack_rows[-1]
    assert "x4721" in overflow_row
    # The unmapped rack must have a spec and populated slot list.
    assert "x4721" in layout.rack_specs
    assert layout.rack_slots["x4721"] == ["x4721c0s0b0n0"]


def test_detect_aurora_includes_x4900_via_overflow_helper():
    """x4900 doesn't match the tightened Aurora pattern, so it lands in the
    _with_overflow_racks overflow row (not the _build_aurora_layout overflow).
    """
    # Mostly curated Aurora names so detection picks aurora.
    curated_names = [
        f"x4702c{c}s{s}b0n0" for c in range(8) for s in range(8)
    ]
    # x4900 doesn't match _AURORA_RACK_PATTERN; it will be caught by
    # _with_overflow_racks after aurora wins the ratio test.
    minority_name = "x4900c0s0b0n0"
    layout = detect_layout(curated_names + [minority_name])

    assert layout.name == "aurora"
    # x4900 must be reachable in rack_slots (either in an overflow row or curated row).
    assert "x4900" in layout.rack_slots
    assert minority_name in layout.rack_slots["x4900"]


def test_detect_layout_preserves_minority_cluster_in_overflow():
    """Mixed Aurora-majority / Polaris-minority snapshot: Polaris nodes must not be dropped.

    Aurora wins the 80% vote; Polaris racks (x3001) are unknown to the Aurora
    curated layout, so _with_overflow_racks appends them as an overflow row.
    """
    # 90 Aurora nodes in one curated rack
    aurora_names = [
        f"x4702c{c}s{s}b0n0" for c in range(8) for s in range(8)
    ]  # 64 names
    # 7 Polaris nodes in x3001 (well below 80% threshold individually)
    polaris_names = [f"x3001c0s{s}b0n0" for s in range(7)]

    layout = detect_layout(aurora_names + polaris_names)

    assert layout.name == "aurora"
    # Polaris rack must appear in rack_slots so its nodes will render.
    assert "x3001" in layout.rack_slots
    assert len(layout.rack_slots["x3001"]) == 7
    # Confirm all polaris names are present.
    assert set(layout.rack_slots["x3001"]) == set(polaris_names)


def test_generic_layout_capacity_sufficient_for_all_nodes():
    """Generic layout must not drop nodes when n > rows * cols before the fix.

    With n=1000, the old code produced cols=32, rows=min(16, ceil(1000/32))=16,
    giving capacity 512 — 488 nodes dropped.  The fix widens cols so
    capacity >= n.
    """
    names = [f"bigcluster-{i:04d}" for i in range(1000)]
    layout = detect_layout(names)
    spec = layout.rack_specs["bigcluster"]
    # capacity must cover every node
    assert spec.capacity() >= 1000
    # slots list must contain all 1000 unique names
    assert len(layout.rack_slots["bigcluster"]) == 1000

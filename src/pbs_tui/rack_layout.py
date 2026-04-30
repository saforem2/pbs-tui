"""Rack-name parsing, machine layouts, and node-id grouping.

This module is intentionally independent of Textual / Rich so it can be unit
tested in isolation.  It supplies three things to ``rack_grid``:

* ``NodeId`` — parsed (rack, slot) from a node name.
* ``RackSpec`` / ``MachineLayout`` — geometry of a known machine.
* ``detect_layout(node_names)`` — returns a curated ``MachineLayout`` for
  Aurora or Polaris, or a generic fallback synthesised from the observed
  names.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


__all__ = [
    "NodeId",
    "parse_node_id",
    "RackSpec",
    "MachineLayout",
    "detect_layout",
]


# ALCF cabinet hostnames share the format "x[rack4]c[chassis]s[slot]b[blade]n[node]"
# (e.g. Aurora "x4000c0s0b0n0", Polaris "x3005c0s7b0n0").  The rack id is the
# leading "xRRRR" and the position within the rack is captured as (chassis,
# slot, blade, node) so visually-adjacent nodes (same slot, different blade)
# get distinct, sortable slot ids in our layout.
_ALCF_PATTERN = re.compile(r"^(x\d{4})c(\d+)s(\d+)b(\d+)n(\d+)$")

# Polaris racks are x30xx, x31xx, x32xx; Aurora racks are x40xx-x47xx.
_POLARIS_RACK_PATTERN = re.compile(r"^x3\d{3}$")
# Aurora racks match the curated x40NN–x47NN grid (8 row-prefixes × 21 racks).
# Using x4[0-7] ensures detection only fires for racks the layout actually
# enumerates; broader patterns like x[4-7]\d{3} would match x50xx etc. which
# are neither curated nor displayed, causing silent node drops.
_AURORA_RACK_PATTERN = re.compile(r"^x4[0-7]\d{2}$")

# Generic last-dash split, e.g. "nodeA-01" -> rack="nodeA", slot="01".
_GENERIC_DASH_PATTERN = re.compile(r"^(.*)-([^-]+)$")


@dataclass(frozen=True)
class NodeId:
    rack: str
    slot: str
    raw: str


def parse_node_id(name: str) -> Optional[NodeId]:
    """Parse *name* into a ``NodeId`` or return ``None`` if it has no content."""
    if not name or not name.strip():
        return None
    name = name.strip()

    if m := _ALCF_PATTERN.match(name):
        # Encode (chassis, slot, blade, node) into a single sortable slot id
        # so that all nodes within a rack get unique, ordered keys.
        chassis, slot, blade, node = (int(g) for g in m.groups()[1:])
        return NodeId(
            rack=m.group(1),
            slot=f"c{chassis}s{slot:02d}b{blade}n{node}",
            raw=name,
        )

    if m := _GENERIC_DASH_PATTERN.match(name):
        return NodeId(rack=m.group(1), slot=m.group(2), raw=name)

    # No separator at all — treat the whole name as its own rack with one slot.
    return NodeId(rack=name, slot="0", raw=name)


@dataclass(frozen=True)
class RackSpec:
    name: str
    rows: int
    cols: int

    def capacity(self) -> int:
        return self.rows * self.cols


@dataclass(frozen=True)
class MachineLayout:
    """Geometry + node-name mapping for a known (or generic) machine.

    ``rack_slots`` maps each rack name to an ordered list of *node names* —
    the canonical hostname the snapshot will refer to.  The builder uses
    this list directly so different machines can have different name shapes
    (Aurora: ``x4702-b07``, Polaris: ``x3005c0s7b0n0``) without the renderer
    having to know.
    """

    name: str
    rack_rows: List[List[str]]
    rack_specs: Dict[str, RackSpec]
    rack_slots: Dict[str, List[str]]

    def all_racks(self) -> List[str]:
        return [rack for row in self.rack_rows for rack in row if rack]


# Aurora curated layout — rack rows listed top-to-bottom with descending row
# prefix (x47XX above x46XX, etc.) to mirror the ALCF status page.
# Each rack row has 21 racks numbered xRR00..xRR20.  Aurora racks are
# extremely dense (~64 nodes per rack = 8 chassis x 8 nodes), so we use a
# wider grid shape than Polaris.
_AURORA_ROW_PREFIXES = ("x47", "x46", "x45", "x44", "x43", "x42", "x41", "x40")
_AURORA_RACK_COLS_PER_ROW = 21  # racks numbered 00..20 within each row


def _build_aurora_layout(node_names: Iterable[str]) -> MachineLayout:
    """Build the Aurora layout, populating each rack's slot list from
    *node_names* so the renderer sees the real hostnames.

    Aurora hostnames share the ALCF "xRRRRcCsSbBnN" format with Polaris;
    only the rack-id prefix distinguishes them (x40-x47 vs x30-x32).
    """
    # Group observed Aurora node names by rack.
    rack_to_nodes: Dict[str, List[str]] = {}
    for name in node_names:
        if not name:
            continue
        m = _ALCF_PATTERN.match(name)
        if m is None or not _AURORA_RACK_PATTERN.match(m.group(1)):
            continue
        rack = m.group(1)
        rack_to_nodes.setdefault(rack, []).append(name)
    for nodes in rack_to_nodes.values():
        nodes.sort(key=lambda n: parse_node_id(n).slot)  # type: ignore[union-attr]

    # Use the largest observed rack to size every rack's mini-grid so racks
    # share dimensions.  Aurora-style chassis layouts are roughly 2:1 wide,
    # so prefer cols >= rows.
    max_n = max((len(v) for v in rack_to_nodes.values()), default=64)
    cols = max(2, math.ceil(math.sqrt(max_n * 2)))
    rows = max(1, math.ceil(max_n / cols))

    rack_rows: List[List[str]] = []
    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    for prefix in _AURORA_ROW_PREFIXES:
        row: List[str] = []
        for col in range(_AURORA_RACK_COLS_PER_ROW):
            name = f"{prefix}{col:02d}"
            row.append(name)
            rack_specs[name] = RackSpec(name=name, rows=rows, cols=cols)
            # Use observed nodes if present, else empty list (renders as
            # all-MISSING cells, which is correct for racks the snapshot
            # doesn't mention).
            rack_slots[name] = rack_to_nodes.get(name, [])
        rack_rows.append(row)

    # Overflow row — any Aurora-pattern racks that the curated x40NN..x47NN
    # grid doesn't enumerate (e.g. x4721 is col 21, which is beyond the
    # 00..20 range).  Append them right-to-left to match Aurora's display
    # convention so they remain visible rather than being silently dropped.
    curated_rack_names = set(rack_specs.keys())
    overflow_row: List[str] = []
    for rack_name in sorted(rack_to_nodes.keys(), reverse=True):
        if rack_name not in curated_rack_names:
            overflow_row.append(rack_name)
            rack_specs[rack_name] = RackSpec(name=rack_name, rows=rows, cols=cols)
            rack_slots[rack_name] = rack_to_nodes[rack_name]
    if overflow_row:
        rack_rows.append(overflow_row)

    return MachineLayout(
        name="aurora",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )


# Polaris curated layout — three rack rows with descending suffix columns
# (rightmost rack is x3001 / x3101 / x3201, leftmost rack is x3016 / x3112 /
# x3212).  Rows 2 and 3 are right-aligned under row 1; the leading 4 slots
# of those rows are filled with empty placeholders ("") that the renderer
# skips for placement but reserves for spacing.
#
# Row 1 contains 16 racks numbered x3001..x3016, displayed right-to-left so
# x3016 sits leftmost and x3001 sits rightmost.  Rows 2 and 3 each contain
# 12 racks numbered x31RR / x32RR (01..12) right-aligned under row 1.
_POLARIS_ROWS = (
    ("x30", 1, 16),
    ("x31", 1, 12),
    ("x32", 1, 12),
)
_POLARIS_TOP_ROW_WIDTH = 16


def _build_polaris_layout(node_names: Iterable[str]) -> MachineLayout:
    """Build the Polaris layout, populating each rack's slot list from
    *node_names* so the renderer sees the real hostnames.

    Slots within a rack are ordered by their canonical (chassis, slot,
    blade, node) tuple from ``parse_node_id`` so adjacent nodes stay
    adjacent in the rendered grid.
    """
    # Group observed Polaris node names by rack, sorted by canonical slot id.
    rack_to_nodes: Dict[str, List[str]] = {}
    for name in node_names:
        if not name:
            continue
        m = _ALCF_PATTERN.match(name)
        if m is None or not _POLARIS_RACK_PATTERN.match(m.group(1)):
            continue
        rack = m.group(1)
        rack_to_nodes.setdefault(rack, []).append(name)
    for nodes in rack_to_nodes.values():
        nodes.sort(key=lambda n: parse_node_id(n).slot)  # type: ignore[union-attr]

    # Compute the per-rack grid shape from the largest observed rack so all
    # racks share dimensions.  Polaris has 14 nodes/rack today, but the shape
    # adjusts automatically if the cluster grows.
    max_n = max((len(v) for v in rack_to_nodes.values()), default=14)
    cols = 2
    rows = max(1, math.ceil(max_n / cols))

    rack_rows: List[List[str]] = []
    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    for prefix, lo, hi in _POLARIS_ROWS:
        # Right-align rows 2 and 3 under row 1.
        leading_blanks = _POLARIS_TOP_ROW_WIDTH - (hi - lo + 1)
        row: List[str] = [""] * leading_blanks
        # Display order is right-to-left within each row.
        for n in range(hi, lo - 1, -1):
            name = f"{prefix}{n:02d}"
            row.append(name)
            rack_specs[name] = RackSpec(name=name, rows=rows, cols=cols)
            # Use observed nodes if we have them, else an empty list (rack
            # renders as all-MISSING cells, which is correct for racks the
            # snapshot doesn't mention).
            rack_slots[name] = rack_to_nodes.get(name, [])
        rack_rows.append(row)
    return MachineLayout(
        name="polaris",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )


def _alcf_rack_ratios(node_names: Iterable[str]) -> tuple[float, float]:
    """Return (aurora_ratio, polaris_ratio) for *node_names*."""
    names = [n for n in node_names if n]
    if not names:
        return 0.0, 0.0
    aurora = polaris = 0
    for name in names:
        if (m := _ALCF_PATTERN.match(name)) is None:
            continue
        rack = m.group(1)
        if _AURORA_RACK_PATTERN.match(rack):
            aurora += 1
        elif _POLARIS_RACK_PATTERN.match(rack):
            polaris += 1
    total = len(names)
    return aurora / total, polaris / total


def _with_overflow_racks(layout: MachineLayout, observed_names: List[str]) -> MachineLayout:
    """Return *layout* augmented with an overflow row for any nodes not already
    mapped.

    When a curated layout (aurora / polaris) wins the detection vote, nodes
    from the minority cluster are not present in ``layout.rack_slots``.  This
    helper groups them by their parsed rack name and appends them as an extra
    "unmapped" row at the bottom of the layout so they remain visible.

    Because :class:`MachineLayout` is ``frozen=True``, a new instance is built
    with the augmented data rather than mutating the existing one.
    """
    # Collect names that parsed OK but have no slot entry in the layout.
    overflow_rack_to_nodes: Dict[str, List[str]] = {}
    for name in observed_names:
        node = parse_node_id(name)
        if node is None:
            continue
        if name in layout.rack_slots.get(node.rack, []):
            continue
        if node.rack in layout.rack_slots:
            # The rack is known but this particular name is absent — already
            # represented by the curated layout (e.g. an empty curated rack).
            continue
        overflow_rack_to_nodes.setdefault(node.rack, []).append(name)

    if not overflow_rack_to_nodes:
        return layout  # no overflow — nothing to do

    # Use the same per-rack grid shape for all overflow racks so dimensions
    # are consistent.
    max_n = max(len(v) for v in overflow_rack_to_nodes.values())
    cols = max(1, math.ceil(math.sqrt(max_n)))
    rows = max(1, math.ceil(max_n / cols))

    new_specs = dict(layout.rack_specs)
    new_slots = dict(layout.rack_slots)
    overflow_row: List[str] = []
    for rack_name in sorted(overflow_rack_to_nodes):
        nodes = sorted(overflow_rack_to_nodes[rack_name],
                       key=lambda n: (parse_node_id(n) or NodeId(rack_name, "", n)).slot)
        new_specs[rack_name] = RackSpec(name=rack_name, rows=rows, cols=cols)
        new_slots[rack_name] = nodes
        overflow_row.append(rack_name)

    new_rows = list(layout.rack_rows) + [overflow_row]
    return MachineLayout(
        name=layout.name,
        rack_rows=new_rows,
        rack_specs=new_specs,
        rack_slots=new_slots,
    )


def detect_layout(node_names: Iterable[str]) -> MachineLayout:
    """Return the best-matching :class:`MachineLayout` for *node_names*.

    Any nodes whose rack is not captured by the winning curated layout are
    appended to an overflow row at the bottom so they remain visible rather
    than being silently dropped.
    """
    names = [n for n in node_names if n]
    aurora_ratio, polaris_ratio = _alcf_rack_ratios(names)
    if aurora_ratio >= 0.80:
        layout = _build_aurora_layout(names)
        return _with_overflow_racks(layout, names)
    if polaris_ratio >= 0.80:
        layout = _build_polaris_layout(names)
        return _with_overflow_racks(layout, names)
    return _build_generic_layout(names)


def _build_generic_layout(names: List[str]) -> MachineLayout:
    """Synthesize a roughly-square MachineLayout from observed node names."""
    rack_to_node_names: Dict[str, List[str]] = {}
    for name in names:
        node = parse_node_id(name)
        if node is None:
            continue
        rack_to_node_names.setdefault(node.rack, []).append(name)

    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    for rack, node_names in rack_to_node_names.items():
        # Preserve insertion order; do not re-sort (slot ordering is not
        # guaranteed meaningful for unknown machines).
        unique = list(dict.fromkeys(node_names))
        n = len(unique)
        cols = max(1, math.ceil(math.sqrt(n)))
        rows = max(1, math.ceil(n / cols))
        if rows > 16:
            rows = 16
            cols = max(cols, math.ceil(n / rows))
        rack_specs[rack] = RackSpec(name=rack, rows=rows, cols=cols)
        rack_slots[rack] = unique

    # Lay racks out in a single row by name — caller's HorizontalScroll handles width
    rack_rows = [sorted(rack_to_node_names.keys())] if rack_to_node_names else []
    return MachineLayout(
        name="generic",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )

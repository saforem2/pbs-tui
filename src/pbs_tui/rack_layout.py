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


# Aurora node name format: "x[rack4]-b[slot2]" e.g. "x4702-b07"
_AURORA_PATTERN = re.compile(r"^(x[34]\d{3})-(b\d{2})$")

# Polaris node name format: "x[rack4]c0s[slot2]b0n0" e.g. "x3005c0s7b0n0".
# The rack id is the leading "x[34]\d{3}" and the slot is the "s\d+" segment.
_POLARIS_PATTERN = re.compile(r"^(x3\d{3})c0s(\d+)b\d+n\d+$")

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

    if m := _AURORA_PATTERN.match(name):
        return NodeId(rack=m.group(1), slot=m.group(2), raw=name)

    if m := _POLARIS_PATTERN.match(name):
        # Normalise Polaris slot to two digits so chassis ordering is stable.
        return NodeId(rack=m.group(1), slot=f"s{int(m.group(2)):02d}", raw=name)

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
# Each rack row has 21 racks numbered xRR00..xRR20.
_AURORA_ROW_PREFIXES = ("x47", "x46", "x45", "x44", "x43", "x42", "x41", "x40")
_AURORA_RACK_COLS = 21  # racks numbered 00..20 within each row
_AURORA_RACK_SHAPE = (7, 2)  # rows x cols of blades per rack


def _build_aurora_layout() -> MachineLayout:
    rack_rows: List[List[str]] = []
    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    n_slots = _AURORA_RACK_SHAPE[0] * _AURORA_RACK_SHAPE[1]
    for prefix in _AURORA_ROW_PREFIXES:
        row: List[str] = []
        for col in range(_AURORA_RACK_COLS):
            name = f"{prefix}{col:02d}"
            row.append(name)
            rack_specs[name] = RackSpec(
                name=name, rows=_AURORA_RACK_SHAPE[0], cols=_AURORA_RACK_SHAPE[1]
            )
            # Aurora node names: "x4702-b07"
            rack_slots[name] = [f"{name}-b{i:02d}" for i in range(n_slots)]
        rack_rows.append(row)
    return MachineLayout(
        name="aurora",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )


_AURORA_LAYOUT = _build_aurora_layout()


# Polaris curated layout — three rack rows with descending suffix columns
# (rightmost rack is x3001 / x3101 / x3201, leftmost rack is x3016 / x3112 /
# x3212).  Rows 2 and 3 are right-aligned under row 1; the leading 4 slots
# of those rows are filled with empty placeholders ("") that the renderer
# skips for placement but reserves for spacing.
#
# Each Polaris rack holds 14 nodes laid out as 2 cols x 7 rows.
_POLARIS_RACK_SHAPE = (7, 2)
# Row 1 contains 16 racks numbered x3001..x3016, displayed right-to-left so
# x3016 sits leftmost and x3001 sits rightmost.  Rows 2 and 3 each contain
# 12 racks numbered x31RR / x32RR (01..12) right-aligned under row 1.
_POLARIS_ROWS = (
    ("x30", 1, 16),
    ("x31", 1, 12),
    ("x32", 1, 12),
)
_POLARIS_TOP_ROW_WIDTH = 16


def _build_polaris_layout() -> MachineLayout:
    rack_rows: List[List[str]] = []
    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    n_slots = _POLARIS_RACK_SHAPE[0] * _POLARIS_RACK_SHAPE[1]
    for prefix, lo, hi in _POLARIS_ROWS:
        # Right-align: pad the front with empty placeholders so the rightmost
        # rack lines up with column (_POLARIS_TOP_ROW_WIDTH - 1).
        leading_blanks = _POLARIS_TOP_ROW_WIDTH - (hi - lo + 1)
        row: List[str] = [""] * leading_blanks
        # Display order is right-to-left: highest rack number on the left,
        # lowest on the right.
        for n in range(hi, lo - 1, -1):
            name = f"{prefix}{n:02d}"
            row.append(name)
            rack_specs[name] = RackSpec(
                name=name, rows=_POLARIS_RACK_SHAPE[0], cols=_POLARIS_RACK_SHAPE[1]
            )
            # Polaris node names: "x3005c0s7b0n0"
            rack_slots[name] = [f"{name}c0s{i}b0n0" for i in range(n_slots)]
        rack_rows.append(row)
    return MachineLayout(
        name="polaris",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )


_POLARIS_LAYOUT = _build_polaris_layout()


def _aurora_match_ratio(node_names: Iterable[str]) -> float:
    names = [n for n in node_names if n]
    if not names:
        return 0.0
    matches = sum(1 for n in names if _AURORA_PATTERN.match(n))
    return matches / len(names)


def _polaris_match_ratio(node_names: Iterable[str]) -> float:
    names = [n for n in node_names if n]
    if not names:
        return 0.0
    matches = sum(1 for n in names if _POLARIS_PATTERN.match(n))
    return matches / len(names)


def detect_layout(node_names: Iterable[str]) -> MachineLayout:
    """Return the best-matching :class:`MachineLayout` for *node_names*."""
    names = [n for n in node_names if n]
    if _polaris_match_ratio(names) >= 0.80:
        return _POLARIS_LAYOUT
    if _aurora_match_ratio(names) >= 0.80:
        return _AURORA_LAYOUT
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
        rows = min(16, max(1, math.ceil(n / cols)))
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

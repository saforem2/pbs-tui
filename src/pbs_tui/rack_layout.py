"""Rack-name parsing, machine layouts, and node-id grouping.

This module is intentionally independent of Textual / Rich so it can be unit
tested in isolation.  It supplies three things to ``rack_grid``:

* ``NodeId`` — parsed (rack, slot) from a node name.
* ``RackSpec`` / ``MachineLayout`` — geometry of a known machine.
* ``detect_layout(node_names)`` — returns either a curated ``MachineLayout``
  (Aurora) or a generic fallback synthesised from the observed names.
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
    name: str
    rack_rows: List[List[str]]
    rack_specs: Dict[str, RackSpec]
    rack_slots: Dict[str, List[str]]

    def all_racks(self) -> List[str]:
        return [rack for row in self.rack_rows for rack in row]


# Aurora curated layout — rack rows listed top-to-bottom with descending row
# prefix (x47XX above x46XX, etc.) to mirror the ALCF status page.
# Each rack row has 21 racks numbered xRR00..xRR20.
_AURORA_ROW_PREFIXES = ("x47", "x46", "x45", "x44", "x43", "x42", "x41", "x40")
_AURORA_RACK_COLS = 21  # racks numbered 00..20 within each row
_AURORA_RACK_SHAPE = (7, 2)  # rows x cols of blades per rack
_AURORA_SLOTS = tuple(f"b{i:02d}" for i in range(_AURORA_RACK_SHAPE[0] * _AURORA_RACK_SHAPE[1]))


def _build_aurora_layout() -> MachineLayout:
    rack_rows: List[List[str]] = []
    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    for prefix in _AURORA_ROW_PREFIXES:
        row: List[str] = []
        for col in range(_AURORA_RACK_COLS):
            name = f"{prefix}{col:02d}"
            row.append(name)
            rack_specs[name] = RackSpec(
                name=name, rows=_AURORA_RACK_SHAPE[0], cols=_AURORA_RACK_SHAPE[1]
            )
            rack_slots[name] = list(_AURORA_SLOTS)
        rack_rows.append(row)
    return MachineLayout(
        name="aurora",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )


_AURORA_LAYOUT = _build_aurora_layout()


def _aurora_match_ratio(node_names: Iterable[str]) -> float:
    names = [n for n in node_names if n]
    if not names:
        return 0.0
    matches = sum(1 for n in names if _AURORA_PATTERN.match(n))
    return matches / len(names)


def detect_layout(node_names: Iterable[str]) -> MachineLayout:
    """Return the best-matching :class:`MachineLayout` for *node_names*."""
    names = [n for n in node_names if n]
    if _aurora_match_ratio(names) >= 0.80:
        return _AURORA_LAYOUT
    return _build_generic_layout(names)


def _build_generic_layout(names: List[str]) -> MachineLayout:
    """Synthesize a roughly-square MachineLayout from observed node names."""
    rack_to_slots: Dict[str, List[str]] = {}
    for name in names:
        node = parse_node_id(name)
        if node is None:
            continue
        rack_to_slots.setdefault(node.rack, []).append(node.slot)

    rack_specs: Dict[str, RackSpec] = {}
    rack_slots: Dict[str, List[str]] = {}
    for rack, slots in rack_to_slots.items():
        # Preserve insertion order; do not re-sort (slot ordering is not
        # guaranteed meaningful for unknown machines).
        unique_slots = list(dict.fromkeys(slots))
        n = len(unique_slots)
        cols = max(1, math.ceil(math.sqrt(n)))
        rows = min(16, max(1, math.ceil(n / cols)))
        rack_specs[rack] = RackSpec(name=rack, rows=rows, cols=cols)
        rack_slots[rack] = unique_slots

    # Lay racks out in a single row by name — caller's HorizontalScroll handles width
    rack_rows = [sorted(rack_to_slots.keys())] if rack_to_slots else []
    return MachineLayout(
        name="generic",
        rack_rows=rack_rows,
        rack_specs=rack_specs,
        rack_slots=rack_slots,
    )

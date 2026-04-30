"""Rack-name parsing, machine layouts, and node-id grouping.

This module is intentionally independent of Textual / Rich so it can be unit
tested in isolation.  It supplies three things to ``rack_grid``:

* ``NodeId`` — parsed (rack, slot) from a node name.
* ``RackSpec`` / ``MachineLayout`` — geometry of a known machine.
* ``detect_layout(node_names)`` — returns either a curated ``MachineLayout``
  (Aurora) or a generic fallback synthesised from the observed names.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


__all__ = [
    "NodeId",
    "parse_node_id",
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

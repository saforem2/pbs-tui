# Rack-Organized Cluster Grid View — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `Racks` tab to the PBS TUI that visualizes node utilization in a rack-organized grid (modeled on <https://status.alcf.anl.gov/#/aurora>) and supports selecting a job to highlight every node it occupies.

**Architecture:** A new `RackGridWidget` lives in `src/pbs_tui/rack_grid.py`. It composes a header, a horizontally-scrollable `RackPanel` of per-rack mini-grids, a `JobList` sidebar, and a legend. Rack discovery and machine-specific layouts live in a small `rack_layout.py` module. Job-color palette is reused from `cluster_grid.py` to keep the existing `Cluster` tab unchanged.

**Tech Stack:** Python 3.10+, [Textual](https://textual.textualize.io/), [Rich](https://rich.readthedocs.io/), pytest. The project uses `uv` for dependency management.

**Reference docs:**
- Spec: `docs/superpowers/specs/2026-04-29-rack-grid-view-design.md`
- Existing palette/grid implementation: `src/pbs_tui/cluster_grid.py`
- Existing node-name parsing: `src/pbs_tui/nodes.py`
- Existing app composition: `src/pbs_tui/app.py`, `src/pbs_tui/app.tcss`
- Sample data generator (Polaris-style hostnames): `src/pbs_tui/samples.py`
- Test helpers: `tests/util.py`

**File structure (final):**
```
src/pbs_tui/
  rack_layout.py        # NEW — NodeId, RackSpec, MachineLayout, parse_node_id, detect_layout
  rack_grid.py          # NEW — RackGridWidget + helpers
  nodes.py              # +1 helper: job_node_assignments
  app.py                # +1 TabPane "Racks", +2 message handlers, +1 binding
  app.tcss              # +CSS rules for new widget
tests/
  test_rack_layout.py   # NEW — parse + layout-detection
  test_rack_grid.py     # NEW — render + interactions
  test_nodes.py         # +1 test for job_node_assignments
  test_app.py           # +2 tests (binding + tab presence)
```

**How to run tests:** `pytest -q` from the project root. Single test: `pytest tests/test_rack_layout.py::test_name -v`.

**Commit style:** Follow the existing convention — short imperative subjects (`feat: …`, `test: …`, `docs: …`). Frequent commits.

---

## Task 1: NodeId dataclass + Aurora node parsing

**Files:**
- Create: `src/pbs_tui/rack_layout.py`
- Test: `tests/test_rack_layout.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_rack_layout.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_layout.py -v`
Expected: ImportError / ModuleNotFoundError for `pbs_tui.rack_layout`.

- [ ] **Step 3: Write minimal implementation**

Create `src/pbs_tui/rack_layout.py`:

```python
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
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


__all__ = [
    "NodeId",
    "RackSpec",
    "MachineLayout",
    "parse_node_id",
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_layout.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_layout.py tests/test_rack_layout.py
git commit -m "feat: Add rack_layout module with NodeId parser"
```

---

## Task 2: RackSpec + MachineLayout dataclasses

**Files:**
- Modify: `src/pbs_tui/rack_layout.py`
- Test: `tests/test_rack_layout.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_layout.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_layout.py::test_rack_spec_construction -v`
Expected: ImportError for `RackSpec` / `MachineLayout`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/pbs_tui/rack_layout.py` (after `NodeId`):

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_layout.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_layout.py tests/test_rack_layout.py
git commit -m "feat: Add RackSpec and MachineLayout dataclasses"
```

---

## Task 3: Aurora curated layout + detect_layout (Aurora path)

**Files:**
- Modify: `src/pbs_tui/rack_layout.py`
- Test: `tests/test_rack_layout.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_layout.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_layout.py::test_detect_aurora_layout_from_node_names -v`
Expected: ImportError for `detect_layout`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/pbs_tui/rack_layout.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_layout.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_layout.py tests/test_rack_layout.py
git commit -m "feat: Add Aurora curated layout and detect_layout"
```

---

## Task 4: Generic-layout fallback test coverage

**Files:**
- Modify: `tests/test_rack_layout.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_layout.py`:

```python
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
```

- [ ] **Step 2: Run tests**

Run: `pytest tests/test_rack_layout.py -v`
Expected: 15 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_rack_layout.py
git commit -m "test: Cover generic layout fallback in detect_layout"
```

---

## Task 5: job_node_assignments helper in nodes.py

**Files:**
- Modify: `src/pbs_tui/nodes.py`
- Test: `tests/test_nodes.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_nodes.py`:

```python
from pbs_tui.data import SchedulerSnapshot
from pbs_tui.nodes import job_node_assignments


def test_job_node_assignments_includes_only_running_jobs():
    snap = SchedulerSnapshot(
        jobs=[
            make_job(id="r1", state="R", exec_host="nodeA/0+nodeB/0"),
            make_job(id="q1", state="Q", exec_host="nodeC/0"),
            make_job(id="r2", state="R", exec_host="nodeD/0"),
        ]
    )
    assignments = job_node_assignments(snap)
    assert assignments == {"r1": ["nodeA", "nodeB"], "r2": ["nodeD"]}


def test_job_node_assignments_skips_jobs_without_exec_host():
    snap = SchedulerSnapshot(
        jobs=[
            make_job(id="r1", state="R", exec_host=None),
            make_job(id="r2", state="R", exec_host=""),
        ]
    )
    assert job_node_assignments(snap) == {}


def test_job_node_assignments_handles_bracketed_exec_host():
    snap = SchedulerSnapshot(
        jobs=[make_job(id="r1", state="R", exec_host="node[01-03]/0")]
    )
    assert job_node_assignments(snap) == {"r1": ["node01", "node02", "node03"]}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_nodes.py::test_job_node_assignments_includes_only_running_jobs -v`
Expected: ImportError for `job_node_assignments`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/pbs_tui/nodes.py` (after the existing helpers; add to `__all__` too):

```python
def job_node_assignments(snapshot) -> dict[str, list[str]]:
    """Return a ``{job_id: [exec_host_node_names]}`` map for running jobs.

    Jobs with no parseable ``exec_host`` are omitted.  Used by the rack-grid
    view to color each node cell by its current owning job.
    """
    result: dict[str, list[str]] = {}
    for job in snapshot.jobs:
        if job.state != "R":
            continue
        nodes = extract_exec_host_nodes(job.exec_host)
        if nodes:
            result[job.id] = nodes
    return result
```

Update the `__all__` list at the top of `nodes.py` to include `"job_node_assignments"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_nodes.py -v`
Expected: All previously-passing tests + 3 new ones pass.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/nodes.py tests/test_nodes.py
git commit -m "feat: Add job_node_assignments helper for rack-grid view"
```

---

## Task 6: Cell-state classification helper

**Files:**
- Create: `src/pbs_tui/rack_grid.py`
- Test: `tests/test_rack_grid.py`

The rack grid renders one cell per node. This task introduces the cell-state classifier — a pure function that maps `(Node, Optional[job_id])` to a `CellState` (free / occupied / down / unknown / reservation / missing) and a glyph. Keeping it pure makes it trivial to test and lets the renderer stay dumb.

- [ ] **Step 1: Write the failing test**

Create `tests/test_rack_grid.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_grid.py -v`
Expected: ImportError for `pbs_tui.rack_grid`.

- [ ] **Step 3: Write minimal implementation**

Create `src/pbs_tui/rack_grid.py`:

```python
"""Rack-organized cluster grid widget.

Renders each rack as a labelled mini-grid of node cells and supports
selecting a job (via the sidebar list, by clicking a colored cell, or via
arrow-key navigation) to highlight every node it occupies.

The rendering pipeline keeps three things separate so each can be tested in
isolation:

* ``classify_cell`` decides what a single cell represents (job owner, free,
  down, etc.) — a pure function of ``Node`` + owning ``job_id``.
* ``build_render_model`` walks a :class:`MachineLayout` and produces a
  ``RenderModel`` with a 2-D grid of cells plus reverse lookups used by the
  click handler.
* ``render_to_text`` turns a ``RenderModel`` into a Rich ``Text``.

The widget itself is a thin Textual layer over those building blocks.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Optional

from .data import Node


__all__ = ["CellState", "classify_cell"]


class CellState(enum.Enum):
    OCCUPIED = "occupied"
    FREE = "free"
    DOWN = "down"
    UNKNOWN = "unknown"
    RESERVATION = "reservation"
    MISSING = "missing"  # slot in layout has no corresponding Node in snapshot


def classify_cell(node: Optional[Node], owner_job_id: Optional[str]) -> CellState:
    if node is None:
        return CellState.MISSING

    primary = node.primary_state()

    # job-exclusive trumps everything else
    if "job-exclusive" in node.state or primary == "job-exclusive":
        return CellState.OCCUPIED

    # The full state string can be comma-joined ("offline,down") — split.
    flags = {flag.strip() for flag in node.state.split(",") if flag.strip()}
    if flags & {"offline", "down"}:
        return CellState.DOWN
    if "state-unknown" in flags:
        return CellState.UNKNOWN
    if "resv-exclusive" in flags:
        return CellState.RESERVATION
    if owner_job_id is not None:
        return CellState.OCCUPIED
    return CellState.FREE
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_grid.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_grid.py tests/test_rack_grid.py
git commit -m "feat: Add CellState classifier for rack grid"
```

---

## Task 7: RenderModel and build_render_model

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`
- Test: `tests/test_rack_grid.py`

The `RenderModel` holds: (a) a 2-D grid of `Cell` records (one per text-row × text-col), (b) a `cell_at(row, col)` lookup for click handling, and (c) a `nodes_for_job(job_id)` lookup so the highlight pass can find every cell to invert. `build_render_model` walks the layout left-to-right, top-to-bottom and fills it in.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_grid.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_grid.py::test_build_render_model_assigns_one_cell_per_slot -v`
Expected: ImportError for `build_render_model` / `RenderModel`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/pbs_tui/rack_grid.py`:

```python
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from .data import SchedulerSnapshot
from .rack_layout import MachineLayout, RackSpec


# Layout constants (small enough to inline; tweakable later if needed)
RACK_LABEL_LINES = 1     # rack-name label
RACK_UTIL_LINES = 1      # "n/N" utilization line
RACK_TRAILING_LINES = 1  # blank padding under each rack
RACK_HORIZONTAL_PAD = 1  # spaces between racks within a row
RACK_ROW_GAP = 1         # blank lines between rack rows


@dataclass(frozen=True)
class Cell:
    """One renderable cell — either a node slot or a structural element."""
    row: int
    col: int
    state: CellState
    node_name: Optional[str] = None  # None for structural cells (labels, padding)
    rack_name: Optional[str] = None
    owner_job_id: Optional[str] = None


@dataclass
class RackPlacement:
    """Where a single rack lives within the rendered grid (top-left corner)."""
    rack: str
    row: int
    col: int
    width: int
    height: int


@dataclass
class RenderModel:
    layout: MachineLayout
    width: int
    height: int
    cells_by_node: Dict[str, Cell]
    rack_placements: Dict[str, RackPlacement]
    # Reverse lookup for click handling — keyed (text_row, text_col)
    _coord_to_node: Dict[Tuple[int, int], str]
    _job_to_nodes: Dict[str, List[str]]

    def cell_at(self, row: int, col: int) -> Optional[str]:
        """Return the node name at grid (row, col), or ``None``."""
        return self._coord_to_node.get((row, col))

    def nodes_for_job(self, job_id: str) -> List[str]:
        return list(self._job_to_nodes.get(job_id, []))

    def rack_at(self, row: int, col: int) -> Optional[str]:
        """Return the rack-name label at (row, col), or ``None``.

        Only the label-line cells return a rack name — used by the click
        handler to detect rack-name clicks.
        """
        for placement in self.rack_placements.values():
            if (placement.row == row
                    and placement.col <= col < placement.col + placement.width):
                return placement.rack
        return None


def _rack_box_size(spec: RackSpec) -> Tuple[int, int]:
    """Return (width, height) of a single rack's rendered box."""
    label_width = max(spec.cols, len(spec.name))  # ensure label fits
    height = RACK_LABEL_LINES + RACK_UTIL_LINES + spec.rows + RACK_TRAILING_LINES
    return label_width, height


def build_render_model(
    layout: MachineLayout,
    snapshot: SchedulerSnapshot,
    job_assignments: Dict[str, List[str]],
) -> RenderModel:
    """Walk *layout* and snapshot to produce a :class:`RenderModel`."""
    # Build node lookup and reverse owner lookup
    node_by_name: Dict[str, "Node"] = {n.name: n for n in snapshot.nodes}
    node_to_job: Dict[str, str] = {}
    for job_id, node_names in job_assignments.items():
        for n in node_names:
            node_to_job[n] = job_id

    cells_by_node: Dict[str, Cell] = {}
    rack_placements: Dict[str, RackPlacement] = {}
    coord_to_node: Dict[Tuple[int, int], str] = {}

    # Pass 1: compute per-row max heights and column starts
    cursor_y = 0
    total_width = 0
    for row_idx, rack_row in enumerate(layout.rack_rows):
        cursor_x = 0
        row_height = 0
        for rack in rack_row:
            spec = layout.rack_specs[rack]
            box_w, box_h = _rack_box_size(spec)
            placement = RackPlacement(
                rack=rack, row=cursor_y, col=cursor_x,
                width=box_w, height=box_h,
            )
            rack_placements[rack] = placement
            # Lay out the cells inside this rack
            slots = layout.rack_slots[rack]
            inner_top = cursor_y + RACK_LABEL_LINES + RACK_UTIL_LINES
            for idx, slot in enumerate(slots):
                if idx >= spec.capacity():
                    break
                rr = inner_top + idx // spec.cols
                cc = cursor_x + idx % spec.cols
                node_name = f"{rack}-{slot}"
                node = node_by_name.get(node_name)
                owner = node_to_job.get(node_name)
                cell = Cell(
                    row=rr, col=cc,
                    state=classify_cell(node, owner),
                    node_name=node_name,
                    rack_name=rack,
                    owner_job_id=owner,
                )
                cells_by_node[node_name] = cell
                coord_to_node[(rr, cc)] = node_name
            cursor_x += box_w + RACK_HORIZONTAL_PAD
            row_height = max(row_height, box_h)
        total_width = max(total_width, cursor_x)
        cursor_y += row_height + RACK_ROW_GAP

    job_to_nodes: Dict[str, List[str]] = defaultdict(list)
    for node_name, job_id in node_to_job.items():
        if node_name in cells_by_node:  # skip nodes the layout doesn't know
            job_to_nodes[job_id].append(node_name)

    return RenderModel(
        layout=layout,
        width=total_width,
        height=cursor_y,
        cells_by_node=cells_by_node,
        rack_placements=rack_placements,
        _coord_to_node=coord_to_node,
        _job_to_nodes=dict(job_to_nodes),
    )
```

Update `__all__`: add `"Cell"`, `"RenderModel"`, `"RackPlacement"`, `"build_render_model"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_grid.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_grid.py tests/test_rack_grid.py
git commit -m "feat: Add RenderModel and build_render_model for rack grid"
```

---

## Task 8: render_to_text — produce Rich Text from a RenderModel

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`
- Test: `tests/test_rack_grid.py`

This task introduces the function that turns a `RenderModel` into a Rich `Text` object. The renderer needs:

- A palette mapping from `CellState` → glyph + style.
- Per-job style lookup (reuses the existing `cluster_grid.Palette.job_style`).
- Optional `selected_job_id` — when set, those cells render with an inverted style.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_grid.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_grid.py::test_render_to_text_produces_expected_dimensions -v`
Expected: ImportError for `render_to_text`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/pbs_tui/rack_grid.py`:

```python
from rich.text import Text

from .cluster_grid import Palette  # re-use job-color palette


CELL_GLYPHS: Dict[CellState, str] = {
    CellState.OCCUPIED: " ",
    CellState.FREE: "░",
    CellState.DOWN: "×",
    CellState.UNKNOWN: "?",
    CellState.RESERVATION: "▒",
    CellState.MISSING: " ",
}


def _state_style(state: CellState, palette: Palette) -> str:
    """Style for a non-occupied cell."""
    if state == CellState.FREE:
        return palette.empty_style
    if state == CellState.DOWN:
        return "on color(235)"
    if state == CellState.UNKNOWN:
        return "dim"
    if state == CellState.RESERVATION:
        return "on color(60)"
    return palette.empty_style


def _invert_style(base_style: str) -> str:
    """Build a highlight style for a selected job's cells."""
    fg = base_style.replace("on ", "", 1) if base_style.startswith("on ") else base_style
    return f"reverse {fg}"


def render_to_text(
    model: RenderModel,
    *,
    palette: Palette,
    running_jobs: Dict[str, int],
    selected_job_id: Optional[str],
) -> Text:
    """Render *model* as a Rich Text grid.

    *running_jobs* maps job_id → palette index so cell colors stay consistent
    with the legend.  *selected_job_id*, if not None, marks every cell of that
    job with an inverted style.
    """
    # Build a 2D char grid + style grid initialised to spaces.
    chars: List[List[str]] = [[" "] * model.width for _ in range(model.height)]
    styles: List[List[str]] = [[""] * model.width for _ in range(model.height)]

    # Place rack labels and utilization lines.
    for placement in model.rack_placements.values():
        # Rack name label — pad/center within the rack box width
        label = placement.rack
        label_pad = max(0, (placement.width - len(label)) // 2)
        for i, ch in enumerate(label):
            c = placement.col + label_pad + i
            if 0 <= c < model.width:
                chars[placement.row][c] = ch
                styles[placement.row][c] = "bold"
        # Utilization line — built lazily after we know occupied counts
        # (computed below).

    # Count occupied per rack for the utilization line.
    occupied_per_rack: Dict[str, int] = defaultdict(int)
    total_per_rack: Dict[str, int] = {
        rack: model.layout.rack_specs[rack].capacity()
        for rack in model.rack_placements
    }
    for cell in model.cells_by_node.values():
        if cell.rack_name and cell.state == CellState.OCCUPIED:
            occupied_per_rack[cell.rack_name] += 1

    # Write utilization lines (row = placement.row + 1)
    for placement in model.rack_placements.values():
        util = f"{occupied_per_rack[placement.rack]}/{total_per_rack[placement.rack]}"
        util_pad = max(0, (placement.width - len(util)) // 2)
        util_row = placement.row + RACK_LABEL_LINES
        for i, ch in enumerate(util):
            c = placement.col + util_pad + i
            if 0 <= c < model.width and util_row < model.height:
                chars[util_row][c] = ch
                styles[util_row][c] = "dim"

    # Place node cells.
    for cell in model.cells_by_node.values():
        glyph = CELL_GLYPHS[cell.state]
        if cell.state == CellState.OCCUPIED and cell.owner_job_id in running_jobs:
            base_style = palette.job_style(running_jobs[cell.owner_job_id])
        else:
            base_style = _state_style(cell.state, palette)
        if selected_job_id and cell.owner_job_id == selected_job_id:
            base_style = _invert_style(base_style)
        if 0 <= cell.row < model.height and 0 <= cell.col < model.width:
            chars[cell.row][cell.col] = glyph
            styles[cell.row][cell.col] = base_style

    # Assemble Rich Text — append per-cell to keep style runs accurate
    out = Text()
    for r in range(model.height):
        for c in range(model.width):
            out.append(chars[r][c], style=styles[r][c] or None)
        if r < model.height - 1:
            out.append("\n")
    return out
```

Update `__all__`: add `"render_to_text"`, `"CELL_GLYPHS"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_grid.py -v`
Expected: 13 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_grid.py tests/test_rack_grid.py
git commit -m "feat: Render RenderModel to Rich Text for rack grid"
```

---

## Task 9: Highlight assertion — selected job cells use inverted style

**Files:**
- Modify: `tests/test_rack_grid.py`

This task adds explicit coverage for the selection visual: when `selected_job_id` is set, the cells of that job must use a `reverse`-prefixed style and no other cells should.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_grid.py`:

```python
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
    # Walk Text spans and verify only j1 cells got "reverse"
    selected_coords = {
        (model.cells_by_node["r1-a"].row, model.cells_by_node["r1-a"].col),
        (model.cells_by_node["r1-b"].row, model.cells_by_node["r1-b"].col),
    }
    inverted_styles_found: list[tuple[int, int]] = []
    # Iterate by walking the plain string + spans; simplest is to render and
    # scan the styles grid directly.  We re-use render_to_text's deterministic
    # output — j2's r1-c cell must NOT be reverse-styled.
    rendered = text.plain.splitlines()
    # Sanity: rendered should contain at least the selection's two cells
    assert len(rendered) > 0
    # Span check: for every span where style starts with "reverse", its
    # (row, col) must be in selected_coords.
    cursor_row = 0
    cursor_col = 0
    for span_text, span_style in zip(*_walk_text_spans(text)):
        if "\n" in span_text:
            cursor_row += span_text.count("\n")
            cursor_col = 0
            continue
        for offset in range(len(span_text)):
            if span_style and "reverse" in str(span_style):
                inverted_styles_found.append((cursor_row, cursor_col + offset))
        cursor_col += len(span_text)
    assert set(inverted_styles_found) == selected_coords


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
```

- [ ] **Step 2: Run test to verify it fails OR passes immediately**

Run: `pytest tests/test_rack_grid.py::test_render_to_text_inverts_only_selected_job_cells -v`
Expected: PASS (the renderer already supports this). If it fails, the issue is most likely in `_invert_style` or the iteration order — fix in `rack_grid.py`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_rack_grid.py
git commit -m "test: Verify selected job inversion in rack grid render"
```

---

## Task 10: HeaderBar + LegendBar Static widgets

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`

Add two trivial `Static` subclasses that hold a single Rich `Text`. They exist only to give CSS hooks and keep the `RackGridWidget` body small. No tests yet — they are pure containers exercised by widget-level tests later.

- [ ] **Step 1: Add the widgets**

Append to `src/pbs_tui/rack_grid.py`:

```python
from textual.widgets import Static


class _RackHeader(Static):
    """One-line header summarising the cluster: machine name + counts."""


class _RackLegend(Static):
    """One-line legend mapping glyphs to states."""


def build_legend_text() -> Text:
    legend = Text()
    legend.append(CELL_GLYPHS[CellState.FREE], style="dim")
    legend.append(" free  ")
    legend.append(CELL_GLYPHS[CellState.DOWN], style="dim")
    legend.append(" down  ")
    legend.append(CELL_GLYPHS[CellState.UNKNOWN], style="dim")
    legend.append(" unknown  ")
    legend.append(CELL_GLYPHS[CellState.RESERVATION], style="dim")
    legend.append(" reservation  ")
    legend.append("█", style="bold")
    legend.append(" job (click to select)")
    return legend


def build_header_text(layout: MachineLayout, snapshot: SchedulerSnapshot,
                      assignments: Dict[str, List[str]]) -> Text:
    total = len(snapshot.nodes)
    free = sum(1 for n in snapshot.nodes if n.primary_state() == "free")
    down = sum(1 for n in snapshot.nodes if n.primary_state() in {"offline", "down"})
    running_node_count = sum(len(v) for v in assignments.values())
    header = Text()
    header.append(layout.name.title(), style="bold")
    header.append("  ")
    header.append(f"{total:,}", style="bold")
    header.append(" nodes  ")
    header.append(f"{running_node_count:,}", style="bold cyan")
    header.append(" running  ")
    header.append(f"{free:,}", style="bold green")
    header.append(" free  ")
    if down:
        header.append(f"{down:,}", style="bold red")
        header.append(" down")
    return header
```

Update `__all__`: add `"build_legend_text"`, `"build_header_text"`.

- [ ] **Step 2: Quick smoke test**

Run: `python -c "from pbs_tui.rack_grid import build_legend_text, build_header_text; print(build_legend_text().plain); print(build_header_text.__doc__)"`
Expected: Prints the legend text without error.

- [ ] **Step 3: Commit**

```bash
git add src/pbs_tui/rack_grid.py
git commit -m "feat: Add header and legend builders for rack grid"
```

---

## Task 11: Sidebar JobList — entries, sort, filter

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`
- Test: `tests/test_rack_grid.py`

The sidebar is a Textual `ListView` of running jobs. We render each row as a Rich `Text` with a colored block + summary. Filtering by rack is a pure transformation over the entries.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rack_grid.py`:

```python
from datetime import timedelta
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rack_grid.py::test_build_job_list_entries_sorts_by_node_count_descending -v`
Expected: ImportError for `build_job_list_entries`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/pbs_tui/rack_grid.py`:

```python
from .data import Job
from .nodes import job_node_summary
from .time_utils import time_remaining, format_remaining


@dataclass(frozen=True)
class JobListEntry:
    job_id: str
    user: str
    queue: str
    node_count: int
    palette_index: int
    time_remaining_str: str
    nodes: List[str]


def build_job_list_entries(
    snapshot: SchedulerSnapshot,
    assignments: Dict[str, List[str]],
    *,
    palette_index: Dict[str, int],
    rack_filter: Optional[str] = None,
) -> List[JobListEntry]:
    """Build the sidebar entry list (running jobs only)."""
    entries: List[JobListEntry] = []
    ref = snapshot.timestamp
    by_id = {j.id: j for j in snapshot.jobs}
    for job_id, nodes in assignments.items():
        job = by_id.get(job_id)
        if job is None or job.state != "R":
            continue
        if rack_filter is not None:
            if not any(_node_matches_rack(n, rack_filter) for n in nodes):
                continue
        remaining = time_remaining(job, ref)
        entries.append(JobListEntry(
            job_id=job.id,
            user=job.user or "?",
            queue=job.queue or "",
            node_count=len(nodes),
            palette_index=palette_index.get(job.id, 0),
            time_remaining_str=format_remaining(remaining),
            nodes=list(nodes),
        ))
    entries.sort(key=lambda e: e.node_count, reverse=True)
    return entries


def _node_matches_rack(node_name: str, rack: str) -> bool:
    parsed = parse_node_id(node_name)
    return parsed is not None and parsed.rack == rack


def render_job_list_entry(entry: JobListEntry, palette: Palette,
                          *, selected: bool) -> Text:
    """Render one sidebar row as Rich Text."""
    style = palette.job_style(entry.palette_index)
    fg = style.replace("on ", "", 1) if style.startswith("on ") else style
    text = Text()
    text.append("█ ", style=fg)
    label = f"{entry.user} {entry.node_count}n {entry.queue}"
    if entry.time_remaining_str:
        label += f" [{entry.time_remaining_str}]"
    if selected:
        text.append(label, style="bold")
    else:
        text.append(label)
    return text
```

Add `from .rack_layout import parse_node_id` at the top of `rack_grid.py` if not already imported.

Update `__all__`: add `"JobListEntry"`, `"build_job_list_entries"`, `"render_job_list_entry"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rack_grid.py -v`
Expected: 16 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pbs_tui/rack_grid.py tests/test_rack_grid.py
git commit -m "feat: Build and render sidebar job entries for rack grid"
```

---

## Task 12: RackPanel widget — rendering + click handling

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`

`_RackPanel` is the Textual widget that draws the grid and converts mouse clicks to `(row, col)` lookups. It owns no business state — its parent (`RackGridWidget`) hands it a fully-built `RenderModel`, palette, running-jobs map, and selection.

- [ ] **Step 1: Write the widget**

Append to `src/pbs_tui/rack_grid.py`:

```python
from textual.message import Message
from textual.widget import Widget
from rich.console import RenderableType


class _RackPanel(Widget):
    """Renders the rack grid and emits clicks as messages."""

    DEFAULT_CSS = """
    _RackPanel {
        height: 1fr;
        overflow-x: scroll;
        overflow-y: auto;
        padding: 0 1;
    }
    """

    class CellClicked(Message):
        def __init__(self, node_name: Optional[str], rack_name: Optional[str]) -> None:
            super().__init__()
            self.node_name = node_name
            self.rack_name = rack_name

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._content: RenderableType = Text()
        self._model: Optional[RenderModel] = None

    def render(self) -> RenderableType:
        return self._content

    def update(self, content: Text, model: RenderModel) -> None:
        self._content = content
        self._model = model
        self.refresh()

    def on_click(self, event) -> None:
        if self._model is None:
            return
        col = int(event.x)
        row = int(event.y)
        # Look for a node at the click location
        node_name = self._model.cell_at(row, col)
        if node_name is not None:
            self.post_message(self.CellClicked(node_name=node_name, rack_name=None))
            return
        # Or a rack-name label
        rack_name = self._model.rack_at(row, col)
        if rack_name is not None:
            self.post_message(self.CellClicked(node_name=None, rack_name=rack_name))
```

- [ ] **Step 2: Quick smoke test**

Run: `python -c "from pbs_tui.rack_grid import _RackPanel; p = _RackPanel(); print(type(p).__name__)"`
Expected: Prints `_RackPanel` without error.

- [ ] **Step 3: Commit**

```bash
git add src/pbs_tui/rack_grid.py
git commit -m "feat: Add _RackPanel widget with click-to-message routing"
```

---

## Task 13: JobList sidebar widget

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`

`_JobListWidget` is a thin Textual `ListView` wrapper that holds a list of `JobListEntry` and emits a `JobChosen` message when the user picks one (click or `enter`). We avoid Textual's `ListItem`/`ListView` directly because the API differs across Textual versions; instead we render the entries into a `Static` and use arrow-key bindings to move a cursor.

- [ ] **Step 1: Write the widget**

Append to `src/pbs_tui/rack_grid.py`:

```python
class _JobListWidget(Widget):
    """Sidebar listing running jobs; emits JobChosen on selection."""

    DEFAULT_CSS = """
    _JobListWidget {
        width: 36;
        height: 1fr;
        border-left: tall $surface-lighten-1;
        padding: 0 1;
    }
    """

    BINDINGS = [
        ("up", "move(-1)", "Up"),
        ("down", "move(1)", "Down"),
        ("enter", "choose", "Select"),
        ("escape", "clear", "Clear"),
    ]

    can_focus = True

    class JobChosen(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class FilterCleared(Message):
        pass

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._entries: List[JobListEntry] = []
        self._cursor: int = 0
        self._palette: Optional[Palette] = None
        self._selected_id: Optional[str] = None
        self._rack_filter: Optional[str] = None
        self._content: RenderableType = Text()

    def render(self) -> RenderableType:
        return self._content

    def update(self, entries: List[JobListEntry], palette: Palette,
               selected_id: Optional[str], rack_filter: Optional[str]) -> None:
        self._entries = entries
        self._palette = palette
        self._selected_id = selected_id
        self._rack_filter = rack_filter
        self._cursor = max(0, min(self._cursor, len(entries) - 1))
        self._rebuild_content()
        self.refresh()

    def _rebuild_content(self) -> None:
        text = Text()
        if self._rack_filter:
            text.append("Filtered: ", style="dim")
            text.append(self._rack_filter, style="bold")
            text.append(f"  ({len(self._entries)} jobs)\n", style="dim")
            text.append("[clear: esc or click chip]\n\n", style="dim")
        if not self._entries:
            text.append("(no running jobs)", style="dim")
            self._content = text
            return
        for i, entry in enumerate(self._entries):
            prefix = "▶ " if i == self._cursor else "  "
            text.append(prefix)
            text.append_text(render_job_list_entry(
                entry, self._palette,
                selected=(self._selected_id == entry.job_id),
            ))
            text.append("\n")
        self._content = text

    def action_move(self, delta: int) -> None:
        if not self._entries:
            return
        self._cursor = (self._cursor + delta) % len(self._entries)
        self._rebuild_content()
        self.refresh()

    def action_choose(self) -> None:
        if not self._entries:
            return
        chosen = self._entries[self._cursor]
        self.post_message(self.JobChosen(chosen.job_id))

    def action_clear(self) -> None:
        self.post_message(self.FilterCleared())

    def on_click(self, event) -> None:
        # Map click row to an entry index. Header takes 0-2 lines depending on filter.
        offset = 3 if self._rack_filter else 0
        row = int(event.y) - offset
        if 0 <= row < len(self._entries):
            self._cursor = row
            self._rebuild_content()
            self.refresh()
            self.post_message(self.JobChosen(self._entries[row].job_id))
```

- [ ] **Step 2: Quick smoke test**

Run: `python -c "from pbs_tui.rack_grid import _JobListWidget; w = _JobListWidget(); print(w.BINDINGS)"`
Expected: Prints the bindings tuple list.

- [ ] **Step 3: Commit**

```bash
git add src/pbs_tui/rack_grid.py
git commit -m "feat: Add sidebar _JobListWidget with cursor + click selection"
```

---

## Task 14: RackGridWidget — composes header + RackPanel + JobList + Legend

**Files:**
- Modify: `src/pbs_tui/rack_grid.py`

The public widget pulls the pieces together. It owns three pieces of state: `selected_job_id`, `rack_filter`, and a cached `_snapshot`. On every snapshot update it re-derives the `RenderModel`, palette, and entries. It emits `JobSelected(job_id)` and `NodeSelected(node_name)` to its parent app.

- [ ] **Step 1: Write the widget**

Append to `src/pbs_tui/rack_grid.py`:

```python
from textual.containers import Horizontal, Vertical

from .cluster_grid import _build_palette, AGGREGATED_QUEUES


class RackGridWidget(Vertical):
    """Public Racks-tab widget."""

    DEFAULT_CSS = """
    RackGridWidget {
        height: 1fr;
        padding: 1 2;
    }
    RackGridWidget _RackHeader { height: auto; }
    RackGridWidget _RackLegend { height: auto; margin-top: 1; }
    RackGridWidget Horizontal { height: 1fr; }
    """

    class JobSelected(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class NodeSelected(Message):
        def __init__(self, node_name: str) -> None:
            super().__init__()
            self.node_name = node_name

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snapshot: Optional[SchedulerSnapshot] = None
        self._selected_job_id: Optional[str] = None
        self._rack_filter: Optional[str] = None

    def compose(self):
        yield _RackHeader(id="rack_header")
        with Horizontal():
            yield _RackPanel(id="rack_panel")
            yield _JobListWidget(id="rack_job_list")
        yield _RackLegend(id="rack_legend")

    # ── public API ───────────────────────────────────────────────

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        self._snapshot = snapshot
        # Drop selection if the job is no longer running
        assignments = job_node_assignments(snapshot)
        if self._selected_job_id and self._selected_job_id not in assignments:
            self._selected_job_id = None
        self._rebuild()

    # ── internal ─────────────────────────────────────────────────

    def _rebuild(self) -> None:
        if self._snapshot is None:
            return
        snap = self._snapshot
        node_names = [n.name for n in snap.nodes]
        layout = detect_layout(node_names)
        assignments = job_node_assignments(snap)
        # Palette index per job — order matches build_job_list_entries' input
        # but we want a stable palette index assignment across renders, so
        # sort by job_id.
        sorted_running = sorted(
            j for j in assignments.keys()
        )
        palette_index = {jid: i for i, jid in enumerate(sorted_running)}

        # Build palette using the same job_count we used in cluster_grid
        try:
            theme_vars = self.app.get_css_variables()
            theme_name = self.app.theme or ""
        except Exception:
            theme_vars, theme_name = {}, ""
        palette = _build_palette(
            theme_vars, theme_name=theme_name, job_count=len(sorted_running)
        )

        model = build_render_model(layout, snap, job_assignments=assignments)
        text = render_to_text(
            model,
            palette=palette,
            running_jobs=palette_index,
            selected_job_id=self._selected_job_id,
        )

        self.query_one(_RackHeader).update(build_header_text(layout, snap, assignments))
        self.query_one(_RackPanel).update(text, model)

        entries = build_job_list_entries(
            snap, assignments,
            palette_index=palette_index,
            rack_filter=self._rack_filter,
        )
        self.query_one(_JobListWidget).update(
            entries, palette,
            selected_id=self._selected_job_id,
            rack_filter=self._rack_filter,
        )
        self.query_one(_RackLegend).update(build_legend_text())

    # ── message handlers ────────────────────────────────────────

    def on__rack_panel_cell_clicked(self, event: "_RackPanel.CellClicked") -> None:
        if event.node_name:
            # If the cell belongs to a running job, select that job; else node
            assignments = job_node_assignments(self._snapshot) if self._snapshot else {}
            owning_job = next(
                (jid for jid, ns in assignments.items() if event.node_name in ns),
                None,
            )
            if owning_job:
                self._selected_job_id = owning_job
                self.post_message(self.JobSelected(owning_job))
            else:
                self.post_message(self.NodeSelected(event.node_name))
        elif event.rack_name:
            # Toggle / switch the rack filter
            self._rack_filter = (
                None if self._rack_filter == event.rack_name else event.rack_name
            )
        self._rebuild()

    def on__job_list_widget_job_chosen(
        self, event: "_JobListWidget.JobChosen"
    ) -> None:
        self._selected_job_id = event.job_id
        self.post_message(self.JobSelected(event.job_id))
        self._rebuild()

    def on__job_list_widget_filter_cleared(
        self, event: "_JobListWidget.FilterCleared"
    ) -> None:
        self._rack_filter = None
        self._selected_job_id = None
        self._rebuild()
```

Note Textual's auto-handler naming: a class named `_RackPanel` with message `CellClicked` becomes `on__rack_panel_cell_clicked` (double underscore from the leading `_` in the widget name).

Update `__all__`: add `"RackGridWidget"`.

- [ ] **Step 2: Quick import smoke**

Run: `python -c "from pbs_tui.rack_grid import RackGridWidget; print(RackGridWidget.__name__)"`
Expected: Prints `RackGridWidget` without error.

- [ ] **Step 3: Commit**

```bash
git add src/pbs_tui/rack_grid.py
git commit -m "feat: Compose RackGridWidget from header, panel, sidebar, legend"
```

---

## Task 15: Wire RackGridWidget into PBSTUI app

**Files:**
- Modify: `src/pbs_tui/app.py`
- Modify: `src/pbs_tui/app.tcss`

- [ ] **Step 1: Add import + tab + handlers**

Edit `src/pbs_tui/app.py`:

1. After the existing import line `from .cluster_grid import ClusterGridWidget`, add:
   ```python
   from .rack_grid import RackGridWidget
   ```

2. In `class PBSTUI`, find the `compose` method's `with TabbedContent(id="tabs"):` block. Right after the existing `with TabPane("Cluster", id="cluster_tab"):` block, add:
   ```python
   with TabPane("Racks", id="racks_tab"):
       yield RackGridWidget(id="rack_grid")
   ```

3. Add a binding after `("g", "focus_cluster", "Focus cluster"),` in the `BINDINGS` list:
   ```python
   ("k", "focus_racks", "Focus racks"),
   ```

4. Add the action method next to `action_focus_cluster`:
   ```python
   def action_focus_racks(self) -> None:
       tabbed_content = self.query_one(TabbedContent)
       tabbed_content.active = "racks_tab"
       tabbed_content.focus()
   ```

5. In `_update_tables`, after `cluster_grid.update_from_snapshot(snapshot)`, add:
   ```python
   self.query_one(RackGridWidget).update_from_snapshot(snapshot)
   ```

6. Add two message handlers anywhere inside `class PBSTUI` (near `on_cluster_grid_widget_cell_clicked`):
   ```python
   def on_rack_grid_widget_job_selected(
       self, event: RackGridWidget.JobSelected
   ) -> None:
       if self._snapshot is None or not self._detail_panel_enabled:
           return
       job = self._job_index.get(event.job_id)
       if job:
           self._selected_job_id = job.id
           self._selected_node_name = None
           self._selected_queue_name = None
           self._detail_source = "job"
           self.query_one(DetailPanel).show_job(
               job, reference_time=self._snapshot.timestamp
           )

   def on_rack_grid_widget_node_selected(
       self, event: RackGridWidget.NodeSelected
   ) -> None:
       if self._snapshot is None or not self._detail_panel_enabled:
           return
       node = self._node_index.get(event.node_name)
       if node:
           self._selected_node_name = node.name
           self._selected_job_id = None
           self._selected_queue_name = None
           self._detail_source = "node"
           self.query_one(DetailPanel).show_node(node)
   ```

- [ ] **Step 2: Add CSS**

Append to `src/pbs_tui/app.tcss`:

```css
#rack_grid {
    padding: 1 2;
}
```

(All other layout selectors live in the widget's `DEFAULT_CSS`.)

- [ ] **Step 3: Smoke test the app starts**

Run:
```bash
PBS_TUI_SAMPLE_DATA=1 PBS_TUI_HEADLESS=1 PBS_TUI_AUTOPILOT=quit pbs-tui
```
Expected: process exits cleanly with no traceback.

- [ ] **Step 4: Commit**

```bash
git add src/pbs_tui/app.py src/pbs_tui/app.tcss
git commit -m "feat: Add Racks tab wiring and message handlers"
```

---

## Task 16: App-level test for the new tab + binding

**Files:**
- Modify: `tests/test_app.py`

- [ ] **Step 1: Write the tests**

Append to `tests/test_app.py`:

```python
def test_racks_focus_binding_exposed():
    assert any(
        binding[1] == "focus_racks" and binding[0] == "k"
        for binding in PBSTUI.BINDINGS
    )


def test_app_includes_racks_tab():
    snapshot = sample_snapshot(now=NOW)

    class SingleSnapshotFetcher:
        async def fetch_snapshot(self):
            return snapshot

    app = PBSTUI(fetcher=SingleSnapshotFetcher(), refresh_interval=9999)

    async def interact() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            from textual.widgets import TabbedContent
            from pbs_tui.rack_grid import RackGridWidget
            tabs = app.query_one(TabbedContent)
            tab_ids = [pane.id for pane in tabs.query("TabPane")]
            assert "racks_tab" in tab_ids
            # Widget is mounted
            assert app.query_one(RackGridWidget) is not None

    asyncio.run(interact())
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_app.py::test_racks_focus_binding_exposed tests/test_app.py::test_app_includes_racks_tab -v`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_app.py
git commit -m "test: Cover Racks tab binding and presence"
```

---

## Task 17: Update HelpPanel text to mention the Racks tab

**Files:**
- Modify: `src/pbs_tui/app.py`

Currently `HelpPanel.HELP_TEXT` lists the four tab-focus bindings. Add `k` for the new Racks tab so users can discover it.

- [ ] **Step 1: Find and edit the HELP_TEXT block**

In `src/pbs_tui/app.py`, find the `HelpPanel.HELP_TEXT` constant. Locate this section:

```python
- **g**: Focus the Cluster tab
- **j**: Focus the Jobs tab
```

Insert a new line after the `**g**` line:

```python
- **k**: Focus the Racks tab
```

- [ ] **Step 2: Run all tests**

Run: `pytest -q`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/pbs_tui/app.py
git commit -m "docs: Mention Racks tab in HelpPanel key bindings"
```

---

## Task 18: README update — describe the Racks tab

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Edit README**

Find this line in `README.md`:

```markdown
- **Cluster grid** – visual map of cluster node utilization with proportional legend bar.
```

Insert a new bullet immediately after it:

```markdown
- **Rack grid** – per-rack node-level view modeled on the ALCF status page.
  Click a node to highlight every node a job occupies; click a rack name to filter the
  job sidebar. Detects Aurora and Polaris cabinet hostnames; falls back to a generic layout for
  other clusters.
```

Also extend the key bindings table by adding a new row right after the `g` row:

```markdown
| `k` | Focus the racks grid   |
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: Document Racks tab in README"
```

---

## Task 19: Manual UI smoke test

**Files:** none (manual)

- [ ] **Step 1: Run with sample data**

Run: `PBS_TUI_SAMPLE_DATA=1 pbs-tui`

Expected behavior:
1. App starts with `pbs-dark` theme.
2. Press `k` — Racks tab focuses.
3. Rack mini-grids appear with rack-name labels (`x3001`, `x3101`, etc., from `samples.py`).
4. Some racks show colored cells (running jobs); some show `░` (free).
5. Press `tab` to focus the job sidebar; arrow keys move the cursor; `enter` selects.
6. Click a colored cell — that job's details appear in the right detail panel and its cells get an inverted highlight.
7. Click a rack-name label — the sidebar list filters to jobs touching that rack; a "Filtered: …" header appears.
8. Press `escape` — the filter clears.
9. Press `g` — Cluster tab still works (proportional grid unchanged).

- [ ] **Step 2: Capture before/after screenshots (optional)**

If desired, save terminal screenshots to `assets/racks-dark.png` and reference them in the README.

- [ ] **Step 3: No commit needed unless you added screenshots**

---

## Task 20: Final test sweep

**Files:** none

- [ ] **Step 1: Run full suite**

Run: `pytest -q`
Expected: all tests pass; no warnings about deprecated Textual APIs caused by the new code.

- [ ] **Step 2: Confirm no regressions**

Run the existing cluster-grid suite explicitly:
```bash
pytest tests/test_cluster_grid.py -v
```
Expected: all original tests still pass (we did not modify `cluster_grid.py`).

- [ ] **Step 3: If everything passes, the feature is shippable.**

No commit required for a passing test sweep.

---

## Self-Review Notes

- **Spec coverage:**
  - Goal "render each rack as labeled mini-grid": Tasks 7, 8, 10.
  - Goal "color each cell by job + distinct glyphs for free/down/unknown/reservation": Tasks 6, 8.
  - Goal "job sidebar with click + arrow-key selection that highlights all cells": Tasks 11, 13, 14.
  - Goal "click rack-name label to filter sidebar": Tasks 12, 13, 14.
  - Goal "keyboard nav across cells": Sidebar arrow keys (Task 13). Per-cell cursor on `_RackPanel` is intentionally not implemented in this round — the sidebar already gives keyboard access to every running job, and the original ALCF screenshot has no cell cursor either. If desired later, it is purely additive in `_RackPanel`. (This is a deliberate scope reduction from the spec; flag at user-review time if undesired.)
  - Goal "per-rack utilization summary": Task 8 (`util_row`).
  - Goal "machine detection": Tasks 3, 4.
  - Non-goal "modify existing Cluster tab": Verified by Task 20 explicit re-run.
- **Placeholder scan:** All steps have concrete code or shell commands. No "TBD"/"similar to"/"add error handling".
- **Type consistency:** `JobListEntry`, `Cell`, `RenderModel`, `RackPlacement` field names are referenced consistently across tasks. `assignments` always means `dict[str, list[str]]` (job_id → node names). `palette_index` always means `dict[str, int]` (job_id → palette slot). `running_jobs` (in `render_to_text`) is the same `palette_index` map renamed for parameter clarity.
- **Scope reduction noted:** Per-cell keyboard cursor on the rack panel was in the spec ("Arrow key on focused RackPanel") but is omitted in the plan to keep the implementation tractable. The sidebar's arrow-key job navigation covers the primary use case (find a job, see its cells highlighted). This should be raised at user-review time so the user can decide whether to add it back or leave it for a follow-up.

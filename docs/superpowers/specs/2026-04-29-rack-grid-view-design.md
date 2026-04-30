# Rack-organized cluster grid view

**Status:** Approved (2026-04-29)
**Author:** Sam Foreman (with Claude)
**Scope:** New `Racks` tab visualizing per-node, per-rack utilization with job-selection highlighting.

## Motivation

The existing `Cluster` tab renders a single proportional bar grid (`cluster_grid.py`) where each
cell represents an arbitrary node-count slice — cells do not correspond to specific physical
nodes, and rack locality is invisible. Operators want to:

1. See each running job's *actual* node placement, not just its size.
2. Tell at a glance whether a job is rack-local or scattered.
3. Select a specific job (by clicking a node cell or picking from a list) and see all of its
   nodes highlighted on the grid.

The reference is the rack visualization at <https://status.alcf.anl.gov/#/aurora>, where each
rack is its own labeled mini-grid and node colors map to running jobs.

## Goals

- Add a `Racks` tab alongside the existing `Cluster` tab. The existing tab is unchanged.
- Render each rack as a labeled mini-grid of node cells preserving canonical chassis layout
  (e.g. Aurora 2-cols × 7-rows blade arrangement).
- Color each cell by the job running on that node (free / down / unknown / reservation each get
  distinct glyphs).
- Provide a job sidebar (running jobs only) with click + arrow-key selection that highlights all
  cells of the chosen job on the rack grid.
- Click a rack-name label to filter the job sidebar to jobs touching that rack.
- Keyboard navigation across cells with arrow keys when the rack panel has focus.
- Show a per-rack utilization summary (e.g. `12/14`) under each rack name.
- Detect known machines (initially Aurora; Polaris/Sophia patterns added as their formats are
  verified) and apply curated rack layouts so empty racks still appear in their canonical slot.
  Unknown machines fall back to a generic row-major layout.

## Non-goals

- Modifying or removing the existing `Cluster` tab (proportional bar) — left untouched.
- Cross-tab selection sync (selection in `Jobs` tab does not auto-highlight on `Racks` tab and
  vice versa) — out of scope for this round.
- Supporting node states beyond what `Node.primary_state()` already returns
  (`free` / `job-exclusive` / `offline` / `down` / `state-unknown` / `resv-exclusive`).
- Rendering historical / time-series data.

## Architecture

### New modules

```
src/pbs_tui/
  rack_layout.py        # NEW — rack-name parsing, machine layouts, NodeId model (~150 LOC)
  rack_grid.py          # NEW — RackGridWidget + rack-mini-grid renderer (~500 LOC)
  nodes.py              # +1 helper: job_node_assignments(snapshot)
  app.py                # +1 TabPane "Racks", + 2 message handlers
  app.tcss              # +CSS rules for new widget
tests/
  test_rack_layout.py   # parsing + layout-detection tests
  test_rack_grid.py     # render + selection tests
```

### Data model — `rack_layout.py`

```python
@dataclass(frozen=True)
class NodeId:
    rack: str          # "x4702"
    slot: str          # "b07"
    raw: str           # original node name

@dataclass(frozen=True)
class RackSpec:
    name: str          # "x4702"
    rows: int          # blade-rows in the mini-grid
    cols: int          # blade-cols (e.g. 2 for Aurora)

@dataclass(frozen=True)
class MachineLayout:
    name: str                          # "aurora" | "polaris" | "sophia" | "generic"
    rack_rows: list[list[str]]         # 2D rack-name grid by display row
    rack_specs: dict[str, RackSpec]
    rack_slots: dict[str, list[str]]   # rack name -> canonical ordered slot ids

def parse_node_id(name: str) -> NodeId | None: ...
def detect_layout(node_names: Iterable[str]) -> MachineLayout: ...
```

**Detection rule.** Parse all node names; if ≥80% match a known machine pattern (initially
`x[34]\d{3}-b\d{2}` for Aurora) use that machine's curated layout. Otherwise produce a generic
layout from the racks observed in the snapshot.

**Curated vs observed racks.** A curated `MachineLayout.rack_rows` enumerates *every* rack the
machine is known to have, including racks that no node in the current snapshot mentions. Such
racks render as empty mini-grids (sized from `rack_specs[name]`) so the spatial geometry is
preserved. Conversely, a node whose rack id is parseable but is *not* in the curated layout
(e.g. a newly added rack the layout doesn't know about) is appended to a final overflow row
labeled "unmapped" so it remains visible.

### Job→node mapping — `nodes.py`

```python
def job_node_assignments(snapshot: SchedulerSnapshot) -> dict[str, list[str]]:
    """Map running-job id -> list of exec_host node names."""
```

Implementation reuses the existing `extract_exec_host_nodes` helper. Jobs without parseable
exec hosts are excluded from the map.

### Widget composition — `rack_grid.py`

`RackGridWidget` is a `Vertical` container composing:

```
HeaderBar              # 1 line: machine name + counts
Horizontal (1fr):
  RackPanel            # HorizontalScroll holding rack mini-grids
  JobList              # ListView, fixed width ~36
LegendBar              # 1 line: glyph legend
```

**Rack mini-grid rendering.** Each rack is a small block: rack name (1 line) + utilization
(1 line dim) + `cols × rows` cells (1 char/cell horizontally, 1 line/cell vertically) + 1
blank line of trailing padding. Racks are joined horizontally within a row with one space of
padding; rack rows are joined vertically with one blank line.

**Cell glyphs and colors.**

| Node state                  | Glyph | Color                                       |
|-----------------------------|-------|---------------------------------------------|
| `job-exclusive`             | space | per-job color (palette inherited from cluster_grid) |
| `free`                      | `░`   | empty-style (gray)                          |
| `offline`, `down`           | `×`   | dark-gray                                   |
| `state-unknown`             | `?`   | dim                                         |
| `resv-exclusive` (or other) | `▒`   | desaturated accent                          |

**Selection visual.** When a job is selected, its cells get an inverted style (foreground = job
color, background = `surface-lighten-3`). The visualization is otherwise unchanged so the user
can still see other running jobs.

**Color reuse.** The job color palette is shared with `cluster_grid.py`: `RackGridWidget`
imports `_build_palette` and color helpers from `cluster_grid` to keep palettes consistent
across tabs and themes (including ANSI themes).

### Job sidebar (`JobList`)

- Built from running jobs only, sorted by node-count descending (matches today's behavior).
- Row format: `█ user 512n queue [12h57m]` — color block matches job color.
- `up`/`down` arrows move selection; `enter` and single click both confirm.
- Selection posts `RackGridWidget.JobSelected(job_id)` to the parent app.
- When a rack filter is active, a header chip above the list reads `Filtered: x4702 (×N jobs)`
  with `×` as a clear-filter affordance; clicking the chip clears the filter.

### Interactions

| Action                                  | Effect                                                     |
|-----------------------------------------|------------------------------------------------------------|
| Click a colored node cell               | Select that job; sidebar cursor moves to it; detail panel shows job |
| Click a free/down/unknown node cell     | Select that node; detail panel shows node                  |
| Click a rack-name label                 | Filter `JobList` to jobs touching that rack; clicking the *same* rack again clears the filter; clicking a *different* rack switches the filter to that rack |
| Arrow key on focused `RackPanel`        | Move cell cursor; wraps within rack, then to next rack in reading order |
| `enter` on focused `RackPanel`          | Select what's under the cursor                              |
| `escape`                                | Clear job selection and rack filter                         |
| Sidebar row click / `enter`             | Same as cell click on a colored cell                        |

### Messages

```python
class RackGridWidget(Widget):
    class JobSelected(Message):
        def __init__(self, job_id: str) -> None: ...
    class NodeSelected(Message):
        def __init__(self, node_name: str) -> None: ...
```

In `PBSTUI` (`app.py`):

```python
def on_rack_grid_widget_job_selected(self, event):
    # Reuses existing DetailPanel.show_job(); no new selection state plumbing.

def on_rack_grid_widget_node_selected(self, event):
    # Reuses existing DetailPanel.show_node().
```

### CSS additions (`app.tcss`)

```css
#rack_grid { padding: 1 2; }
#rack_grid_header { height: auto; }
#rack_panel { height: 1fr; overflow-x: scroll; overflow-y: auto; }
#rack_job_list { width: 36; height: 1fr; border-left: tall $surface; }
#rack_legend { height: auto; margin-top: 1; }
```

## Layout detection details

**Aurora (`name="aurora"`)**
- Rack pattern: `x[34]\d{3}-b\d{2}` where the rack id is the leading `x[34]\d{3}` and the slot
  is the trailing `b\d{2}`.
- Rack rows in display order: `x47XX`, `x46XX`, `x45XX`, ... (descending — top-to-bottom mirrors
  the ALCF screenshot, with top racks numbered higher).
- Per rack: 2 cols × 7 rows = 14 blades.

**Polaris and Sophia**
- Curated layouts deferred until their actual node-name patterns are verified against live data.
- Until then, both clusters fall through to the generic detection path. Adding a curated entry
  later is purely additive (a new entry in the layout-detection table) and does not require
  changes elsewhere in the widget.

**Generic (`name="generic"`)**
- Group nodes by best-effort rack prefix (the substring before the last `-`, or the leading
  alphabetic-prefix of the name) and arrange resulting racks in a row-major grid sized to the
  terminal width. Each rack's mini-grid is shaped roughly square: `cols = ceil(sqrt(n))`,
  `rows = ceil(n / cols)`, capping `rows` at 16 so very large racks split into multiple
  side-by-side mini-grids. This guarantees the widget renders something sane on any cluster.

## Testing

- `tests/test_rack_layout.py`: `parse_node_id` covers Aurora and pathological names;
  `detect_layout` covers the ≥80% threshold logic and the curated-vs-observed reconciliation
  (curated empty racks render; unmapped observed racks land in the overflow row); the generic
  fallback shapes racks roughly square as specified.
- `tests/test_rack_grid.py`:
  - Renders a small synthetic snapshot (3 racks × 4 nodes) with two jobs and asserts cell
    glyphs and color-runs.
  - Asserts that selecting a job marks exactly its node cells as inverted.
  - Asserts rack-name filter narrows the `JobList`.
  - Asserts node-state glyphs for `free`/`offline`/`state-unknown`/`resv-exclusive`.
- Existing tests for `cluster_grid.py` are not modified; the `Cluster` tab behavior is
  unchanged.

## Risks & mitigations

- **Terminal width.** A full Aurora layout (~21 racks × 8 rack rows) is wide. Mitigated by
  `HorizontalScroll` so layout stays geometrically faithful.
- **Color palette parity.** Reusing `cluster_grid._build_palette` keeps job colors stable across
  the two tabs as long as the running-job set hasn't changed between renders. If they ever
  diverge (different `job_count` parameters), palette rotation could differ. Mitigation: pass
  the same `job_count` (number of non-aggregated running jobs) into both calls; share a single
  palette per snapshot if practical.
- **Polaris/Sophia rack patterns.** Curated patterns may need adjustment on first contact with
  real data; the generic fallback prevents the tab from breaking.
- **Reservation detection.** PBS surfaces reservations through node `state` strings and through
  `Job.queue`/`Job.location`; this round only handles node-state glyphs (`resv-exclusive`).
  Reservation-holding jobs not appearing in the sidebar is intentional.
- **Test fixtures.** The existing `samples.py` Aurora-style mock data already produces parseable
  node names (`x3001-b00` etc.), so the new tab will render meaningfully under
  `PBS_TUI_SAMPLE_DATA=1`.

## Out of scope (explicit)

- Cross-tab selection synchronization.
- Hover tooltips (Textual mouse-hover support is uneven across terminals).
- Persistent selection across refresh cycles beyond the current session (selection is reset on
  data refresh if the selected job no longer exists).
- Animated highlighting (pulse).
- Configurable rack layouts via user config — only built-in machine detection.

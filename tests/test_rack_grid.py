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
            "r1": ["r1-a", "r1-b", "r1-c", "r1-d"],
            "r2": ["r2-a", "r2-b", "r2-c", "r2-d"],
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
    # Walk Text spans and verify only j1 cells got the highlight style
    # (bold underline) — j2's r1-c cell must NOT be highlighted.
    selected_coords = {
        (model.cells_by_node["r1-a"].row, model.cells_by_node["r1-a"].col),
        (model.cells_by_node["r1-b"].row, model.cells_by_node["r1-b"].col),
    }
    highlighted: list[tuple[int, int]] = []
    rendered = text.plain.splitlines()
    assert len(rendered) > 0
    cursor_row = 0
    cursor_col = 0
    for span_text, span_style in zip(*_walk_text_spans(text)):
        if "\n" in span_text:
            cursor_row += span_text.count("\n")
            cursor_col = 0
            continue
        for offset in range(len(span_text)):
            style_str = str(span_style) if span_style else ""
            if "underline" in style_str and "bold" in style_str:
                highlighted.append((cursor_row, cursor_col + offset))
        cursor_col += len(span_text)
    assert set(highlighted) == selected_coords


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


from pbs_tui.rack_grid import build_job_list_entries, JobListEntry, _rack_box_size
from pbs_tui.rack_layout import RackSpec
from tests.util import make_job


# ---------------------------------------------------------------------------
# Tests for items from PR #19 review feedback
# ---------------------------------------------------------------------------


def test_escape_binding_exists_on_rack_panel():
    """_RackPanel.BINDINGS must include an escape binding."""
    from pbs_tui.rack_grid import _RackPanel
    keys = [b[0] for b in _RackPanel.BINDINGS]
    assert "escape" in keys


def test_rack_panel_selection_cleared_message_class_exists():
    """_RackPanel.SelectionCleared message class must exist."""
    from pbs_tui.rack_grid import _RackPanel
    assert hasattr(_RackPanel, "SelectionCleared")
    # It should be a Message subclass
    from textual.message import Message
    assert issubclass(_RackPanel.SelectionCleared, Message)


def test_cell_clicked_message_carries_owner_job_id():
    """_RackPanel.CellClicked must carry owner_job_id."""
    from pbs_tui.rack_grid import _RackPanel
    msg = _RackPanel.CellClicked(node_name="r1-a", rack_name=None, owner_job_id="j99")
    assert msg.node_name == "r1-a"
    assert msg.rack_name is None
    assert msg.owner_job_id == "j99"


def test_cell_clicked_owner_job_id_defaults_to_none():
    """owner_job_id should default to None for backward compat (rack clicks)."""
    from pbs_tui.rack_grid import _RackPanel
    msg = _RackPanel.CellClicked(node_name=None, rack_name="r1")
    assert msg.owner_job_id is None


def test_rack_box_size_accounts_for_util_width():
    """Width must be at least as wide as the util 'n/N' string."""
    # Rack "r1" with cols=2, capacity 14: util "14/14" is 5 chars but cols=2
    spec = RackSpec(name="r1", rows=7, cols=2)
    assert spec.capacity() == 14
    util_str = f"{spec.capacity()}/{spec.capacity()}"
    assert len(util_str) == 5  # "14/14"
    width, _ = _rack_box_size(spec)
    assert width >= len(util_str), (
        f"rack box width {width} narrower than util string '{util_str}'"
    )


def test_rack_box_size_name_still_fits():
    """Width must also fit the rack name."""
    spec = RackSpec(name="x4702", rows=7, cols=2)
    width, _ = _rack_box_size(spec)
    assert width >= len("x4702")


def test_render_to_text_uses_real_slot_count_for_util():
    """Util denominator is len(rack_slots[rack]), not spec.capacity().

    When a rack has fewer real slots than capacity() (common after Aurora
    geometry rounding), the denominator shown must match the slot count.
    """
    from pbs_tui.rack_layout import MachineLayout

    # Build a layout where r1 has capacity 4 (2×2) but only 3 real slots.
    spec = RackSpec(name="r1", rows=2, cols=2)
    assert spec.capacity() == 4
    layout = MachineLayout(
        name="test",
        rack_rows=[["r1"]],
        rack_specs={"r1": spec},
        rack_slots={"r1": ["r1-a", "r1-b", "r1-c"]},  # only 3 slots
    )
    snap = SchedulerSnapshot(
        nodes=[
            Node(name="r1-a", state="job-exclusive"),
            Node(name="r1-b", state="free"),
            Node(name="r1-c", state="free"),
        ],
        jobs=[],
    )
    model = build_render_model(layout, snap, job_assignments={"j1": ["r1-a"]})
    text = render_to_text(model, palette=_palette(), running_jobs={"j1": 0}, selected_job_id=None)
    # The util line should show "1/3" (1 occupied out of 3 real slots), not "1/4"
    assert "1/3" in text.plain
    assert "1/4" not in text.plain


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


# ---------------------------------------------------------------------------
# Job-list scrolling (right sidebar in the Racks tab)
# ---------------------------------------------------------------------------


def _many_entries(count: int) -> list:
    """Build *count* JobListEntry rows for driving the sidebar widget."""
    return [
        JobListEntry(
            job_id=f"j{i}",
            user=f"user{i}",
            queue="preemptable",
            node_count=1,
            palette_index=i,
            time_remaining_str="63h48m",
            nodes=[f"x{i}"],
        )
        for i in range(count)
    ]


def test_job_list_scrolls_when_entries_overflow_viewport():
    """The sidebar must expose vertical scrolling when the running-job list is
    taller than its viewport — otherwise off-screen jobs are unreachable.

    Regression test: previously ``_JobListWidget`` subclassed a bare ``Widget``
    and rendered its own ``Text``, so Textual clamped its virtual size to the
    viewport and ``max_scroll_y`` stayed 0.
    """
    import asyncio

    from textual.app import App, ComposeResult
    from textual.containers import Horizontal

    from pbs_tui.rack_grid import _JobListWidget

    palette = _palette_many(50)
    entries = _many_entries(50)

    class Probe(App):
        def compose(self) -> ComposeResult:
            with Horizontal():
                yield _JobListWidget(id="jl")

    async def interact() -> None:
        app = Probe()
        async with app.run_test(size=(80, 24)) as pilot:
            widget = app.query_one("#jl", _JobListWidget)
            widget.update(entries, palette, selected_id=None, rack_filter=None)
            widget.focus()
            await pilot.pause()

            assert widget.is_scrollable, "job list should be scrollable"
            assert widget.max_scroll_y > 0, (
                f"expected positive max_scroll_y, got {widget.max_scroll_y}; "
                f"virtual height={widget.virtual_size.height}"
            )

            widget.scroll_end(animate=False)
            await pilot.pause()
            assert widget.scroll_offset.y > 0, "scroll_end should move the viewport"

    asyncio.run(interact())


def _palette_many(n: int) -> "Palette":
    return Palette(
        job_styles=[f"on color({16 + (i % 200)})" for i in range(max(1, n))],
        agg_colors=["#445566"],
        empty_style="on color(236)",
    )


def test_job_list_click_selects_correct_job_when_scrolled():
    """Clicking a row after scrolling must select that row's job, not the row
    that happens to sit at the same viewport offset.

    Regression test: the old handler mapped ``event.y`` directly to an entry
    index, which selected the wrong job once the list was scrolled.
    """
    import asyncio

    from textual.app import App, ComposeResult
    from textual.containers import Horizontal

    from pbs_tui.rack_grid import _JobListWidget, _JobRow

    palette = _palette_many(50)
    entries = _many_entries(50)
    chosen: list = []

    class Probe(App):
        def compose(self) -> ComposeResult:
            with Horizontal():
                yield _JobListWidget(id="jl")

        def on__job_list_widget_job_chosen(self, event) -> None:
            chosen.append(event.job_id)

    async def interact() -> None:
        app = Probe()
        async with app.run_test(size=(80, 24)) as pilot:
            widget = app.query_one("#jl", _JobListWidget)
            widget.update(entries, palette, selected_id=None, rack_filter=None)
            widget.focus()
            await pilot.pause()

            # Scroll well down the list, then click a row that is now on screen.
            widget.scroll_to(y=30, animate=False)
            await pilot.pause()

            # Find a row widget that is actually visible in the viewport and
            # click it; its job_id must be what gets chosen.
            target = None
            for row in widget.query(_JobRow):
                if widget.scrollable_content_region.contains_region(row.region):
                    target = row
                    break
            assert target is not None, "expected a visible row after scrolling"
            expected_job = target.job_id
            await pilot.click(target)
            await pilot.pause()

            assert chosen == [expected_job], (
                f"clicked row {expected_job!r} but chose {chosen!r}"
            )

    asyncio.run(interact())


def test_job_list_cursor_scrolls_into_view_on_arrow_navigation():
    """Moving the cursor with arrow keys past the viewport edge must scroll the
    list so the cursor stays visible."""
    import asyncio

    from textual.app import App, ComposeResult
    from textual.containers import Horizontal

    from pbs_tui.rack_grid import _JobListWidget

    palette = _palette_many(50)
    entries = _many_entries(50)

    class Probe(App):
        def compose(self) -> ComposeResult:
            with Horizontal():
                yield _JobListWidget(id="jl")

    async def interact() -> None:
        app = Probe()
        async with app.run_test(size=(80, 24)) as pilot:
            widget = app.query_one("#jl", _JobListWidget)
            widget.update(entries, palette, selected_id=None, rack_filter=None)
            widget.focus()
            await pilot.pause()

            assert widget.scroll_offset.y == 0
            # Press up once: cursor wraps to the last entry, which is off-screen,
            # so the list must scroll down to reveal it.
            await pilot.press("up")
            await pilot.pause()
            assert widget.scroll_offset.y > 0, (
                "cursor moved to the bottom entry but the list did not scroll"
            )

    asyncio.run(interact())


def test_job_list_filter_chip_click_clears_filter():
    """Clicking the filter chip in the header emits FilterCleared."""
    import asyncio

    from textual.app import App, ComposeResult
    from textual.containers import Horizontal

    from pbs_tui.rack_grid import _JobListWidget, _JobListFilterChip

    palette = _palette_many(5)
    entries = _many_entries(5)
    cleared: list = []

    class Probe(App):
        def compose(self) -> ComposeResult:
            with Horizontal():
                yield _JobListWidget(id="jl")

        def on__job_list_widget_filter_cleared(self, event) -> None:
            cleared.append(True)

    async def interact() -> None:
        app = Probe()
        async with app.run_test(size=(80, 24)) as pilot:
            widget = app.query_one("#jl", _JobListWidget)
            widget.update(entries, palette, selected_id=None, rack_filter="r1")
            widget.focus()
            await pilot.pause()

            chip = app.query_one(_JobListFilterChip)
            await pilot.click(chip)
            await pilot.pause()
            assert cleared, "clicking the filter chip should clear the filter"

    asyncio.run(interact())


# ---------------------------------------------------------------------------
# Rack panel click mapping under scroll (left grid pane)
# ---------------------------------------------------------------------------


def _overflowing_layout(rack_rows: int, rack_cols: int) -> MachineLayout:
    """A machine layout whose rendered grid overflows a small viewport on both
    axes: *rack_rows* × *rack_cols* racks, each a 3×2 mini-grid."""
    specs = {}
    rows = []
    slots = {}
    for r in range(rack_rows):
        row = []
        for c in range(rack_cols):
            name = f"R{r}_{c}"
            specs[name] = RackSpec(name=name, rows=3, cols=2)
            slots[name] = [f"{name}s{i}" for i in range(6)]
            row.append(name)
        rows.append(row)
    return MachineLayout(name="t", rack_specs=specs, rack_rows=rows, rack_slots=slots)


def test_rack_panel_click_selects_correct_cell_under_scroll():
    """Clicking a node glyph must select that node in every scroll state.

    Regression test: `_RackPanel.on_click` added `scroll_offset` to the click
    coordinates, but Textual already delivers `event.x`/`event.y` in the inner
    Static's content space (scroll applied). Adding the offset double-counted,
    so clicks selected the wrong cell — or None — once the panel was scrolled.
    """
    import asyncio

    from textual.app import App, ComposeResult
    from textual.containers import Horizontal

    from pbs_tui.rack_grid import _RackPanel, render_to_text

    layout = _overflowing_layout(rack_rows=8, rack_cols=10)
    snap = SchedulerSnapshot(
        nodes=[
            Node(name=n, state="job-exclusive")
            for lst in layout.rack_slots.values()
            for n in lst
        ],
        jobs=[],
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    model = build_render_model(layout, snap, job_assignments={})
    palette = _palette()
    captured: list = []

    class Probe(App):
        def compose(self) -> ComposeResult:
            with Horizontal():
                yield _RackPanel(id="rp")

        def on__rack_panel_cell_clicked(self, event) -> None:
            captured.append(event.node_name)

    async def interact() -> None:
        app = Probe()
        async with app.run_test(size=(50, 18)) as pilot:
            panel = app.query_one("#rp", _RackPanel)
            text = render_to_text(
                model, palette=palette, running_jobs={}, selected_job_id=None
            )
            panel.update(text, model)
            panel.focus()
            await pilot.pause()
            inner = panel.query_one("#rack_panel_inner")

            def first_visible_target():
                """A node whose glyph is on-screen and away from the left edge,
                so a mis-mapped click lands on a *different* identifiable node."""
                for name, cell in model.cells_by_node.items():
                    sx = inner.region.offset.x + cell.col
                    sy = inner.region.offset.y + cell.row
                    if (
                        panel.content_region.contains(sx, sy)
                        and sx > panel.content_region.x + 1
                    ):
                        return name, sx, sy
                return None

            for label, (sox, soy) in [
                ("unscrolled", (0, 0)),
                ("v-scroll", (0, 8)),
                ("h-scroll", (8, 0)),
                ("both", (8, 8)),
            ]:
                panel.scroll_to(x=sox, y=soy, animate=False)
                await pilot.pause()
                target = first_visible_target()
                assert target is not None, f"no visible target for {label}"
                name, sx, sy = target
                captured.clear()
                await pilot.click("#rp", offset=(sx, sy))
                await pilot.pause()
                assert captured == [name], (
                    f"{label} (scroll {sox},{soy}): clicked glyph of {name!r} "
                    f"but panel reported {captured!r}"
                )

    asyncio.run(interact())

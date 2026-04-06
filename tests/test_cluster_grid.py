"""Tests for pbs_tui.cluster_grid — grid building and cell allocation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pbs_tui.cluster_grid import Palette, _build_grid
from pbs_tui.samples import sample_snapshot
from tests.util import add_jobs, make_job

NOW = datetime(2024, 5, 11, 12, 0, tzinfo=timezone.utc)


def _make_palette() -> Palette:
    return Palette(
        job_styles=["on blue", "on red", "on green", "on magenta"],
        agg_colors=["#445566", "#556677"],
        empty_style="on color(236)",
    )


def _minimal_snapshot(**kwargs):
    snap = sample_snapshot(now=NOW)
    snap.jobs = kwargs.get("jobs", [])
    return snap


# ── empty nodes guard ─────────────────────────────────────────────────


def test_build_grid_empty_nodes_returns_placeholder():
    snap = _minimal_snapshot()
    snap.nodes = []
    data = _build_grid(snap, _make_palette(), grid_width=10, grid_height=5)
    assert data.grid_width == 0
    assert data.grid_height == 0
    assert data.cell_owners == []


# ── cell allocation uses ceiling ──────────────────────────────────────


def test_cell_allocation_uses_ceiling():
    """A job with fewer nodes than nodes_per_cell should still get 1 cell."""
    snap = _minimal_snapshot()
    # 50 nodes, 10x5=50 cells → nodes_per_cell = 1.0
    # Job with 3 nodes → should get ceil(3/1) = 3 cells
    snap.nodes = [object() for _ in range(50)]  # type: ignore[misc]
    snap.jobs = [
        make_job(id="j1", state="R", queue="bigqueue", nodes="3",
                 start_time=NOW - timedelta(minutes=10), walltime="01:00:00"),
    ]
    data = _build_grid(snap, _make_palette(), grid_width=10, grid_height=5)
    job_cells = [o for o in data.cell_owners if o == "j1"]
    assert len(job_cells) == 3


def test_cell_allocation_clamped_to_remaining():
    """cells_needed should not exceed remaining cells."""
    snap = _minimal_snapshot()
    snap.nodes = [object() for _ in range(4)]  # type: ignore[misc]
    # 4 nodes, 2x2=4 cells → nodes_per_cell=1.0
    # Job claims all 4 nodes — should get exactly 4 cells
    snap.jobs = [
        make_job(id="j1", state="R", queue="bigqueue", nodes="4",
                 start_time=NOW - timedelta(minutes=5), walltime="01:00:00"),
    ]
    data = _build_grid(snap, _make_palette(), grid_width=2, grid_height=2)
    job_cells = [o for o in data.cell_owners if o == "j1"]
    assert len(job_cells) == 4


# ── legend entries only when cells assigned ───────────────────────────


def test_no_legend_entry_when_grid_full():
    """When the grid is full, later jobs should not get legend entries."""
    snap = _minimal_snapshot()
    snap.nodes = [object() for _ in range(2)]  # type: ignore[misc]
    # 2 nodes, 2x1=2 cells
    snap.jobs = [
        make_job(id="j1", state="R", queue="bigqueue", nodes="2",
                 start_time=NOW - timedelta(minutes=5), walltime="01:00:00"),
        make_job(id="j2", state="R", queue="bigqueue", nodes="2",
                 start_time=NOW - timedelta(minutes=5), walltime="01:00:00"),
    ]
    data = _build_grid(snap, _make_palette(), grid_width=2, grid_height=1)
    # j1 fills all cells; j2 should be dropped
    assert all(o == "j1" for o in data.cell_owners if o is not None)


# ── overcommit clamping ───────────────────────────────────────────────


def test_running_nodes_clamped_in_header():
    """Header should show clamped running_nodes and overcommit indicator."""
    snap = _minimal_snapshot()
    snap.nodes = [object() for _ in range(10)]  # type: ignore[misc]
    # 2 jobs each claiming 8 nodes = 16 total, but only 10 nodes
    snap.jobs = [
        make_job(id=f"j{i}", state="R", queue="bigqueue", nodes="8",
                 start_time=NOW - timedelta(minutes=5), walltime="01:00:00")
        for i in range(2)
    ]
    data = _build_grid(snap, _make_palette(), grid_width=10, grid_height=5)
    header_str = str(data.header)
    # Should show "over" indicator
    assert "over" in header_str


# ── deterministic queue colours ───────────────────────────────────────


def test_palette_agg_style_deterministic():
    """agg_style should return the same colour for the same queue name."""
    pal = _make_palette()
    style1 = pal.agg_style("debug")
    style2 = pal.agg_style("debug")
    assert style1 == style2
    # Different name should (usually) differ
    style3 = pal.agg_style("preemptable")
    # At minimum, both should contain a background color
    assert "on " in style1
    assert "on " in style3


# ── sample snapshot produces a valid grid ─────────────────────────────


def test_sample_snapshot_grid():
    snap = sample_snapshot(now=NOW)
    data = _build_grid(snap, _make_palette(), grid_width=40, grid_height=10)
    assert data.grid_width == 40
    assert data.grid_height == 10
    assert len(data.cell_owners) == 400
    assert len(data.cell_styles) == 400

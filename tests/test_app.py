from datetime import datetime, timezone

import re

import pytest

from rich.console import Console

from pbs_tui.app import PBSTUI, _env_flag, run, snapshot_to_markdown, snapshot_to_table
from pbs_tui.data import Job, SchedulerSnapshot
from pbs_tui.samples import sample_snapshot


def make_job(**overrides):
    defaults = dict(
        id="job",
        name="demo",
        user="alice",
        queue="work",
        state="Q",
        exec_host=None,
        nodes=None,
        resources_requested={},
    )
    return Job(**(defaults | overrides))


def test_env_flag_truthy(monkeypatch):
    monkeypatch.delenv("TEST_FLAG", raising=False)
    assert not _env_flag("TEST_FLAG")
    monkeypatch.setenv("TEST_FLAG", "1")
    assert _env_flag("TEST_FLAG")
    monkeypatch.setenv("TEST_FLAG", "false")
    assert not _env_flag("TEST_FLAG")


def test_run_honours_environment(monkeypatch):
    monkeypatch.setenv("PBS_TUI_HEADLESS", "1")
    monkeypatch.setenv("PBS_TUI_AUTOPILOT", "quit")
    captured = {}

    class DummyFetcher:
        async def fetch_snapshot(self):  # pragma: no cover - not used in this path
            raise AssertionError("fetch_snapshot should not be called during CLI setup")

    def fake_run(self, *, headless=False, auto_pilot=None, **kwargs):
        captured["headless"] = headless
        captured["auto_pilot"] = auto_pilot

    monkeypatch.setattr(PBSTUI, "run", fake_run, raising=False)
    run(argv=[], fetcher=DummyFetcher())
    assert captured["headless"] is True
    assert captured["auto_pilot"] is not None


def test_run_inline_prints_rich_table(monkeypatch, capsys):
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    snapshot = sample_snapshot(now=now)
    snapshot.jobs.append(
        make_job(
            id="multi.123",
            name="multi",
            state="R",
            exec_host="nodeA/0+nodeB/1",
            nodes="nodeA+nodeB",
        )
    )
    snapshot.jobs.append(
        make_job(
            id="missing_nodes.456",
            name="missing_nodes",
            state="Q",
            exec_host=None,
            nodes=None,
        )
    )
    snapshot.jobs.append(
        make_job(
            id="malformed_nodes.789",
            name="malformed_nodes",
            state="Q",
            exec_host=None,
            nodes="!!!",
        )
    )

    class InlineFetcher:
        async def fetch_snapshot(self):
            return snapshot

    monkeypatch.delenv("PBS_TUI_HEADLESS", raising=False)
    monkeypatch.delenv("PBS_TUI_AUTOPILOT", raising=False)

    run(argv=["--inline"], fetcher=InlineFetcher())
    captured = capsys.readouterr()
    assert "PBS Jobs as of" in captured.out
    assert "Job ID" in captured.out
    assert "Node Count" in captured.out
    assert "climate_model" in captured.out
    assert "sample data" in captured.err.lower()

    job_line = next(
        (line for line in captured.out.splitlines() if "104829.aqua" in line),
        None,
    )
    assert job_line is not None, "Expected climate_model job row in inline output"
    cells = [cell.strip() for cell in re.split(r"\s{2,}", job_line.strip()) if cell.strip()]
    assert cells[6] == "1"
    assert cells[7] == "nid000001"

    multi_line = next(
        (line for line in captured.out.splitlines() if "multi.123" in line),
        None,
    )
    assert multi_line is not None, "Expected multi-node job row in inline output"
    multi_cells = [
        cell.strip()
        for cell in re.split(r"\s{2,}", multi_line.strip())
        if cell.strip()
    ]
    assert multi_cells[6] == "2"
    assert multi_cells[7] == "nodeA"

    missing_line = next(
        (line for line in captured.out.splitlines() if "missing_no" in line),
        None,
    )
    assert missing_line is not None, "Expected missing-nodes row in inline output"
    missing_cells = [
        cell.strip()
        for cell in re.split(r"\s{2,}", missing_line.strip())
        if cell.strip()
    ]
    assert missing_cells[6] == "-"
    assert missing_cells[7] == "-"

    malformed_line = next(
        (line for line in captured.out.splitlines() if "malformed_" in line),
        None,
    )
    assert malformed_line is not None, "Expected malformed-nodes row in inline output"
    malformed_cells = [
        cell.strip()
        for cell in re.split(r"\s{2,}", malformed_line.strip())
        if cell.strip()
    ]
    assert malformed_cells[5] == "!!!"
    assert malformed_cells[6] == "-"
    assert malformed_cells[7] == "-"


def test_run_inline_writes_markdown_file(monkeypatch, tmp_path, capsys):
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    snapshot = sample_snapshot(now=now)
    snapshot.jobs.append(
        make_job(
            id="multi.123",
            name="multi",
            state="R",
            exec_host="nodeA/0+nodeB/1",
            nodes="nodeA+nodeB",
        )
    )
    snapshot.jobs.append(
        make_job(
            id="numeric.456",
            name="numeric",
            nodes="2",
        )
    )

    class InlineFetcher:
        async def fetch_snapshot(self):
            return snapshot

    output_path = tmp_path / "snapshot.md"

    monkeypatch.delenv("PBS_TUI_HEADLESS", raising=False)
    monkeypatch.delenv("PBS_TUI_AUTOPILOT", raising=False)

    run(argv=["--inline", "--file", str(output_path)], fetcher=InlineFetcher())
    captured = capsys.readouterr()

    assert output_path.exists()
    contents = output_path.read_text()
    assert contents.startswith("### PBS Jobs as of")
    assert "| Job ID |" in contents
    assert "| Node Count |" in contents
    assert "climate_model" in contents
    assert "PBS Jobs as of" in captured.out
    assert "sample data" in captured.err.lower()

    job_row = next(
        (line for line in contents.splitlines() if "| 104829.aqua |" in line),
        None,
    )
    assert job_row is not None, "Expected climate_model row in markdown output"
    cells = [cell.strip() for cell in job_row.strip().strip("|").split("|")]
    assert cells[6] == "1"
    assert cells[7] == "nid000001"

    multi_row = next(
        (line for line in contents.splitlines() if "| multi.123 |" in line),
        None,
    )
    assert multi_row is not None, "Expected multi-node row in markdown output"
    multi_cells = [cell.strip() for cell in multi_row.strip().strip("|").split("|")]
    assert multi_cells[6] == "2"
    assert multi_cells[7] == "nodeA"

    numeric_row = next(
        (line for line in contents.splitlines() if "| numeric.456 |" in line),
        None,
    )
    assert numeric_row is not None, "Expected numeric-only row in markdown output"
    numeric_cells = [
        cell.strip()
        for cell in numeric_row.strip().strip("|").split("|")
    ]
    assert numeric_cells[5] == "2"
    assert numeric_cells[6] == "2"
    assert numeric_cells[7] == "-"


def test_run_file_without_inline_exits(monkeypatch):
    class DummyFetcher:
        async def fetch_snapshot(self):  # pragma: no cover - should not run
            raise AssertionError("fetch_snapshot should not be called")

    monkeypatch.delenv("PBS_TUI_HEADLESS", raising=False)
    monkeypatch.delenv("PBS_TUI_AUTOPILOT", raising=False)

    with pytest.raises(SystemExit):
        run(argv=["--file", "out.md"], fetcher=DummyFetcher())


def test_snapshot_outputs_handle_job_without_nodes():
    job = make_job(id="no_nodes", name="no_nodes", resources_requested={})
    snapshot = SchedulerSnapshot(jobs=[job], source="test")

    markdown = snapshot_to_markdown(snapshot)
    markdown_row = next(
        (line for line in markdown.splitlines() if "| no_nodes |" in line),
        None,
    )
    assert markdown_row is not None
    markdown_cells = [cell.strip() for cell in markdown_row.strip().strip("|").split("|")]
    assert markdown_cells[6] == "-"
    assert markdown_cells[7] == "-"

    table = snapshot_to_table(snapshot)
    console = Console(record=True, width=120)
    console.print(table)
    rendered = console.export_text()
    table_row = next((line for line in rendered.splitlines() if "no_nodes" in line), None)
    assert table_row is not None
    table_cells = [cell.strip() for cell in re.split(r"\s{2,}", table_row.strip()) if cell.strip()]
    assert table_cells[6] == "-"
    assert table_cells[7] == "-"



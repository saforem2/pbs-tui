from datetime import datetime, timezone

import re

import pytest

from rich.console import Console

from pbs_tui.app import (
    JOB_TABLE_COLUMNS,
    PBSTUI,
    _env_flag,
    run,
    snapshot_to_markdown,
    snapshot_to_table,
)
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


def add_jobs(snapshot: SchedulerSnapshot, *job_overrides: dict) -> None:
    snapshot.jobs.extend(make_job(**overrides) for overrides in job_overrides)


COLUMN_NAMES = [name for name, _ in JOB_TABLE_COLUMNS]


def _row_from_rich(line: str) -> dict[str, str]:
    cells = [cell.strip() for cell in re.split(r"\s{2,}", line.strip()) if cell.strip()]
    assert len(cells) == len(COLUMN_NAMES), f"Unexpected cell count in line: {line}"
    return dict(zip(COLUMN_NAMES, cells))


def _row_from_markdown(line: str) -> dict[str, str]:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    assert len(cells) == len(COLUMN_NAMES), f"Unexpected cell count in markdown row: {line}"
    return dict(zip(COLUMN_NAMES, cells))


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
    add_jobs(
        snapshot,
        dict(
            id="multi.123",
            name="multi",
            state="R",
            exec_host="nodeA/0+nodeB/1",
            nodes="nodeA+nodeB",
        ),
        dict(
            id="missing_nodes.456",
            name="missing_nodes",
            state="Q",
            exec_host=None,
            nodes=None,
        ),
        dict(
            id="malformed_nodes.789",
            name="malformed_nodes",
            state="Q",
            exec_host=None,
            nodes="!!!",
        ),
        dict(
            id="conflict.456",
            name="conflict_job",
            state="R",
            exec_host="nodeX/0+nodeY/1",
            nodes="nodeA+nodeB",
        ),
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
    row = _row_from_rich(job_line)
    assert row["Node Count"] == "1"
    assert row["First Node"] == "nid000001"

    multi_line = next(
        (line for line in captured.out.splitlines() if "multi.123" in line),
        None,
    )
    assert multi_line is not None, "Expected multi-node job row in inline output"
    multi_row = _row_from_rich(multi_line)
    assert multi_row["Node Count"] == "2"
    assert multi_row["First Node"] == "nodeA"

    missing_line = next(
        (line for line in captured.out.splitlines() if "missing_no" in line),
        None,
    )
    assert missing_line is not None, "Expected missing-nodes row in inline output"
    missing_row = _row_from_rich(missing_line)
    assert missing_row["Node Count"] == "-"
    assert missing_row["First Node"] == "-"

    malformed_line = next(
        (line for line in captured.out.splitlines() if "malformed_" in line),
        None,
    )
    assert malformed_line is not None, "Expected malformed-nodes row in inline output"
    malformed_row = _row_from_rich(malformed_line)
    assert malformed_row["Nodes"] == "!!!"
    assert malformed_row["Node Count"] == "-"
    assert malformed_row["First Node"] == "-"

    conflict_line = next(
        (line for line in captured.out.splitlines() if "conflict_job" in line),
        None,
    )
    assert conflict_line is not None, "Expected conflicting exec_host row in inline output"
    conflict_row = _row_from_rich(conflict_line)
    assert conflict_row["Node Count"] == "2"
    assert conflict_row["First Node"] == "nodeX"


def test_run_inline_writes_markdown_file(monkeypatch, tmp_path, capsys):
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    snapshot = sample_snapshot(now=now)
    add_jobs(
        snapshot,
        dict(
            id="multi.123",
            name="multi",
            state="R",
            exec_host="nodeA/0+nodeB/1",
            nodes="nodeA+nodeB",
        ),
        dict(id="numeric.456", name="numeric", nodes="2"),
        dict(
            id="bracket.789",
            name="bracketed",
            state="Q",
            nodes="node[01-03]",
        ),
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
    row = _row_from_markdown(job_row)
    assert row["Node Count"] == "1"
    assert row["First Node"] == "nid000001"

    multi_row = next(
        (line for line in contents.splitlines() if "| multi.123 |" in line),
        None,
    )
    assert multi_row is not None, "Expected multi-node row in markdown output"
    multi_row_cells = _row_from_markdown(multi_row)
    assert multi_row_cells["Node Count"] == "2"
    assert multi_row_cells["First Node"] == "nodeA"

    numeric_row = next(
        (line for line in contents.splitlines() if "| numeric.456 |" in line),
        None,
    )
    assert numeric_row is not None, "Expected numeric-only row in markdown output"
    numeric_row_cells = _row_from_markdown(numeric_row)
    assert numeric_row_cells["Nodes"] == "2"
    assert numeric_row_cells["Node Count"] == "2"
    assert numeric_row_cells["First Node"] == "-"

    bracket_row = next(
        (line for line in contents.splitlines() if "| bracket.789 |" in line),
        None,
    )
    assert bracket_row is not None, "Expected bracketed row in markdown output"
    bracket_row_cells = _row_from_markdown(bracket_row)
    assert bracket_row_cells["Nodes"] == "node[01-03]"
    assert bracket_row_cells["Node Count"] == "3"
    assert bracket_row_cells["First Node"] == "node01"


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
    resource_job = make_job(
        id="resource_only",
        name="resource_only",
        nodes=None,
        resources_requested={"select": "2:ncpus=36"},
    )
    snapshot = SchedulerSnapshot(jobs=[job, resource_job], source="test")

    markdown = snapshot_to_markdown(snapshot)
    markdown_row = next(
        (line for line in markdown.splitlines() if "| no_nodes |" in line),
        None,
    )
    assert markdown_row is not None
    markdown_cells = _row_from_markdown(markdown_row)
    assert markdown_cells["Node Count"] == "-"
    assert markdown_cells["First Node"] == "-"

    table = snapshot_to_table(snapshot)
    console = Console(record=True, width=120)
    console.print(table)
    rendered = console.export_text()
    table_row = next((line for line in rendered.splitlines() if "no_nodes" in line), None)
    assert table_row is not None
    table_cells = _row_from_rich(table_row)
    assert table_cells["Node Count"] == "-"
    assert table_cells["First Node"] == "-"

    resource_markdown_row = next(
        (line for line in markdown.splitlines() if "| resource_only |" in line),
        None,
    )
    assert resource_markdown_row is not None
    resource_markdown_cells = _row_from_markdown(resource_markdown_row)
    assert resource_markdown_cells["Node Count"] == "2"
    assert resource_markdown_cells["First Node"] == "-"

    resource_table_row = next(
        (line for line in rendered.splitlines() if "resource_only" in line),
        None,
    )
    assert resource_table_row is not None
    resource_table_cells = _row_from_rich(resource_table_row)
    assert resource_table_cells["Node Count"] == "2"
    assert resource_table_cells["First Node"] == "-"



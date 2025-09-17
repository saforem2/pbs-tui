from datetime import datetime, timezone

import re

import pytest

from rich.console import Console

from pbs_tui.app import (
    PBSTUI,
    _env_flag,
    format_job_table_cells,
    run,
    snapshot_to_markdown,
    snapshot_to_table,
)
from pbs_tui.data import SchedulerSnapshot
from pbs_tui.samples import sample_snapshot
from pbs_tui.ui_config import JOB_TABLE_COLUMNS

from .util import add_jobs, make_job


NOW = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


COLUMN_NAMES = [name for name, _ in JOB_TABLE_COLUMNS]


def _row_from_rich(line: str) -> dict[str, str]:
    cells = [cell.strip() for cell in re.split(r"\s{2,}", line.strip()) if cell.strip()]
    assert len(cells) == len(COLUMN_NAMES), f"Unexpected cell count in line: {line}"
    return dict(zip(COLUMN_NAMES, cells))


def _row_from_markdown(line: str) -> dict[str, str]:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    assert len(cells) == len(COLUMN_NAMES), f"Unexpected cell count in markdown row: {line}"
    return dict(zip(COLUMN_NAMES, cells))


def _find_markdown_row(markdown: str, job_id: str) -> dict[str, str]:
    row = next((line for line in markdown.splitlines() if f"| {job_id} |" in line), None)
    assert row is not None, f"Expected markdown row for job {job_id}"
    return _row_from_markdown(row)


def _find_rich_row(rendered: str, job_id: str) -> dict[str, str]:
    row = next((line for line in rendered.splitlines() if job_id in line), None)
    assert row is not None, f"Expected rich row for job {job_id}"
    return _row_from_rich(row)


def _assert_snapshot_row(
    markdown: str,
    rendered: str,
    job_id: str,
    *,
    node_count: str,
    first_node: str,
) -> None:
    markdown_cells = _find_markdown_row(markdown, job_id)
    assert markdown_cells["Node Count"] == node_count
    assert markdown_cells["First Node"] == first_node

    table_cells = _find_rich_row(rendered, job_id)
    assert table_cells["Node Count"] == node_count
    assert table_cells["First Node"] == first_node


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


def _inline_output(monkeypatch, capsys, *job_overrides: dict) -> tuple[str, str]:
    snapshot = sample_snapshot(now=NOW)
    if job_overrides:
        add_jobs(snapshot, *job_overrides)

    class InlineFetcher:
        async def fetch_snapshot(self):
            return snapshot

    monkeypatch.delenv("PBS_TUI_HEADLESS", raising=False)
    monkeypatch.delenv("PBS_TUI_AUTOPILOT", raising=False)

    run(argv=["--inline"], fetcher=InlineFetcher())
    captured = capsys.readouterr()
    return captured.out, captured.err


def test_format_job_table_cells_matches_columns():
    job = make_job()
    cells = format_job_table_cells(job, NOW)
    assert len(cells) == len(JOB_TABLE_COLUMNS)


def test_run_inline_displays_sample_job_data(monkeypatch, capsys):
    out, err = _inline_output(monkeypatch, capsys)
    assert "PBS Jobs as of" in out
    assert "Job ID" in out
    assert "Node Count" in out
    assert "climate_model" in out
    assert "sample data" in err.lower()

    job_line = next((line for line in out.splitlines() if "104829.aqua" in line), None)
    assert job_line is not None, "Expected climate_model job row in inline output"
    row = _row_from_rich(job_line)
    assert row["Node Count"] == "1"
    assert row["First Node"] == "nid000001"


def test_run_inline_handles_multi_node_job(monkeypatch, capsys):
    out, _ = _inline_output(
        monkeypatch,
        capsys,
        dict(
            id="multi.123",
            name="multi",
            state="R",
            exec_host="nodeA/0+nodeB/1",
            nodes="nodeA+nodeB",
        ),
    )

    multi_line = next((line for line in out.splitlines() if "multi.123" in line), None)
    assert multi_line is not None, "Expected multi-node job row in inline output"
    multi_row = _row_from_rich(multi_line)
    assert multi_row["Node Count"] == "2"
    assert multi_row["First Node"] == "nodeA"


def test_run_inline_handles_bracketed_exec_host(monkeypatch, capsys):
    out, _ = _inline_output(
        monkeypatch,
        capsys,
        dict(
            id="bracket_exec.001",
            name="bracket_exec",
            state="R",
            exec_host="node[01-03]/0",
            nodes="node[01-03]",
        ),
    )

    bracket_line = next((line for line in out.splitlines() if "bracket_exec" in line), None)
    assert bracket_line is not None, "Expected bracketed exec_host row in inline output"
    bracket_row = _row_from_rich(bracket_line)
    assert bracket_row["Node Count"] == "3"
    assert bracket_row["First Node"] == "node01"


def test_run_inline_shows_placeholders_for_missing_nodes(monkeypatch, capsys):
    out, _ = _inline_output(
        monkeypatch,
        capsys,
        dict(
            id="missing_nodes.456",
            name="missing_nodes",
            state="Q",
            exec_host=None,
            nodes=None,
        ),
    )

    missing_line = next((line for line in out.splitlines() if "missing_no" in line), None)
    assert missing_line is not None, "Expected missing-nodes row in inline output"
    missing_row = _row_from_rich(missing_line)
    assert missing_row["Node Count"] == "-"
    assert missing_row["First Node"] == "-"


def test_run_inline_handles_malformed_nodes(monkeypatch, capsys):
    out, _ = _inline_output(
        monkeypatch,
        capsys,
        dict(
            id="malformed_nodes.789",
            name="malformed_nodes",
            state="Q",
            exec_host=None,
            nodes="!!!",
        ),
    )

    malformed_line = next((line for line in out.splitlines() if "malformed_" in line), None)
    assert malformed_line is not None, "Expected malformed-nodes row in inline output"
    malformed_row = _row_from_rich(malformed_line)
    assert malformed_row["Nodes"] == "!!!"
    assert malformed_row["Node Count"] == "-"
    assert malformed_row["First Node"] == "-"


def test_run_inline_prefers_exec_host_over_nodes(monkeypatch, capsys):
    out, _ = _inline_output(
        monkeypatch,
        capsys,
        dict(
            id="conflict.456",
            name="conflict_job",
            state="R",
            exec_host="nodeX/0+nodeY/1",
            nodes="nodeA+nodeB",
        ),
    )

    conflict_line = next((line for line in out.splitlines() if "conflict_job" in line), None)
    assert conflict_line is not None, "Expected conflicting exec_host row in inline output"
    conflict_row = _row_from_rich(conflict_line)
    assert conflict_row["Node Count"] == "2"
    assert conflict_row["First Node"] == "nodeX"


def test_run_inline_exec_host_nodes_count_disagreement(monkeypatch, capsys):
    out, _ = _inline_output(
        monkeypatch,
        capsys,
        dict(
            id="disagree.789",
            name="disagree_job",
            state="R",
            exec_host="nodeA/0+nodeB/1+nodeC/2",
            nodes="nodeA+nodeB",
        ),
    )

    disagree_line = next((line for line in out.splitlines() if "disagree_job" in line), None)
    assert disagree_line is not None, "Expected disagreement row in inline output"
    disagree_row = _row_from_rich(disagree_line)
    assert disagree_row["Node Count"] == "3"
    assert disagree_row["First Node"] == "nodeA"


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
    table = snapshot_to_table(snapshot)
    console = Console(record=True, width=120)
    console.print(table)
    rendered = console.export_text()

    _assert_snapshot_row(markdown, rendered, "no_nodes", node_count="-", first_node="-")
    _assert_snapshot_row(
        markdown, rendered, "resource_only", node_count="2", first_node="-"
    )


def test_snapshot_outputs_handle_malformed_resource_specs():
    malformed_jobs = [
        make_job(
            id="malformed_1",
            name="malformed_1",
            nodes=None,
            resources_requested={"select": "not_a_number:ncpus=abc"},
        ),
        make_job(id="malformed_2", name="malformed_2", nodes=None, resources_requested={"select": ""}),
        make_job(id="malformed_3", name="malformed_3", nodes=None, resources_requested={"select": None}),
    ]
    snapshot = SchedulerSnapshot(jobs=malformed_jobs, source="test")

    markdown = snapshot_to_markdown(snapshot)
    table = snapshot_to_table(snapshot)
    console = Console(record=True, width=120)
    console.print(table)
    rendered = console.export_text()

    expected = {
        "malformed_1": ("1", "-"),
        "malformed_2": ("-", "-"),
        "malformed_3": ("-", "-"),
    }

    for job in malformed_jobs:
        count, first = expected[job.id]
        _assert_snapshot_row(markdown, rendered, job.id, node_count=count, first_node=first)



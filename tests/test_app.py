from datetime import datetime, timezone

import pytest

from pbs_tui.app import PBSTUI, _env_flag, run, _job_node_summary
from pbs_tui.data import Job
from pbs_tui.samples import sample_snapshot


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


def test_run_inline_writes_markdown_file(monkeypatch, tmp_path, capsys):
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    snapshot = sample_snapshot(now=now)

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


def test_run_file_without_inline_exits(monkeypatch):
    class DummyFetcher:
        async def fetch_snapshot(self):  # pragma: no cover - should not run
            raise AssertionError("fetch_snapshot should not be called")

    monkeypatch.delenv("PBS_TUI_HEADLESS", raising=False)
    monkeypatch.delenv("PBS_TUI_AUTOPILOT", raising=False)

    with pytest.raises(SystemExit):
        run(argv=["--file", "out.md"], fetcher=DummyFetcher())


def test_job_node_summary_exec_host():
    job = Job(
        id="1",
        name="demo",
        user="alice",
        queue="work",
        state="R",
        exec_host="nodeA/0+nodeA/1+nodeB/0*2",
        nodes="2:ppn=1",
    )
    count, first = _job_node_summary(job)
    assert count == 2
    assert first == "nodeA"


def test_job_node_summary_requested_nodes():
    job = Job(
        id="2",
        name="demo",
        user="bob",
        queue="work",
        state="Q",
        nodes="node01+node02:ppn=2",
    )
    count, first = _job_node_summary(job)
    assert count == 2
    assert first == "node01"


def test_job_node_summary_numeric_fallback():
    job = Job(
        id="3",
        name="demo",
        user="carol",
        queue="work",
        state="Q",
        nodes="3:ppn=64",
        resources_requested={"nodes": "3:ppn=64"},
    )
    count, first = _job_node_summary(job)
    assert count == 3
    assert first is None


def test_job_node_summary_nodect_fallback():
    job = Job(
        id="4",
        name="demo",
        user="dave",
        queue="work",
        state="Q",
        resources_requested={"nodect": "5"},
    )
    count, first = _job_node_summary(job)
    assert count == 5
    assert first is None

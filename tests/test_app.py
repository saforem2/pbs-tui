from datetime import datetime, timezone

import pytest

from pbs_tui.app import PBSTUI, _env_flag, run, _job_node_summary
from pbs_tui.data import Job
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
    defaults.update(overrides)
    return Job(**defaults)


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
    job = make_job(
        state="R",
        exec_host="nodeA/0+nodeA/1+nodeB/0*2",
        nodes="2:ppn=1",
    )
    count, first = _job_node_summary(job)
    assert count == 2
    assert first == "nodeA"


def test_job_node_summary_exec_host_empty():
    job = make_job(exec_host="", nodes="node01+node02")
    count, first = _job_node_summary(job)
    assert count == 2
    assert first == "node01"


def test_job_node_summary_exec_host_duplicates():
    job = make_job(state="R", exec_host="nodeC/0+nodeC/0+nodeC/1")
    count, first = _job_node_summary(job)
    assert count == 1
    assert first == "nodeC"


def test_job_node_summary_exec_host_malformed_parts():
    job = make_job(state="R", exec_host="nodeD/0++nodeE/1*+  +/junk")
    count, first = _job_node_summary(job)
    assert count == 2
    assert first == "nodeD"


def test_job_node_summary_requested_nodes():
    job = make_job(nodes="node01+node02:ppn=2")
    count, first = _job_node_summary(job)
    assert count == 2
    assert first == "node01"


def test_job_node_summary_requested_nodes_numeric_only():
    job = make_job(nodes="2")
    count, first = _job_node_summary(job)
    assert count == 2
    assert first is None


def test_job_node_summary_requested_nodes_delimiter_only():
    job = make_job(nodes="++")
    count, first = _job_node_summary(job)
    assert count is None
    assert first is None


def test_job_node_summary_requested_nodes_mixed_numeric_named():
    job = make_job(nodes="2+nodeX")
    count, first = _job_node_summary(job)
    assert count == 3
    assert first == "nodeX"


def test_job_node_summary_requested_nodes_range_expression():
    job = make_job(nodes="node[01-03]")
    count, first = _job_node_summary(job)
    assert count == 3
    assert first == "node01"


def test_job_node_summary_numeric_fallback():
    job = make_job(nodes="3:ppn=64", resources_requested={"nodes": "3:ppn=64"})
    count, first = _job_node_summary(job)
    assert count == 3
    assert first is None


def test_job_node_summary_nodect_fallback():
    job = make_job(resources_requested={"nodect": "5"})
    count, first = _job_node_summary(job)
    assert count == 5
    assert first is None


def test_job_node_summary_multiple_fallback_candidates():
    job = make_job(resources_requested={"select": "2:ncpus=36", "nodect": "5"})
    count, first = _job_node_summary(job)
    assert count == 2
    assert first is None


def test_job_node_summary_missing_nodes():
    job = make_job()
    count, first = _job_node_summary(job)
    assert count is None
    assert first is None


def test_job_node_summary_blank_nodes():
    job = make_job(nodes="", resources_requested={})
    count, first = _job_node_summary(job)
    assert count is None
    assert first is None

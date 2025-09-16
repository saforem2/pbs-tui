from datetime import datetime, timezone

from pbs_tui.samples import sample_snapshot


def test_sample_snapshot_contents():
    now = datetime(2024, 5, 11, 12, 0, tzinfo=timezone.utc)
    snapshot = sample_snapshot(now=now)
    assert snapshot.timestamp == now
    assert snapshot.jobs and snapshot.nodes and snapshot.queues
    assert snapshot.source == "sample"
    assert any("sample data" in message.lower() for message in snapshot.errors)

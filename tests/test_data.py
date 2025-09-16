from datetime import datetime, timedelta, timezone

from pbs_tui.app import _format_datetime, _format_duration
from pbs_tui.data import Job


def test_job_runtime_handles_mixed_timezones():
    start = datetime(2024, 5, 10, 12, 0, tzinfo=timezone.utc)
    job = Job(
        id="1",
        name="job",
        user="user",
        queue="queue",
        state="R",
        start_time=start,
    )
    reference = datetime(2024, 5, 10, 13, 0)  # naive
    assert job.runtime(reference) == timedelta(hours=1)


def test_job_runtime_never_negative():
    start = datetime(2024, 5, 10, 12, 0, tzinfo=timezone.utc)
    job = Job(
        id="1",
        name="job",
        user="user",
        queue="queue",
        state="Q",
        start_time=start,
    )
    reference = datetime(2024, 5, 10, 11, 30, tzinfo=timezone.utc)
    assert job.runtime(reference) == timedelta(0)


def test_format_duration_variants():
    assert _format_duration(None) == "-"
    assert _format_duration(timedelta(hours=1, minutes=2, seconds=3)) == "01:02:03"
    assert _format_duration(timedelta(days=2, hours=3)) == "2d 03:00:00"
    assert _format_duration(timedelta(seconds=-30)) == "00:00:00"


def test_format_datetime_localises():
    aware = datetime(2024, 1, 5, 12, 34, 56, tzinfo=timezone.utc)
    result = _format_datetime(aware)
    assert "2024-01-05 12:34:56" in result
    assert result.endswith("UTC")
    assert _format_datetime(None) == "-"

"""Tests for pbs_tui.time_utils — shared duration parsing and formatting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pbs_tui.time_utils import format_remaining, parse_duration_spec, time_remaining
from tests.util import make_job


# ── parse_duration_spec ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, None, id="none"),
        pytest.param("", None, id="empty"),
        pytest.param("   ", None, id="whitespace"),
        pytest.param("3600", timedelta(seconds=3600), id="seconds-only"),
        pytest.param("30:00", timedelta(minutes=30), id="mm-ss"),
        pytest.param("01:30:00", timedelta(hours=1, minutes=30), id="hh-mm-ss"),
        pytest.param(
            "1:02:03:04",
            timedelta(days=1, hours=2, minutes=3, seconds=4),
            id="dd-hh-mm-ss",
        ),
        pytest.param(
            "2-12:00:00",
            timedelta(days=2, hours=12),
            id="day-prefix",
        ),
        pytest.param("abc", None, id="non-numeric"),
        pytest.param("1:2:3:4:5", None, id="too-many-parts"),
        pytest.param("-1:00:00", None, id="negative-day-prefix"),
        pytest.param("1:-2:00", None, id="negative-unit"),
    ],
)
def test_parse_duration_spec(value, expected):
    assert parse_duration_spec(value) == expected


# ── time_remaining ────────────────────────────────────────────────────


def test_time_remaining_with_walltime_and_start():
    ref = datetime(2024, 5, 11, 13, 0, tzinfo=timezone.utc)
    job = make_job(
        state="R",
        walltime="02:00:00",
        start_time=datetime(2024, 5, 11, 12, 0, tzinfo=timezone.utc),
    )
    result = time_remaining(job, ref)
    assert result == timedelta(hours=1)


def test_time_remaining_no_walltime():
    ref = datetime(2024, 5, 11, 13, 0, tzinfo=timezone.utc)
    job = make_job(state="R", walltime=None)
    assert time_remaining(job, ref) is None


def test_time_remaining_no_start_time():
    ref = datetime(2024, 5, 11, 13, 0, tzinfo=timezone.utc)
    job = make_job(state="R", walltime="01:00:00", start_time=None)
    result = time_remaining(job, ref)
    assert result == timedelta(hours=1)


def test_time_remaining_clamps_to_zero():
    ref = datetime(2024, 5, 11, 15, 0, tzinfo=timezone.utc)
    job = make_job(
        state="R",
        walltime="01:00:00",
        start_time=datetime(2024, 5, 11, 12, 0, tzinfo=timezone.utc),
    )
    result = time_remaining(job, ref)
    assert result == timedelta(0)


# ── format_remaining ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "td, expected",
    [
        pytest.param(None, "", id="none"),
        pytest.param(timedelta(0), "done", id="zero"),
        pytest.param(timedelta(seconds=-10), "done", id="negative"),
        pytest.param(timedelta(minutes=45), "45m", id="minutes"),
        pytest.param(timedelta(hours=2, minutes=5), "2h05m", id="hours-minutes"),
    ],
)
def test_format_remaining(td, expected):
    assert format_remaining(td) == expected

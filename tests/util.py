from __future__ import annotations

from pbs_tui.data import Job, SchedulerSnapshot


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

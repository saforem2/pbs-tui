"""Built-in fallback data for demonstrating the TUI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .data import Job, Node, Queue, SchedulerSnapshot


def sample_snapshot(now: datetime | None = None) -> SchedulerSnapshot:
    """Return a representative snapshot used when PBS commands are unavailable."""

    now = now or datetime.now(timezone.utc)
    jobs = [
        Job(
            id="104829.aqua",
            name="climate_model",
            user="aurora",
            queue="prod",
            state="R",
            exec_host="nid000001",
            create_time=now - timedelta(hours=3, minutes=15),
            start_time=now - timedelta(hours=2, minutes=5),
            walltime="04:00:00",
            nodes="2:ppn=128",
            resources_requested={"nodes": "2:ppn=128", "walltime": "04:00:00", "select": "2:ncpus=128"},
            resources_used={
                "walltime": "02:05:11",
                "cput": "04:10:22",
                "mem": "1200gb",
            },
        ),
        Job(
            id="104830.aqua",
            name="fom_analysis",
            user="zenith",
            queue="prod",
            state="Q",
            create_time=now - timedelta(minutes=45),
            walltime="01:00:00",
            nodes="1:ppn=64",
            resources_requested={"nodes": "1:ppn=64", "walltime": "01:00:00"},
        ),
        Job(
            id="104731.vesta",
            name="vis_debug",
            user="lattice",
            queue="debug",
            state="H",
            create_time=now - timedelta(hours=1, minutes=20),
            walltime="00:30:00",
            nodes="1:ppn=32",
            resources_requested={"nodes": "1:ppn=32", "walltime": "00:30:00"},
            comment="Awaiting data set availability",
        ),
    ]

    nodes = [
        Node(
            name="nid000001",
            state="job-exclusive",
            ncpus=128,
            properties=["has_ssd", "zen4"],
            jobs=["0/104829.aqua"],
            resources_available={"ncpus": "128", "ngpus": "4", "mem": "512gb"},
            resources_assigned={"ncpus": "128", "mem": "480gb"},
        ),
        Node(
            name="nid000002",
            state="free",
            ncpus=128,
            properties=["zen4"],
            resources_available={"ncpus": "128", "ngpus": "4", "mem": "512gb"},
        ),
        Node(
            name="nid000010",
            state="offline,down",
            ncpus=128,
            properties=["zen4", "maintenance"],
            comment="File system diagnostics in progress",
            resources_available={"ncpus": "128", "ngpus": "4", "mem": "512gb"},
        ),
    ]

    queues = [
        Queue(
            name="prod",
            enabled=True,
            started=True,
            total_jobs=2,
            job_states={"R": 1, "Q": 1},
            resources_default={"walltime": "02:00:00"},
            resources_max={"walltime": "24:00:00"},
            comment="Production queue for capability jobs",
        ),
        Queue(
            name="debug",
            enabled=True,
            started=True,
            total_jobs=1,
            job_states={"H": 1},
            resources_default={"walltime": "00:30:00"},
            comment="Interactive debugging queue",
        ),
        Queue(
            name="analysis",
            enabled=False,
            started=False,
            total_jobs=0,
            job_states={},
            comment="Queue paused for scheduled maintenance",
        ),
    ]

    snapshot = SchedulerSnapshot(
        jobs=jobs,
        nodes=nodes,
        queues=queues,
        timestamp=now,
        source="sample",
        errors=[
            "Displaying bundled sample data because PBS utilities were unavailable.",
        ],
    )
    return snapshot


__all__ = ["sample_snapshot"]

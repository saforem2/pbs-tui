"""Built-in fallback data for demonstrating the TUI.

Generates a realistic Aurora-like mock cluster with ~561 nodes, ~40 jobs,
and ~10 queues so the cluster grid and job tables are visually representative
when PBS commands are unavailable.
"""

from __future__ import annotations

import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from .data import Job, Node, Queue, SchedulerSnapshot

# ── constants ───────────────────────────────────────────────────────────

_USERS = [
    "janekim", "awu1018", "adillali", "culoncei", "foremans",
    "kevienzzq", "rama", "belssl", "yswang987", "sucheta",
    "mpuputt", "nawood", "jahn", "ychang4", "hsu_and",
]

_ACCOUNTS = [
    "climate_research", "materials_lab", "argonne_tpc", "FoundEnergy",
    "astro_sim", "bio_comp", "lattice_qcd", "13652", "cfd_group",
    "neural_arch", "fusion_eng", "genomics",
]

_JOB_NAMES = [
    "train_llm", "climate_sim", "mol_dynamics", "cfd_run", "lattice_qcd",
    "genome_align", "fusion_sim", "protein_fold", "astro_nbody", "quantum_chem",
    "diffusion_model", "weather_forecast", "particle_sim", "neural_ode",
    "monte_carlo", "ray_trace", "fluid_sim", "spin_glass", "turbulence",
    "band_structure", "phonon_calc", "dft_relax", "md_equilib", "gan_train",
    "reinforcement", "graph_neural", "cosmology", "plasma_sim", "opt_control",
    "rl_policy", "transformer", "vae_train", "flow_match", "pinn_solve",
]

_QUEUE_DEFS: List[Dict] = [
    {"name": "large", "walltime_max": "24:00:00", "comment": "Capability jobs (>= 64 nodes)"},
    {"name": "capacity", "walltime_max": "12:00:00", "comment": "General capacity queue"},
    {"name": "backfill-large", "walltime_max": "06:00:00", "comment": "Backfill for large jobs"},
    {"name": "backfill-medium", "walltime_max": "06:00:00", "comment": "Backfill for medium jobs"},
    {"name": "small", "walltime_max": "03:00:00", "comment": "Small jobs (<= 8 nodes)"},
    {"name": "medium", "walltime_max": "06:00:00", "comment": "Medium jobs (8-64 nodes)"},
    {"name": "debug", "walltime_max": "01:00:00", "comment": "Interactive debugging"},
    {"name": "debug-scaling", "walltime_max": "01:00:00", "comment": "Scaling tests"},
    {"name": "preemptable", "walltime_max": "72:00:00", "comment": "Low-priority preemptable"},
    {"name": "analysis", "walltime_max": "02:00:00", "comment": "Data analysis queue"},
]

# Running job specs: (queue, min_nodes, max_nodes)
_RUNNING_JOB_SPECS = [
    ("large", 100, 100),
    ("capacity", 40, 50),
    ("capacity", 20, 35),
    ("backfill-large", 25, 30),
    ("medium", 40, 60),
    ("medium", 15, 25),
    ("medium", 10, 20),
    ("small", 4, 8),
    ("small", 2, 5),
    ("small", 1, 3),
    ("small", 1, 2),
    ("debug", 1, 4),
    ("debug", 1, 2),
    ("debug-scaling", 2, 8),
    ("preemptable", 5, 15),
    ("preemptable", 2, 10),
    ("preemptable", 1, 4),
    ("backfill-medium", 8, 16),
    ("backfill-medium", 4, 8),
    ("small", 1, 1),
    ("small", 1, 1),
    ("debug", 1, 1),
]


# ── generator ───────────────────────────────────────────────────────────


def _fmt_walltime(td: timedelta) -> str:
    total = int(td.total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def sample_snapshot(now: datetime | None = None) -> SchedulerSnapshot:
    """Return a realistic mock cluster snapshot."""

    rng = random.Random(42)
    now = now or datetime.now(timezone.utc)

    # ── build all node names ────────────────────────────────────────
    # Aurora-style: racks x3001-x3016, x3101-x3112, x3201-x3212
    rack_ranges = [
        ("x30", 1, 16),
        ("x31", 1, 12),
        ("x32", 1, 12),
    ]
    all_node_names: List[str] = []
    for prefix, start, end in rack_ranges:
        for rack_num in range(start, end + 1):
            rack = f"{prefix}{rack_num:02d}"
            # Each rack has ~14 nodes
            for blade in range(14):
                all_node_names.append(f"{rack}-b{blade:02d}")

    # Trim to target size
    all_node_names = all_node_names[:561]
    rng.shuffle(all_node_names)

    # ── partition nodes by state ────────────────────────────────────
    n_offline = 15
    n_unknown = 6

    offline_nodes = set(all_node_names[:n_offline])
    unknown_nodes = set(all_node_names[n_offline : n_offline + n_unknown])
    available_nodes = all_node_names[n_offline + n_unknown :]

    # ── create running jobs and assign nodes ────────────────────────
    job_id_counter = 6800000
    jobs: List[Job] = []
    node_job_map: Dict[str, List[str]] = {}  # node_name → [job_id_spec]
    node_cursor = 0

    wt_hours = {"large": 24, "capacity": 12, "medium": 6, "small": 3,
                "debug": 1, "debug-scaling": 1, "preemptable": 72,
                "backfill-large": 6, "backfill-medium": 6, "analysis": 2}

    for spec_queue, min_n, max_n in _RUNNING_JOB_SPECS:
        nc = rng.randint(min_n, max_n)
        # Clamp to available nodes
        if node_cursor + nc > len(available_nodes):
            nc = len(available_nodes) - node_cursor
        if nc <= 0:
            break

        job_nodes = available_nodes[node_cursor : node_cursor + nc]
        node_cursor += nc

        job_id = f"{job_id_counter}.aurora-pbs-0001"
        job_id_counter += rng.randint(1, 50)

        user = rng.choice(_USERS)
        account = rng.choice(_ACCOUNTS)
        job_name = rng.choice(_JOB_NAMES)

        max_wt = wt_hours.get(spec_queue, 6)
        walltime_h = rng.uniform(0.5, max_wt)
        walltime_td = timedelta(hours=walltime_h)

        # Elapsed: 5% to 90% of walltime
        elapsed_frac = rng.uniform(0.05, 0.9)
        elapsed_td = timedelta(seconds=walltime_td.total_seconds() * elapsed_frac)
        start_time = now - elapsed_td

        exec_host = "+".join(f"{n}/0" for n in job_nodes)

        job = Job(
            id=job_id,
            name=f"{job_name}_{rng.randint(1, 999):03d}",
            user=user,
            queue=spec_queue,
            state="R",
            exec_host=exec_host,
            account=account,
            create_time=start_time - timedelta(minutes=rng.randint(5, 120)),
            queue_time=start_time - timedelta(minutes=rng.randint(1, 60)),
            eligible_time=start_time - timedelta(minutes=rng.randint(0, 30)),
            start_time=start_time,
            walltime=_fmt_walltime(walltime_td),
            nodes=f"{nc}:ppn=208",
            resources_requested={
                "nodect": str(nc),
                "walltime": _fmt_walltime(walltime_td),
                "select": f"{nc}:ncpus=208",
            },
            resources_used={
                "walltime": _fmt_walltime(elapsed_td),
                "cput": _fmt_walltime(elapsed_td * nc),
                "mem": f"{rng.randint(100, 480) * nc}gb",
            },
        )
        jobs.append(job)

        for n in job_nodes:
            node_job_map.setdefault(n, []).append(f"0/{job_id}")

    # Free nodes are the ones after the cursor
    free_nodes = set(available_nodes[node_cursor:])

    # ── create queued jobs ──────────────────────────────────────────
    queued_queues = ["large", "capacity", "medium", "small", "backfill-medium",
                     "backfill-large", "preemptable", "debug"]
    for _ in range(rng.randint(15, 22)):
        job_id_counter += rng.randint(1, 30)
        q = rng.choice(queued_queues)
        nc = rng.randint(1, 80)
        user = rng.choice(_USERS)
        max_wt = wt_hours.get(q, 6)
        walltime_td = timedelta(hours=rng.uniform(0.5, max_wt))
        queued_ago = timedelta(minutes=rng.randint(5, 600))

        est_start: Optional[datetime] = None
        if rng.random() < 0.5:
            est_start = now + timedelta(minutes=rng.randint(10, 480))

        job = Job(
            id=f"{job_id_counter}.aurora-pbs-0001",
            name=f"{rng.choice(_JOB_NAMES)}_{rng.randint(1, 999):03d}",
            user=user,
            queue=q,
            state="Q",
            account=rng.choice(_ACCOUNTS),
            create_time=now - queued_ago,
            queue_time=now - queued_ago,
            eligible_time=now - queued_ago + timedelta(minutes=rng.randint(0, 10)),
            estimated_start_time=est_start,
            walltime=_fmt_walltime(walltime_td),
            nodes=f"{nc}:ppn=208",
            resources_requested={
                "nodect": str(nc),
                "walltime": _fmt_walltime(walltime_td),
                "select": f"{nc}:ncpus=208",
            },
        )
        jobs.append(job)

    # ── create held jobs ────────────────────────────────────────────
    hold_comments = [
        "Awaiting dataset availability",
        "User hold — debugging input files",
        "System hold — quota exceeded",
        "Dependency on job 6799980",
        "Manual hold for review",
    ]
    for i in range(rng.randint(4, 7)):
        job_id_counter += rng.randint(1, 20)
        job = Job(
            id=f"{job_id_counter}.aurora-pbs-0001",
            name=f"{rng.choice(_JOB_NAMES)}_{rng.randint(1, 999):03d}",
            user=rng.choice(_USERS),
            queue=rng.choice(["debug", "analysis", "small"]),
            state="H",
            account=rng.choice(_ACCOUNTS),
            create_time=now - timedelta(hours=rng.randint(1, 48)),
            queue_time=now - timedelta(hours=rng.randint(1, 48)),
            walltime=_fmt_walltime(timedelta(hours=rng.uniform(0.5, 3))),
            nodes=f"{rng.randint(1, 4)}:ppn=208",
            resources_requested={"nodect": str(rng.randint(1, 4))},
            comment=hold_comments[i % len(hold_comments)],
        )
        jobs.append(job)

    # ── build nodes ─────────────────────────────────────────────────
    nodes: List[Node] = []
    for name in sorted(all_node_names):
        if name in offline_nodes:
            state = rng.choice(["offline", "offline,down"])
            comment = rng.choice([
                "File system diagnostics in progress",
                "Hardware maintenance scheduled",
                "GPU RMA pending",
                "Network card replacement",
            ])
            nodes.append(Node(
                name=name, state=state, ncpus=208,
                properties=["xe", "slingshot"],
                resources_available={"ncpus": "208", "ngpus": "12", "mem": "512gb"},
                comment=comment,
            ))
        elif name in unknown_nodes:
            nodes.append(Node(
                name=name, state="state-unknown", ncpus=208,
                properties=["xe", "slingshot"],
                resources_available={"ncpus": "208", "ngpus": "12", "mem": "512gb"},
            ))
        elif name in free_nodes:
            nodes.append(Node(
                name=name, state="free", ncpus=208,
                properties=["xe", "slingshot"],
                resources_available={"ncpus": "208", "ngpus": "12", "mem": "512gb"},
            ))
        else:
            # job-exclusive
            node_jobs = node_job_map.get(name, [])
            nodes.append(Node(
                name=name, state="job-exclusive", ncpus=208,
                properties=["xe", "slingshot"],
                jobs=node_jobs,
                resources_available={"ncpus": "208", "ngpus": "12", "mem": "512gb"},
                resources_assigned={"ncpus": "208", "mem": "480gb"},
            ))

    # ── build queues ────────────────────────────────────────────────
    # Tally job states per queue
    queue_job_states: Dict[str, Counter] = {}
    for job in jobs:
        queue_job_states.setdefault(job.queue, Counter())[job.state] += 1

    queues: List[Queue] = []
    for qdef in _QUEUE_DEFS:
        qname = qdef["name"]
        states = dict(queue_job_states.get(qname, {}))
        total = sum(states.values())
        queues.append(Queue(
            name=qname,
            enabled=True,
            started=qname != "analysis",
            total_jobs=total,
            job_states=states,
            resources_default={"walltime": "01:00:00"},
            resources_max={"walltime": qdef["walltime_max"]},
            comment=qdef["comment"],
        ))

    return SchedulerSnapshot(
        jobs=jobs,
        nodes=nodes,
        queues=queues,
        timestamp=now,
        source="sample",
        errors=[
            "Displaying bundled sample data because PBS utilities were unavailable.",
        ],
    )


__all__ = ["sample_snapshot"]

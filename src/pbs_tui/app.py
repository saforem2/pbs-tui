"""Textual application providing a PBS Pro dashboard."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import Counter
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterable, Optional, Sequence

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Static, TabPane, TabbedContent
from textual.pilot import Pilot

from .data import Job, Node, Queue, SchedulerSnapshot
from .fetcher import PBSDataFetcher

JOB_STATE_LABELS = {
    "B": "Begun",
    "E": "Exiting",
    "F": "Finished",
    "H": "Held",
    "Q": "Queued",
    "R": "Running",
    "S": "Suspended",
    "T": "Transit",
    "W": "Waiting",
}


def _sort_jobs_for_display(jobs: Iterable[Job]) -> list[Job]:
    return sorted(
        jobs,
        key=lambda job: (
            0 if job.state == "R" else 1,
            job.queue or "",
            job.id,
        ),
    )


def _format_bool(value: Optional[bool]) -> str:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    return "?"


def _format_duration(duration: Optional[timedelta]) -> str:
    if duration is None:
        return "-"
    total = int(duration.total_seconds())
    if total < 0:
        total = 0
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days:
        return f"{days}d {hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_datetime(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    try:
        local_value = value.astimezone()
    except ValueError:
        local_value = value
    return local_value.strftime("%Y-%m-%d %H:%M:%S %Z")


class SummaryWidget(Static):
    """Display aggregate scheduler information."""

    def update_from_snapshot(self, snapshot: SchedulerSnapshot) -> None:
        job_counts = Counter(job.state for job in snapshot.jobs if job.state)
        node_counts = Counter(node.primary_state() for node in snapshot.nodes)
        queue_enabled = sum(1 for queue in snapshot.queues if queue.enabled)
        queue_started = sum(1 for queue in snapshot.queues if queue.started)
        total_queues = len(snapshot.queues)

        job_table = Table.grid(padding=(0, 1))
        job_table.add_column(justify="left")
        job_table.add_row(Text(f"Total: {len(snapshot.jobs)}", style="bold"))
        if job_counts:
            for state, count in sorted(job_counts.items()):
                label = JOB_STATE_LABELS.get(state, state)
                job_table.add_row(f"{label}: {count}")
        else:
            job_table.add_row("No jobs")

        node_table = Table.grid(padding=(0, 1))
        node_table.add_column(justify="left")
        node_table.add_row(Text(f"Total: {len(snapshot.nodes)}", style="bold"))
        if node_counts:
            for state, count in sorted(node_counts.items()):
                node_table.add_row(f"{state}: {count}")
        else:
            node_table.add_row("No nodes")

        queue_table = Table.grid(padding=(0, 1))
        queue_table.add_column(justify="left")
        queue_table.add_row(Text(f"Total: {total_queues}", style="bold"))
        queue_table.add_row(f"Enabled: {queue_enabled}")
        queue_table.add_row(f"Started: {queue_started}")
        queue_job_counts = Counter()
        for queue in snapshot.queues:
            for state, count in queue.job_states.items():
                queue_job_counts[state] += count
        for state_code in ("R", "Q", "H"):
            if queue_job_counts.get(state_code):
                label = JOB_STATE_LABELS.get(state_code, state_code)
                queue_table.add_row(f"{label}: {queue_job_counts[state_code]}")

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column()
        grid.add_column()
        grid.add_row(
            Panel(job_table, title="Jobs", border_style="cyan"),
            Panel(node_table, title="Nodes", border_style="green"),
            Panel(queue_table, title="Queues", border_style="magenta"),
        )
        self.update(grid)


class StatusBar(Static):
    """Display status messages."""

    def update_status(self, message: str, *, severity: str = "info") -> None:
        if severity == "warning":
            text = Text(message, style="yellow")
        elif severity == "error":
            text = Text(message, style="red")
        else:
            text = Text(message)
        self.update(text)


class DetailPanel(Static):
    """Show details for the currently selected object."""

    def show_job(self, job: Job, *, reference_time: Optional[datetime] = None) -> None:
        table = Table.grid(padding=(0, 1))
        table.add_column(style="bold cyan", justify="right")
        table.add_column(justify="left")
        table.add_row("Job ID", job.id)
        table.add_row("Name", job.name)
        table.add_row("User", job.user)
        table.add_row("Queue", job.queue)
        table.add_row("State", JOB_STATE_LABELS.get(job.state, job.state))
        table.add_row("Exec host", job.exec_host or "-")
        table.add_row("Submitted", _format_datetime(job.create_time))
        table.add_row("Started", _format_datetime(job.start_time))
        table.add_row("Finished", _format_datetime(job.end_time))
        table.add_row("Runtime", _format_duration(job.runtime(reference_time)))
        table.add_row(
            "Requested",
            ", ".join(f"{k}={v}" for k, v in sorted(job.resources_requested.items())) or "-",
        )
        if job.resources_used:
            table.add_row(
                "Used",
                ", ".join(f"{k}={v}" for k, v in sorted(job.resources_used.items())),
            )
        table.add_row("Walltime", job.walltime or "-")
        table.add_row("Nodes", job.nodes or "-")
        table.add_row("Comment", job.comment or "-")
        table.add_row("Exit status", job.exit_status or "-")
        self.update(Panel(table, title=f"Job {job.id}", border_style="cyan"))

    def show_node(self, node: Node) -> None:
        table = Table.grid(padding=(0, 1))
        table.add_column(style="bold green", justify="right")
        table.add_column(justify="left")
        table.add_row("Node", node.name)
        table.add_row("State", node.state)
        table.add_row("NP", str(node.np) if node.np is not None else "-")
        table.add_row("CPUs", str(node.ncpus) if node.ncpus is not None else "-")
        table.add_row("Properties", ", ".join(node.properties) or "-")
        table.add_row("Jobs", ", ".join(node.jobs) or "-")
        if node.resources_available:
            table.add_row(
                "Available",
                ", ".join(f"{k}={v}" for k, v in sorted(node.resources_available.items())),
            )
        if node.resources_assigned:
            table.add_row(
                "Assigned",
                ", ".join(f"{k}={v}" for k, v in sorted(node.resources_assigned.items())),
            )
        table.add_row("Comment", node.comment or "-")
        self.update(Panel(table, title=f"Node {node.name}", border_style="green"))

    def show_queue(self, queue: Queue) -> None:
        table = Table.grid(padding=(0, 1))
        table.add_column(style="bold magenta", justify="right")
        table.add_column(justify="left")
        table.add_row("Queue", queue.name)
        table.add_row("Enabled", _format_bool(queue.enabled))
        table.add_row("Started", _format_bool(queue.started))
        table.add_row("Total jobs", str(queue.total_jobs) if queue.total_jobs is not None else "-")
        if queue.job_states:
            job_state_desc = ", ".join(
                f"{JOB_STATE_LABELS.get(state, state)}={count}"
                for state, count in sorted(queue.job_states.items())
            )
            table.add_row("States", job_state_desc)
        if queue.resources_default:
            table.add_row(
                "Default",
                ", ".join(f"{k}={v}" for k, v in sorted(queue.resources_default.items())),
            )
        if queue.resources_max:
            table.add_row(
                "Max",
                ", ".join(f"{k}={v}" for k, v in sorted(queue.resources_max.items())),
            )
        table.add_row("Comment", queue.comment or "-")
        self.update(Panel(table, title=f"Queue {queue.name}", border_style="magenta"))

    def show_message(self, message: str) -> None:
        self.update(Panel(Text(message), title="Details"))


class JobsTable(DataTable):
    """Data table displaying jobs."""

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.show_header = True
        self.add_columns("Job ID", "Name", "User", "Queue", "State", "Nodes", "Walltime", "Runtime")

    def update_jobs(self, jobs: Iterable[Job], reference_time: datetime) -> None:
        self.clear()
        for job in _sort_jobs_for_display(jobs):
            runtime = _format_duration(job.runtime(reference_time))
            self.add_row(
                job.id,
                job.name,
                job.user,
                job.queue,
                JOB_STATE_LABELS.get(job.state, job.state),
                job.nodes or "-",
                job.walltime or "-",
                runtime,
                key=job.id,
            )


class NodesTable(DataTable):
    """Data table displaying nodes."""

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.show_header = True
        self.add_columns("Node", "State", "CPUs", "Jobs", "Comment")

    def update_nodes(self, nodes: Iterable[Node]) -> None:
        self.clear()
        for node in sorted(nodes, key=lambda node: node.name or ""):
            job_count = str(len(node.jobs)) if node.jobs else "0"
            self.add_row(
                node.name,
                node.state,
                str(node.ncpus) if node.ncpus is not None else "-",
                job_count,
                node.comment or "-",
                key=node.name,
            )


class QueuesTable(DataTable):
    """Data table displaying queues."""

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.show_header = True
        self.add_columns("Queue", "Enabled", "Started", "Queued", "Running", "Held")

    def update_queues(self, queues: Iterable[Queue]) -> None:
        self.clear()
        for queue in sorted(queues, key=lambda queue: queue.name or ""):
            job_states = Counter(queue.job_states)
            queued = job_states.get("Q", 0) + job_states.get("W", 0) + job_states.get("T", 0)
            running = job_states.get("R", 0)
            held = job_states.get("H", 0) + job_states.get("S", 0)
            self.add_row(
                queue.name,
                _format_bool(queue.enabled),
                _format_bool(queue.started),
                str(queued),
                str(running),
                str(held),
                key=queue.name,
            )


class PBSTUI(App[None]):
    """Main Textual application class."""

    CSS_PATH = "app.tcss"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh now"),
        ("j", "focus_jobs", "Focus jobs"),
        ("n", "focus_nodes", "Focus nodes"),
        ("u", "focus_queues", "Focus queues"),
    ]

    refresh_interval: float = 30.0

    def __init__(
        self,
        *,
        fetcher: Optional[PBSDataFetcher] = None,
        refresh_interval: float = 30.0,
    ) -> None:
        super().__init__()
        self.fetcher = fetcher or PBSDataFetcher()
        self.refresh_interval = refresh_interval
        self._snapshot: Optional[SchedulerSnapshot] = None
        self._job_index: dict[str, Job] = {}
        self._node_index: dict[str, Node] = {}
        self._queue_index: dict[str, Queue] = {}
        self._refreshing: bool = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main"):
            with Vertical(id="left_panel"):
                yield SummaryWidget(id="summary")
                with TabbedContent(id="tabs"):
                    with TabPane("Jobs", id="jobs_tab"):
                        yield JobsTable(id="jobs_table")
                    with TabPane("Nodes", id="nodes_tab"):
                        yield NodesTable(id="nodes_table")
                    with TabPane("Queues", id="queues_tab"):
                        yield QueuesTable(id="queues_table")
            yield DetailPanel(id="details")
        yield StatusBar(id="status")
        yield Footer()

    async def on_mount(self) -> None:
        self.set_interval(self.refresh_interval, self.refresh_data)
        await self.refresh_data()

    async def refresh_data(self) -> None:
        if self._refreshing:
            return
        self._refreshing = True
        try:
            snapshot = await self.fetcher.fetch_snapshot()
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Failed to refresh PBS data: {exc}"
            self.query_one(StatusBar).update_status(message, severity="error")
            self.log.exception("Failed to refresh PBS data")
        else:
            self._snapshot = snapshot
            self._update_tables(snapshot)
            summary = self.query_one(SummaryWidget)
            summary.update_from_snapshot(snapshot)
            severity = "info"
            message_parts = [
                f"Last updated {snapshot.timestamp.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                f"Source: {snapshot.source}",
            ]
            if snapshot.errors:
                severity = "warning"
                message_parts.append("; ".join(snapshot.errors))
            status = self.query_one(StatusBar)
            status.update_status(" | ".join(message_parts), severity=severity)
            details = self.query_one(DetailPanel)
            if snapshot.jobs:
                details.show_job(snapshot.jobs[0], reference_time=snapshot.timestamp)
            elif snapshot.nodes:
                details.show_node(snapshot.nodes[0])
            elif snapshot.queues:
                details.show_queue(snapshot.queues[0])
            else:
                details.show_message("No scheduler data available")
        finally:
            self._refreshing = False

    def _update_tables(self, snapshot: SchedulerSnapshot) -> None:
        jobs_table = self.query_one(JobsTable)
        nodes_table = self.query_one(NodesTable)
        queues_table = self.query_one(QueuesTable)
        jobs_table.update_jobs(snapshot.jobs, snapshot.timestamp)
        nodes_table.update_nodes(snapshot.nodes)
        queues_table.update_queues(snapshot.queues)
        self._job_index = {job.id: job for job in snapshot.jobs}
        self._node_index = {node.name: node for node in snapshot.nodes}
        self._queue_index = {queue.name: queue for queue in snapshot.queues}

    async def action_refresh(self) -> None:
        await self.refresh_data()

    def action_focus_jobs(self) -> None:
        self.query_one(TabbedContent).active = "jobs_tab"
        self.query_one(JobsTable).focus()

    def action_focus_nodes(self) -> None:
        self.query_one(TabbedContent).active = "nodes_tab"
        self.query_one(NodesTable).focus()

    def action_focus_queues(self) -> None:
        self.query_one(TabbedContent).active = "queues_tab"
        self.query_one(QueuesTable).focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if isinstance(event.data_table, JobsTable):
            job_id = str(event.row_key.value if hasattr(event.row_key, "value") else event.row_key)
            if self._snapshot is None:
                return
            job = self._job_index.get(job_id)
            if job:
                self.query_one(DetailPanel).show_job(
                    job, reference_time=self._snapshot.timestamp
                )
        elif isinstance(event.data_table, NodesTable):
            node_name = str(event.row_key.value if hasattr(event.row_key, "value") else event.row_key)
            if self._snapshot is None:
                return
            node = self._node_index.get(node_name)
            if node:
                self.query_one(DetailPanel).show_node(node)
        elif isinstance(event.data_table, QueuesTable):
            queue_name = str(event.row_key.value if hasattr(event.row_key, "value") else event.row_key)
            if self._snapshot is None:
                return
            queue = self._queue_index.get(queue_name)
            if queue:
                self.query_one(DetailPanel).show_queue(queue)


def _escape_markdown_cell(text: str) -> str:
    cleaned = text.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
    return cleaned.strip()


def _markdown_cell(value: Optional[str]) -> str:
    if value is None:
        return "-"
    text = str(value)
    if not text.strip():
        return "-"
    return _escape_markdown_cell(text)


def _table_cell(value: Optional[str]) -> str:
    if value is None:
        return "-"
    text = str(value)
    if not text.strip():
        return "-"
    return text


def snapshot_to_markdown(snapshot: SchedulerSnapshot) -> str:
    """Return a Markdown table describing the jobs in *snapshot*."""

    if snapshot.timestamp:
        try:
            timestamp = snapshot.timestamp.astimezone()
        except ValueError:
            timestamp = snapshot.timestamp
    else:
        timestamp = datetime.now()
    lines = [
        f"### PBS Jobs as of {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"*Source*: {snapshot.source}",
        "",
    ]
    headers = ["Job ID", "Name", "User", "Queue", "State", "Nodes", "Walltime", "Runtime"]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    reference_time = snapshot.timestamp or datetime.now()
    if snapshot.jobs:
        for job in _sort_jobs_for_display(snapshot.jobs):
            runtime = _format_duration(job.runtime(reference_time))
            row = [
                job.id,
                job.name,
                job.user,
                job.queue,
                JOB_STATE_LABELS.get(job.state, job.state),
                job.nodes or "-",
                job.walltime or "-",
                runtime,
            ]
            lines.append("| " + " | ".join(_markdown_cell(cell) for cell in row) + " |")
    else:
        empty_row = ["_No jobs available_"] + [""] * (len(headers) - 1)
        lines.append("| " + " | ".join(empty_row) + " |")
    if snapshot.errors:
        lines.append("")
        for error in snapshot.errors:
            lines.append(f"> {error}")
    return "\n".join(lines)


def snapshot_to_table(snapshot: SchedulerSnapshot) -> Table:
    """Return a Rich table describing the jobs in *snapshot*."""

    if snapshot.timestamp:
        try:
            timestamp = snapshot.timestamp.astimezone()
        except ValueError:
            timestamp = snapshot.timestamp
    else:
        timestamp = datetime.now()

    table = Table(
        title=f"PBS Jobs as of {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        caption=f"Source: {snapshot.source}",
        box=box.SIMPLE_HEAVY,
        highlight=True,
    )
    headers = [
        ("Job ID", "left"),
        ("Name", "left"),
        ("User", "left"),
        ("Queue", "left"),
        ("State", "left"),
        ("Nodes", "left"),
        ("Walltime", "left"),
        ("Runtime", "left"),
    ]
    for header, justify in headers:
        table.add_column(header, justify=justify)

    reference_time = snapshot.timestamp or datetime.now()
    if snapshot.jobs:
        for job in _sort_jobs_for_display(snapshot.jobs):
            table.add_row(
                _table_cell(job.id),
                _table_cell(job.name),
                _table_cell(job.user),
                _table_cell(job.queue),
                _table_cell(JOB_STATE_LABELS.get(job.state, job.state)),
                _table_cell(job.nodes or "-"),
                _table_cell(job.walltime or "-"),
                _table_cell(_format_duration(job.runtime(reference_time))),
            )
    else:
        table.add_row(
            "No jobs available",
            *[""] * (len(headers) - 1),
            style="italic",
        )
    return table


def _env_flag(name: str) -> bool:
    """Return ``True`` when *name* is set to a truthy value."""

    value = os.getenv(name)
    if value is None:
        return False
    return value.strip().lower() not in {"", "0", "false", "no"}


def run(
    argv: Optional[Sequence[str]] = None,
    *,
    fetcher: Optional[PBSDataFetcher] = None,
) -> None:
    """Entry point used by the ``pbs-tui`` console script."""

    parser = argparse.ArgumentParser(description="PBS Pro scheduler dashboard")
    parser.add_argument(
        "--inline",
        action="store_true",
        help="Fetch PBS data once and print a Rich table instead of starting the TUI.",
    )
    parser.add_argument(
        "--file",
        type=Path,
        metavar="PATH",
        help="With --inline, write a Markdown snapshot to PATH.",
    )
    parser.add_argument(
        "--refresh-interval",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="How often the TUI refreshes PBS data (default: 30).",
    )
    args = parser.parse_args(argv)

    fetcher_instance = fetcher or PBSDataFetcher()

    if args.file and not args.inline:
        parser.error("--file can only be used together with --inline")

    if args.inline:
        snapshot = asyncio.run(fetcher_instance.fetch_snapshot())
        console = Console()
        console.print(snapshot_to_table(snapshot))
        if args.file:
            args.file.write_text(snapshot_to_markdown(snapshot) + "\n")
        if snapshot.errors:
            for message in snapshot.errors:
                print(message, file=sys.stderr)
        return

    headless = _env_flag("PBS_TUI_HEADLESS")
    auto_pilot = None
    auto_flag = os.getenv("PBS_TUI_AUTOPILOT", "").strip().lower()

    if auto_flag in {"quit", "exit"}:

        async def _auto_quit(pilot: Pilot) -> None:
            await pilot.pause(0.1)
            await pilot.press("q")

        auto_pilot = _auto_quit

    app = PBSTUI(fetcher=fetcher_instance, refresh_interval=args.refresh_interval)
    app.run(headless=headless, auto_pilot=auto_pilot)


__all__ = ["PBSTUI", "run"]

"""Textual application providing a PBS Pro dashboard."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import Counter
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult, SystemCommand
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Markdown,
    Static,
    TabPane,
    TabbedContent,
)
from textual.pilot import Pilot

from .data import Job, Node, Queue, SchedulerSnapshot
from .fetcher import PBSDataFetcher
from .nodes import (
    extract_exec_host_nodes,
    extract_requested_nodes,
    first_requested_node,
    parse_node_count_spec,
)
from .ui_config import JOB_TABLE_COLUMNS

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

    if value.tzinfo is None:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    try:
        normalized = value.astimezone(timezone.utc)
    except ValueError:
        normalized = value
    return normalized.strftime("%Y-%m-%d %H:%M:%S %Z")


def _parse_duration_spec(value: Optional[str]) -> Optional[timedelta]:
    if value is None:
        return None
    spec = value.strip()
    if not spec:
        return None
    days = 0
    time_part = spec
    if "-" in spec:
        day_part, remainder = spec.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            return None
        time_part = remainder
    parts = time_part.split(":")
    try:
        units = [int(part) for part in parts]
    except ValueError:
        return None
    if len(units) > 4:
        return None
    if len(units) == 4:
        days += units[0]
        hours, minutes, seconds = units[1:]
    elif len(units) == 3:
        hours, minutes, seconds = units
    elif len(units) == 2:
        hours, minutes, seconds = 0, units[0], units[1]
    elif len(units) == 1:
        hours, minutes, seconds = 0, 0, units[0]
    else:
        return None
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def _job_runtime_delta(job: Job, reference_time: datetime) -> Optional[timedelta]:
    runtime_reference = job.end_time or reference_time
    return job.runtime(runtime_reference)


def _job_queue_duration(job: Job, reference_time: datetime) -> Optional[timedelta]:
    start = job.queue_time or job.create_time
    if start is None:
        return None
    end = job.start_time or reference_time
    if start.tzinfo and not end.tzinfo:
        end = end.replace(tzinfo=start.tzinfo)
    elif end.tzinfo and not start.tzinfo:
        start = start.replace(tzinfo=end.tzinfo)
    delta = end - start
    if delta.total_seconds() < 0:
        return timedelta(seconds=0)
    return delta


def _job_time_remaining(
    job: Job, reference_time: datetime, runtime: Optional[timedelta] = None
) -> Optional[timedelta]:
    walltime = _parse_duration_spec(job.walltime)
    if walltime is None:
        return None
    runtime = runtime if runtime is not None else _job_runtime_delta(job, reference_time)
    if runtime is None:
        return walltime
    remaining = walltime - runtime
    if remaining.total_seconds() < 0:
        return timedelta(seconds=0)
    return remaining
def job_node_summary(job: Job) -> tuple[Optional[int], Optional[str]]:
    """Return a heuristic ``(node_count, first_node)`` tuple for *job*.

    The summary prefers concrete execution host assignments before falling back
    to requested node specifications and resource metadata published with the
    job.  This keeps UI code focused on presentation while centralising the
    parsing heuristics in :mod:`pbs_tui.nodes`.
    """

    if exec_nodes := extract_exec_host_nodes(job.exec_host):
        first_exec = next((node for node in exec_nodes if not node.isdigit()), exec_nodes[0])
        return len(exec_nodes), first_exec

    first_node = first_requested_node(job.nodes)

    if (count := parse_node_count_spec(job.nodes)) is not None:
        return count, first_node

    if requested_nodes := extract_requested_nodes(job.nodes):
        return len(requested_nodes), first_node

    for key in ("select", "nodes", "nodect"):
        if (count := parse_node_count_spec(job.resources_requested.get(key))) is not None:
            return count, first_node
    return None, None


def format_job_table_cells(job: Job, reference_time: datetime) -> dict[str, Optional[str]]:
    """Return column-keyed cell values for job table renderers."""

    node_count, first_node = job_node_summary(job)
    runtime_delta = _job_runtime_delta(job, reference_time)
    queued_duration = _job_queue_duration(job, reference_time)
    time_remaining = _job_time_remaining(job, reference_time, runtime_delta)
    est_start = job.estimated_start_time or job.start_time
    if node_count is not None and first_node:
        nodes_display = f"{node_count} ({first_node})"
    elif node_count is not None:
        nodes_display = str(node_count)
    else:
        nodes_display = first_node
    raw_values: dict[str, Optional[str]] = {
        "#JobId": job.id.split(".", 1)[0],
        "User": job.user,
        "Account": job.account,
        "JobName": job.name,
        "WallTime": job.walltime,
        "QueuedTime": _format_duration(queued_duration),
        "EstStart": _format_datetime(est_start),
        "RunTime": _format_duration(runtime_delta),
        "TimeRemaining": _format_duration(time_remaining),
        "Nodes": nodes_display,
        "State": JOB_STATE_LABELS.get(job.state, job.state),
        "Queue": job.queue,
    }
    ordered = {label: raw_values[label] for label, _ in JOB_TABLE_COLUMNS}
    assert len(ordered) == len(JOB_TABLE_COLUMNS), "job table cells must match column metadata"
    return ordered


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

    can_focus = True

    def hide(self) -> None:
        """Hide the panel and clear any existing content."""

        self.display = False
        self.update("")

    def show_job(self, job: Job, *, reference_time: Optional[datetime] = None) -> None:
        self.display = True
        effective_reference = reference_time or datetime.now(timezone.utc)
        runtime_delta = _job_runtime_delta(job, effective_reference)
        queued_duration = _job_queue_duration(job, effective_reference)
        remaining = _job_time_remaining(job, effective_reference, runtime_delta)
        node_count, first_node = job_node_summary(job)

        table = Table.grid(padding=(0, 1))
        table.add_column(style="bold cyan", justify="right")
        table.add_column(justify="left")

        def add_row(label: str, value: Optional[str]) -> None:
            if value is None:
                display = "-"
            else:
                display = value if value.strip() else "-"
            table.add_row(label, display)

        add_row("Job ID", job.id)
        add_row("Job name", job.name)
        add_row("User", job.user)
        add_row("Account", job.account)
        add_row("Queue", job.queue)
        add_row("State", JOB_STATE_LABELS.get(job.state, job.state))
        add_row("Exec host", job.exec_host)

        summary_parts: list[str] = []
        if node_count is not None:
            label = "node" if node_count == 1 else "nodes"
            summary_parts.append(f"{node_count} {label}")
        if first_node:
            summary_parts.append(f"first: {first_node}")
        add_row("Node summary", ", ".join(summary_parts) if summary_parts else None)

        add_row("Requested nodes", job.nodes)
        add_row("Walltime", job.walltime)
        add_row("Runtime", _format_duration(runtime_delta))
        add_row("Time remaining", _format_duration(remaining))
        add_row("Queued time", _format_duration(queued_duration))
        add_row("Submitted", _format_datetime(job.create_time))
        add_row("Queue time", _format_datetime(job.queue_time))
        add_row("Eligible", _format_datetime(job.eligible_time))
        add_row("Estimated start", _format_datetime(job.estimated_start_time))
        add_row("Started", _format_datetime(job.start_time))
        add_row("Finished", _format_datetime(job.end_time))

        location_parts: list[str] = []
        if job.location:
            location_parts.append(job.location)
        if job.comment:
            location_parts.append(job.comment)
        location_text = "\n".join(location_parts) if location_parts else None
        add_row("Location / Comments", location_text)

        add_row("Exit status", job.exit_status)

        if job.resources_requested:
            requested = ", ".join(
                f"{key}={value}" for key, value in sorted(job.resources_requested.items())
            )
            add_row("Resources requested", requested)
        if job.resources_used:
            used = ", ".join(
                f"{key}={value}" for key, value in sorted(job.resources_used.items())
            )
            add_row("Resources used", used)

        self.update(Panel(table, title=f"Job {job.id}", border_style="cyan"))

    def show_node(self, node: Node) -> None:
        self.display = True
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
        self.display = True
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
        self.display = True
        self.update(Panel(Text(message), title="Details"))


class JobsTable(DataTable):
    """Data table displaying jobs."""

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.show_header = True
        self.add_columns(*(label for label, _ in JOB_TABLE_COLUMNS))

    def update_jobs(self, jobs: Iterable[Job], reference_time: datetime) -> None:
        self.clear()
        for job in _sort_jobs_for_display(jobs):
            cells = format_job_table_cells(job, reference_time)
            ordered = [cells[label] for label, _ in JOB_TABLE_COLUMNS]
            self.add_row(*(_format_cell_value(value) for value in ordered), key=job.id)


class HelpPanel(Static):
    """Display application help and key bindings."""

    HELP_TEXT = """
# PBS TUI Help

This dashboard provides a quick overview of the PBS scheduler state.

## Key bindings

- **q**: Quit the application
- **r**: Refresh scheduler data
- **j**: Focus the Jobs tab
- **n**: Focus the Nodes tab
- **u**: Focus the Queues tab
- **Ctrl+P**: Toggle the command palette

## Jobs table filtering

Use the **Filter jobs** input above the Jobs table to narrow results. Type one or
more search terms; each term must match a value from any column. For example,
enter `running` to show only running jobs or `alice gpu` to show jobs that match
both terms across all columns.

## Details panel

Selecting a row from any table populates the details panel on the right. When
there is more information than fits on screen, the panel becomes scrollable.
"""

    def on_mount(self) -> None:  # pragma: no cover - static content
        self.mount(Markdown(self.HELP_TEXT))


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
        ("ctrl+p", "command_palette", "Command palette"),
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
        self._job_filter: str = ""
        self._selected_job_id: Optional[str] = None
        self._selected_node_name: Optional[str] = None
        self._selected_queue_name: Optional[str] = None
        self._detail_source: Optional[str] = None
        self._detail_panel_enabled: bool = True

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main"):
            with Vertical(id="left_panel"):
                yield SummaryWidget(id="summary")
                with TabbedContent(id="tabs"):
                    with TabPane("Jobs", id="jobs_tab"):
                        with Vertical(id="jobs_tab_content"):
                            yield Input(
                                placeholder="Filter jobs…",
                                id="jobs_filter",
                            )
                            yield JobsTable(id="jobs_table")
                    with TabPane("Nodes", id="nodes_tab"):
                        yield NodesTable(id="nodes_table")
                    with TabPane("Queues", id="queues_tab"):
                        yield QueuesTable(id="queues_table")
                    with TabPane("Help", id="help_tab"):
                        yield HelpPanel(id="help_panel")
            yield DetailPanel(id="details")
        yield StatusBar(id="status")
        yield Footer()

    async def on_mount(self) -> None:
        self.set_interval(self.refresh_interval, self.refresh_data)
        await self.refresh_data()
        self.query_one(DetailPanel).hide()

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
        finally:
            self._refreshing = False

    def _update_tables(self, snapshot: SchedulerSnapshot) -> None:
        nodes_table = self.query_one(NodesTable)
        queues_table = self.query_one(QueuesTable)
        self._refresh_jobs_table()
        nodes_table.update_nodes(snapshot.nodes)
        queues_table.update_queues(snapshot.queues)
        self._job_index = {job.id: job for job in snapshot.jobs}
        self._node_index = {node.name: node for node in snapshot.nodes}
        self._queue_index = {queue.name: queue for queue in snapshot.queues}
        self._update_detail_panel(reference_time=snapshot.timestamp)

    def _refresh_jobs_table(self) -> None:
        jobs_table = self.query_one(JobsTable)
        if self._snapshot is None:
            return
        reference_time = self._snapshot.timestamp or datetime.now()
        filtered_jobs = self._get_filtered_jobs(reference_time)
        jobs_table.update_jobs(filtered_jobs, reference_time)
        if self._selected_job_id:
            selected_job = next(
                (job for job in filtered_jobs if job.id == self._selected_job_id),
                None,
            )
            if selected_job:
                # Ensure we keep the stored id for later refreshes.
                self._selected_job_id = selected_job.id
            else:
                self._selected_job_id = None
                if self._detail_source == "job":
                    self._detail_source = None
                    self.query_one(DetailPanel).hide()
        elif self._detail_source == "job":
            self._detail_source = None
            self.query_one(DetailPanel).hide()
        self._update_detail_panel(reference_time=reference_time)

    def _get_filtered_jobs(self, reference_time: datetime) -> list[Job]:
        if self._snapshot is None:
            return []
        return [
            job
            for job in _sort_jobs_for_display(self._snapshot.jobs)
            if self._job_matches_filter(job, reference_time)
        ]

    def _job_matches_filter(self, job: Job, reference_time: datetime) -> bool:
        if not self._job_filter:
            return True
        terms = self._job_filter.lower().split()
        if not terms:
            return True
        cells = format_job_table_cells(job, reference_time)
        row_values = [value or "" for value in cells.values()]
        for term in terms:
            if not any(term in value.lower() for value in row_values):
                return False
        return True

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

    def action_toggle_detail_panel(self) -> None:
        """Toggle the visibility of the detail panel."""

        self._detail_panel_enabled = not self._detail_panel_enabled
        self._update_detail_panel(reference_time=self._snapshot.timestamp if self._snapshot else None)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "jobs_filter":
            self._job_filter = event.value.strip()
            self._refresh_jobs_table()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if isinstance(event.data_table, JobsTable):
            job_id = str(event.row_key.value if hasattr(event.row_key, "value") else event.row_key)
            if self._snapshot is None:
                return
            job = self._job_index.get(job_id)
            if job:
                self._selected_job_id = job.id
                self._selected_node_name = None
                self._selected_queue_name = None
                self._detail_source = "job"
                if self._detail_panel_enabled:
                    self.query_one(DetailPanel).show_job(
                        job, reference_time=self._snapshot.timestamp
                    )
                else:
                    self.query_one(DetailPanel).hide()
        elif isinstance(event.data_table, NodesTable):
            self._selected_job_id = None
            node_name = str(event.row_key.value if hasattr(event.row_key, "value") else event.row_key)
            if self._snapshot is None:
                return
            node = self._node_index.get(node_name)
            if node:
                self._selected_node_name = node.name
                self._selected_queue_name = None
                self._detail_source = "node"
                if self._detail_panel_enabled:
                    self.query_one(DetailPanel).show_node(node)
                else:
                    self.query_one(DetailPanel).hide()
        elif isinstance(event.data_table, QueuesTable):
            self._selected_job_id = None
            queue_name = str(event.row_key.value if hasattr(event.row_key, "value") else event.row_key)
            if self._snapshot is None:
                return
            queue = self._queue_index.get(queue_name)
            if queue:
                self._selected_queue_name = queue.name
                self._selected_node_name = None
                self._detail_source = "queue"
                if self._detail_panel_enabled:
                    self.query_one(DetailPanel).show_queue(queue)
                else:
                    self.query_one(DetailPanel).hide()

    def _update_detail_panel(self, *, reference_time: Optional[datetime] = None) -> None:
        """Refresh the detail panel based on current selection and visibility."""

        details = self.query_one(DetailPanel)

        if not self._detail_panel_enabled:
            details.hide()
            return

        if self._detail_source == "job" and self._selected_job_id and self._snapshot:
            job = self._job_index.get(self._selected_job_id)
            if job:
                details.show_job(job, reference_time=reference_time or self._snapshot.timestamp)
                return
            self._selected_job_id = None
            self._detail_source = None
        elif self._detail_source == "node" and self._selected_node_name:
            node = self._node_index.get(self._selected_node_name)
            if node:
                details.show_node(node)
                return
            self._selected_node_name = None
            self._detail_source = None
        elif self._detail_source == "queue" and self._selected_queue_name:
            queue = self._queue_index.get(self._selected_queue_name)
            if queue:
                details.show_queue(queue)
                return
            self._selected_queue_name = None
            self._detail_source = None

        details.hide()

    def get_system_commands(self, screen: Screen) -> Iterable[SystemCommand]:
        yield from super().get_system_commands(screen)
        yield SystemCommand(
            "Toggle detail panel",
            "Show or hide the detail pane",
            self.action_toggle_detail_panel,
        )


def _escape_markdown_cell(text: str) -> str:
    cleaned = text.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
    return cleaned.strip()


def _format_cell_value(value: Optional[str]) -> str:
    text = "-" if value is None else str(value)
    return text if text.strip() else "-"


def _markdown_cell(value: Optional[str]) -> str:
    return _escape_markdown_cell(_format_cell_value(value))


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
    headers = [label for label, _ in JOB_TABLE_COLUMNS]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    reference_time = snapshot.timestamp or datetime.now()
    if snapshot.jobs:
        for job in _sort_jobs_for_display(snapshot.jobs):
            row = format_job_table_cells(job, reference_time)
            ordered = [row[label] for label, _ in JOB_TABLE_COLUMNS]
            lines.append("| " + " | ".join(_markdown_cell(cell) for cell in ordered) + " |")
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
    for header, justify in JOB_TABLE_COLUMNS:
        table.add_column(header, justify=justify)

    reference_time = snapshot.timestamp or datetime.now()
    if snapshot.jobs:
        for job in _sort_jobs_for_display(snapshot.jobs):
            row = format_job_table_cells(job, reference_time)
            ordered = [row[label] for label, _ in JOB_TABLE_COLUMNS]
            table.add_row(*(_format_cell_value(cell) for cell in ordered))
    else:
        table.add_row(
            "No jobs available",
            *[""] * (len(JOB_TABLE_COLUMNS) - 1),
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
        stdout_is_tty = sys.stdout.isatty()
        console = Console(
            force_terminal=not stdout_is_tty,
            color_system=None if not stdout_is_tty else None,
        )
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

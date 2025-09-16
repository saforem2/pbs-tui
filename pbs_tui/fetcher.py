"""Utilities for gathering PBS Pro scheduler information."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar
from xml.etree import ElementTree as ET

from .data import Job, Node, Queue, SchedulerSnapshot
from .samples import sample_snapshot

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


def _stringify(value: object) -> str:
    """Convert *value* into a human-readable string."""

    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, set)):
        parts = [segment for segment in (_stringify(item).strip() for item in value) if segment]
        return ", ".join(parts)
    return json.dumps(value, sort_keys=True)


def _flatten_mapping(mapping: Dict[str, object], prefix: str = "") -> Dict[str, str]:
    """Flatten nested dictionaries using dotted keys."""

    flat: Dict[str, str] = {}
    for raw_key, raw_value in mapping.items():
        key = f"{prefix}{raw_key}" if prefix else str(raw_key)
        if isinstance(raw_value, dict):
            flat.update(_flatten_mapping(raw_value, f"{key}."))
        else:
            flat[key] = _stringify(raw_value)
    return flat


def _extract_records(
    payload: object, options: Iterable[str]
) -> List[Tuple[Optional[str], Dict[str, object]]]:
    """Return ``(key, mapping)`` pairs matching any of *options* within *payload*."""

    records: List[Tuple[Optional[str], Dict[str, object]]] = []
    if isinstance(payload, dict):
        for candidate in options:
            if candidate in payload:
                value = payload[candidate]
                if isinstance(value, dict):
                    for key, item in value.items():
                        if isinstance(item, dict):
                            records.append((str(key), item))
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            records.append((None, item))
                if records:
                    return records
        for value in payload.values():
            nested = _extract_records(value, options)
            if nested:
                return nested
    return records


def _parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    value = value.strip().lower()
    if value in {"true", "t", "1", "yes", "y"}:
        return True
    if value in {"false", "f", "0", "no", "n"}:
        return False
    return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


_PBS_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%a %b %d %H:%M:%S %Y",
)


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.isdigit():
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        except (ValueError, OSError):
            return None
    for fmt in _PBS_DATETIME_FORMATS:
        try:
            if fmt.endswith("%z") or value.endswith("Z"):
                normalised = value.replace("Z", "+0000")
                return datetime.strptime(normalised, fmt)
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _collect_child_text(element: Optional[ET.Element]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if element is None:
        return result
    for child in element:
        key = child.tag
        text = child.text.strip() if child.text else ""
        result[key] = text
    return result


class PBSDataFetcher:
    """Fetch PBS Pro scheduler data via the command-line utilities."""

    def __init__(
        self,
        *,
        qstat_path: str = "qstat",
        pbsnodes_path: str = "pbsnodes",
        include_nodes: bool = True,
        include_queues: bool = True,
        command_timeout: float = 15.0,
        fallback_to_sample: bool = True,
        force_sample: Optional[bool] = None,
    ) -> None:
        env_force_sample = os.getenv("PBS_TUI_SAMPLE_DATA")
        if force_sample is None and env_force_sample is not None:
            force_sample = env_force_sample.strip().lower() not in {"0", "false", "no", ""}
        self.force_sample = bool(force_sample)
        self.qstat_path = qstat_path
        self.pbsnodes_path = pbsnodes_path
        self.include_nodes = include_nodes
        self.include_queues = include_queues
        self.command_timeout = command_timeout
        self.fallback_to_sample = fallback_to_sample
        self._qstat_jobs_json_cmd = [self.qstat_path, "-f", "-F", "json"]
        self._qstat_jobs_cmd = [self.qstat_path, "-f", "-x"]
        self._qstat_jobs_text_cmd = [self.qstat_path, "-f"]
        self._qstat_queue_json_cmd = [self.qstat_path, "-Q", "-f", "-F", "json"]
        self._qstat_queue_cmd = [self.qstat_path, "-Q", "-f", "-x"]
        self._qstat_queue_text_cmd = [self.qstat_path, "-Q", "-f"]
        self._pbsnodes_json_cmd = [self.pbsnodes_path, "-a", "-F", "json"]
        self._pbsnodes_cmd = [self.pbsnodes_path, "-x"]
        self._pbsnodes_text_cmd = [self.pbsnodes_path, "-a"]

    async def fetch_snapshot(self) -> SchedulerSnapshot:
        """Collect scheduler information.

        Returns a :class:`SchedulerSnapshot` either from the live PBS system or
        from bundled sample data when the PBS utilities are not available.
        """

        if self.force_sample:
            snapshot = sample_snapshot()
            snapshot.errors.insert(
                0,
                "Using bundled sample data because PBS_TUI_SAMPLE_DATA was set.",
            )
            return snapshot

        jobs, job_errors = await self._fetch_jobs()
        nodes: List[Node] = []
        node_errors: List[str] = []
        queues: List[Queue] = []
        queue_errors: List[str] = []

        if self.include_nodes:
            nodes, node_errors = await self._fetch_nodes()
        if self.include_queues:
            queues, queue_errors = await self._fetch_queues()

        errors = job_errors + node_errors + queue_errors

        if not jobs and not nodes and self.fallback_to_sample:
            snapshot = sample_snapshot()
            snapshot.errors = errors + snapshot.errors
            return snapshot

        snapshot = SchedulerSnapshot(
            jobs=jobs,
            nodes=nodes,
            queues=queues,
            timestamp=datetime.now(timezone.utc),
            source="pbs",
            errors=errors,
        )
        self._populate_queue_job_counts(snapshot)
        return snapshot

    async def _fetch_jobs(self) -> Tuple[List[Job], List[str]]:
        attempts: Sequence[Tuple[List[str], Callable[[str], List[Job]], str]] = (
            (self._qstat_jobs_json_cmd, self._parse_jobs_json, "qstat JSON output"),
            (self._qstat_jobs_cmd, self._parse_jobs_xml, "qstat XML output"),
            (self._qstat_jobs_text_cmd, self._parse_jobs_text, "qstat full output"),
        )
        return await self._attempt_fetch(
            attempts, "Unable to obtain job information from qstat"
        )

    async def _fetch_nodes(self) -> Tuple[List[Node], List[str]]:
        attempts: Sequence[Tuple[List[str], Callable[[str], List[Node]], str]] = (
            (self._pbsnodes_json_cmd, self._parse_nodes_json, "pbsnodes JSON output"),
            (self._pbsnodes_cmd, self._parse_nodes_xml, "pbsnodes XML output"),
            (self._pbsnodes_text_cmd, self._parse_nodes_text, "pbsnodes full output"),
        )
        return await self._attempt_fetch(
            attempts, "Unable to obtain node information from pbsnodes"
        )

    async def _fetch_queues(self) -> Tuple[List[Queue], List[str]]:
        attempts: Sequence[Tuple[List[str], Callable[[str], List[Queue]], str]] = (
            (self._qstat_queue_json_cmd, self._parse_queues_json, "queue JSON output"),
            (self._qstat_queue_cmd, self._parse_queues_xml, "queue XML output"),
            (self._qstat_queue_text_cmd, self._parse_queues_text, "queue full output"),
        )
        return await self._attempt_fetch(
            attempts, "Unable to obtain queue information from qstat"
        )

    async def _attempt_fetch(
        self,
        attempts: Sequence[Tuple[List[str], Callable[[str], List[T]], str]],
        base_error: str,
    ) -> Tuple[List[T], List[str]]:
        errors: List[str] = []
        for command, parser, description in attempts:
            output, error = await self._run_command(command)
            if output is None:
                if error:
                    errors.append(error)
                else:
                    errors.append(f"Unable to execute {' '.join(command)}")
                continue
            try:
                return parser(output), []
            except asyncio.CancelledError:
                raise
            except ET.ParseError as exc:
                message = f"Failed to parse {description}: {exc}"
                _LOGGER.warning(message)
                errors.append(message)
            except Exception as exc:  # pragma: no cover - defensive
                message = f"Failed to parse {description}: {exc}"
                _LOGGER.warning(message)
                errors.append(message)
        return [], errors or [base_error]

    async def _run_command(self, cmd: List[str]) -> Tuple[Optional[str], Optional[str]]:
        """Execute *cmd* asynchronously and return ``(stdout, error)``."""

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            message = f"Command not found: {' '.join(cmd)}"
            _LOGGER.debug(message)
            return None, message
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self.command_timeout
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            message = f"Command timed out after {self.command_timeout}s: {' '.join(cmd)}"
            _LOGGER.warning(message)
            return None, message
        if process.returncode != 0:
            message = stderr.decode().strip() or f"Command failed: {' '.join(cmd)}"
            _LOGGER.warning("%s (exit %s)", message, process.returncode)
            return None, message
        return stdout.decode(), None

    def _parse_jobs_xml(self, xml_text: str) -> List[Job]:
        root = ET.fromstring(xml_text)
        jobs: List[Job] = []
        for job_el in root.findall(".//Job"):
            job_id = job_el.findtext("Job_Id", "").strip()
            if not job_id:
                continue
            job = Job(
                id=job_id,
                name=job_el.findtext("Job_Name", "").strip(),
                user=self._normalise_user(job_el.findtext("Job_Owner") or job_el.findtext("owner")),
                queue=job_el.findtext("queue", "").strip(),
                state=job_el.findtext("job_state", "").strip(),
                exec_host=(job_el.findtext("exec_host") or "").strip() or None,
                create_time=_parse_timestamp(job_el.findtext("ctime")),
                start_time=_parse_timestamp(
                    job_el.findtext("start_time")
                    or job_el.findtext("stime")
                    or job_el.findtext("etime")
                ),
                end_time=_parse_timestamp(job_el.findtext("comp_time") or job_el.findtext("mtime")),
                walltime=(
                    job_el.findtext("Resource_List/walltime")
                    or job_el.findtext("resources_default/walltime")
                ),
                nodes=job_el.findtext("Resource_List/nodes"),
                resources_requested=_collect_child_text(job_el.find("Resource_List")),
                resources_used=_collect_child_text(job_el.find("resources_used")),
                comment=(job_el.findtext("comment") or job_el.findtext("sched_comment")),
                exit_status=job_el.findtext("Exit_status"),
            )
            jobs.append(job)
        return jobs

    def _parse_nodes_xml(self, xml_text: str) -> List[Node]:
        root = ET.fromstring(xml_text)
        nodes: List[Node] = []
        for node_el in root.findall(".//Node"):
            name = node_el.findtext("name", "").strip()
            if not name:
                continue
            resources_available = _collect_child_text(node_el.find("resources_available"))
            resources_assigned = _collect_child_text(node_el.find("resources_assigned"))
            jobs_field = (node_el.findtext("jobs") or "").strip()
            jobs_list = self._parse_node_jobs(jobs_field)
            node = Node(
                name=name,
                state=(node_el.findtext("state") or "").strip(),
                np=_parse_int(node_el.findtext("np")),
                ncpus=_parse_int(resources_available.get("ncpus")),
                properties=self._parse_properties(node_el.findtext("properties")),
                jobs=jobs_list,
                resources_available=resources_available,
                resources_assigned=resources_assigned,
                comment=(node_el.findtext("comment") or "").strip() or None,
            )
            nodes.append(node)
        return nodes

    def _parse_queues_xml(self, xml_text: str) -> List[Queue]:
        root = ET.fromstring(xml_text)
        queues: List[Queue] = []
        for queue_el in root.findall(".//Queue"):
            name = queue_el.findtext("queue_name", "").strip()
            if not name:
                continue
            queue = Queue(
                name=name,
                state=(queue_el.findtext("state_count") or queue_el.findtext("state") or "").strip() or None,
                enabled=_parse_bool(queue_el.findtext("enabled")),
                started=_parse_bool(queue_el.findtext("started")),
                total_jobs=_parse_int(queue_el.findtext("total_jobs")),
                job_states=self._parse_state_counts(queue_el.findtext("state_count")),
                resources_default=_collect_child_text(queue_el.find("resources_default")),
                resources_max=_collect_child_text(queue_el.find("resources_max")),
                comment=(queue_el.findtext("comment") or "").strip() or None,
            )
            queues.append(queue)
        return queues

    def _parse_jobs_json(self, json_text: str) -> List[Job]:
        payload = json.loads(json_text)
        jobs: List[Job] = []
        for key, record in _extract_records(payload, ("Jobs", "jobs", "Job", "job")):
            mapping = _flatten_mapping(record)
            if key and "Job_Id" not in mapping:
                mapping["Job_Id"] = key
            job = self._job_from_mapping(mapping)
            if job is not None:
                jobs.append(job)
        return jobs

    def _parse_nodes_json(self, json_text: str) -> List[Node]:
        payload = json.loads(json_text)
        nodes: List[Node] = []
        for key, record in _extract_records(payload, ("nodes", "Nodes", "Node")):
            mapping = _flatten_mapping(record)
            name_value = (
                mapping.get("name")
                or mapping.get("Node_Name")
                or mapping.get("Node")
                or key
                or ""
            )
            node = self._node_from_mapping(str(name_value), mapping)
            if node is not None:
                nodes.append(node)
        return nodes

    def _parse_queues_json(self, json_text: str) -> List[Queue]:
        payload = json.loads(json_text)
        queues: List[Queue] = []
        for key, record in _extract_records(payload, ("Queue", "Queues", "queue")):
            mapping = _flatten_mapping(record)
            name = (
                mapping.get("queue_name")
                or mapping.get("name")
                or mapping.get("Queue")
                or key
                or ""
            )
            if not name:
                continue
            queue = self._queue_from_mapping(str(name), mapping)
            queues.append(queue)
        return queues

    def _parse_jobs_text(self, text: str) -> List[Job]:
        records: List[Dict[str, str]] = []
        record: Dict[str, str] = {}
        last_key: Optional[str] = None
        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                if record:
                    records.append(record)
                    record = {}
                last_key = None
                continue
            stripped = line.strip()
            key: str
            value: str
            if stripped.lower().startswith("job id"):
                if record:
                    records.append(record)
                    record = {}
                key, value = self._split_key_value(stripped)
                record["Job_Id"] = value
                last_key = "Job_Id"
                continue
            key, value = self._split_key_value(stripped)
            if key:
                record[key] = value
                last_key = key
            elif last_key:
                combined = f"{record.get(last_key, '')} {stripped}".strip()
                record[last_key] = combined
        if record:
            records.append(record)

        jobs: List[Job] = []
        for mapping in records:
            job = self._job_from_mapping(mapping)
            if job is not None:
                jobs.append(job)
        return jobs

    def _job_from_mapping(self, mapping: Dict[str, str]) -> Optional[Job]:
        job_id = (
            mapping.get("Job_Id")
            or mapping.get("Job Id")
            or mapping.get("JobID")
            or mapping.get("JobId")
        )
        if not job_id:
            return None

        resources_requested = {
            key.split(".", 1)[1]: value.strip()
            for key, value in mapping.items()
            if key.startswith("Resource_List.") and "." in key
        }
        resources_used = {
            key.split(".", 1)[1]: value.strip()
            for key, value in mapping.items()
            if key.startswith("resources_used.") and "." in key
        }

        comment = (mapping.get("comment") or mapping.get("sched_comment") or "").strip()
        if not comment:
            comment = None

        exit_status = (mapping.get("Exit_status") or mapping.get("exit_status") or "").strip()
        if not exit_status:
            exit_status = None

        job = Job(
            id=job_id.strip(),
            name=(mapping.get("Job_Name") or mapping.get("Job Name") or "").strip(),
            user=self._normalise_user(
                mapping.get("Job_Owner") or mapping.get("owner") or mapping.get("User_List")
            ),
            queue=(mapping.get("queue") or mapping.get("Queue") or "").strip(),
            state=(mapping.get("job_state") or mapping.get("jobstate") or "").strip(),
            exec_host=(mapping.get("exec_host") or mapping.get("exec_host2") or "").strip() or None,
            create_time=_parse_timestamp(mapping.get("ctime")),
            start_time=_parse_timestamp(
                mapping.get("start_time")
                or mapping.get("stime")
                or mapping.get("etime")
                or mapping.get("qtime")
            ),
            end_time=_parse_timestamp(mapping.get("comp_time") or mapping.get("mtime")),
            walltime=resources_requested.get("walltime")
            or mapping.get("walltime")
            or mapping.get("resources_default.walltime"),
            nodes=resources_requested.get("nodes") or mapping.get("nodes"),
            resources_requested=resources_requested,
            resources_used=resources_used,
            comment=comment,
            exit_status=exit_status,
        )
        return job

    def _parse_nodes_text(self, text: str) -> List[Node]:
        nodes: List[Node] = []
        mapping: Dict[str, str] = {}
        current_name: Optional[str] = None
        last_key: Optional[str] = None
        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                if current_name:
                    node = self._node_from_mapping(current_name, mapping)
                    if node is not None:
                        nodes.append(node)
                mapping = {}
                current_name = None
                last_key = None
                continue
            if not raw_line.startswith((" ", "\t")) and "=" not in line and ":" not in line:
                if current_name:
                    node = self._node_from_mapping(current_name, mapping)
                    if node is not None:
                        nodes.append(node)
                current_name = line.strip()
                mapping = {}
                last_key = None
                continue
            stripped = line.strip()
            key, value = self._split_key_value(stripped)
            if key:
                mapping[key] = value
                last_key = key
            elif last_key:
                combined = f"{mapping.get(last_key, '')} {stripped}".strip()
                mapping[last_key] = combined
        if current_name:
            node = self._node_from_mapping(current_name, mapping)
            if node is not None:
                nodes.append(node)
        return nodes

    def _node_from_mapping(self, name: str, mapping: Dict[str, str]) -> Optional[Node]:
        name = name.strip()
        if not name:
            return None
        resources_available = {
            key.split(".", 1)[1]: value.strip()
            for key, value in mapping.items()
            if key.startswith("resources_available.") and "." in key
        }
        resources_assigned = {
            key.split(".", 1)[1]: value.strip()
            for key, value in mapping.items()
            if key.startswith("resources_assigned.") and "." in key
        }
        comment = (mapping.get("comment") or mapping.get("note") or "").strip()
        if not comment:
            comment = None
        node = Node(
            name=name,
            state=(mapping.get("state") or mapping.get("states") or "").strip(),
            np=_parse_int(mapping.get("np")),
            ncpus=_parse_int(
                resources_available.get("ncpus")
                or mapping.get("ncpus")
                or resources_available.get("np")
            ),
            properties=self._parse_properties(mapping.get("properties")),
            jobs=self._parse_node_jobs(mapping.get("jobs", "")),
            resources_available=resources_available,
            resources_assigned=resources_assigned,
            comment=comment,
        )
        return node

    def _parse_queues_text(self, text: str) -> List[Queue]:
        queues: List[Queue] = []
        mapping: Dict[str, str] = {}
        current_name: Optional[str] = None
        last_key: Optional[str] = None
        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                if current_name:
                    queue = self._queue_from_mapping(current_name, mapping)
                    queues.append(queue)
                mapping = {}
                current_name = None
                last_key = None
                continue
            stripped = line.strip()
            lower = stripped.lower()
            if lower.startswith("queue:") or lower.startswith("queue ="):
                if current_name:
                    queue = self._queue_from_mapping(current_name, mapping)
                    queues.append(queue)
                _, value = self._split_key_value(stripped)
                current_name = value
                mapping = {}
                last_key = None
                continue
            key, value = self._split_key_value(stripped)
            if key:
                mapping[key] = value
                last_key = key
            elif last_key:
                combined = f"{mapping.get(last_key, '')} {stripped}".strip()
                mapping[last_key] = combined
        if current_name:
            queue = self._queue_from_mapping(current_name, mapping)
            queues.append(queue)
        return queues

    def _queue_from_mapping(self, name: str, mapping: Dict[str, str]) -> Queue:
        state_count_value: Optional[object] = mapping.get("state_count")
        if state_count_value is None:
            nested_counts: Dict[str, str] = {
                key.split(".", 1)[1]: value
                for key, value in mapping.items()
                if key.startswith("state_count.") and "." in key
            }
            if nested_counts:
                state_count_value = nested_counts
        resources_default = {
            key.split(".", 1)[1]: value.strip()
            for key, value in mapping.items()
            if key.startswith("resources_default.") and "." in key
        }
        resources_max = {
            key.split(".", 1)[1]: value.strip()
            for key, value in mapping.items()
            if key.startswith("resources_max.") and "." in key
        }
        comment = (mapping.get("comment") or "").strip() or None
        queue = Queue(
            name=name.strip(),
            state=(mapping.get("state_count") or mapping.get("state") or "").strip() or None,
            enabled=_parse_bool(mapping.get("enabled")),
            started=_parse_bool(mapping.get("started")),
            total_jobs=_parse_int(mapping.get("total_jobs")),
            job_states=self._parse_state_counts(state_count_value),
            resources_default=resources_default,
            resources_max=resources_max,
            comment=comment,
        )
        return queue

    @staticmethod
    def _split_key_value(line: str) -> Tuple[str, str]:
        for separator in ("=", ":"):
            if separator in line:
                key, value = line.split(separator, 1)
                return key.strip(), value.strip()
        return "", line.strip()

    def _populate_queue_job_counts(self, snapshot: SchedulerSnapshot) -> None:
        if not snapshot.queues and not snapshot.jobs:
            return
        queue_map = {queue.name: queue for queue in snapshot.queues}
        counts: Dict[str, Counter[str]] = defaultdict(Counter)
        for job in snapshot.jobs:
            if not job.queue:
                continue
            counts[job.queue][job.state] += 1
        for queue_name, counter in counts.items():
            queue = queue_map.get(queue_name)
            if queue is None:
                queue = Queue(name=queue_name)
                snapshot.queues.append(queue)
                queue_map[queue_name] = queue
            if counter:
                queue.job_states = dict(counter)
                queue.total_jobs = sum(counter.values())
        for queue in snapshot.queues:
            if queue.total_jobs is None and queue.job_states:
                queue.total_jobs = sum(queue.job_states.values())

    @staticmethod
    def _normalise_user(owner: Optional[str]) -> str:
        if not owner:
            return ""
        owner = owner.strip()
        if "@" in owner:
            owner = owner.split("@", 1)[0]
        return owner

    @staticmethod
    def _parse_properties(properties: Optional[str]) -> List[str]:
        if not properties:
            return []
        return [prop.strip() for prop in properties.split(",") if prop.strip()]

    @staticmethod
    def _parse_node_jobs(jobs_field: str) -> List[str]:
        if not jobs_field:
            return []
        jobs: List[str] = []
        for part in jobs_field.replace("\n", " ").split():
            cleaned = part.strip().strip(",")
            if cleaned:
                jobs.append(cleaned)
        return jobs

    @staticmethod
    def _parse_state_counts(state_count: Optional[object]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        if not state_count:
            return counts
        mapping = {
            "transit": "T",
            "queued": "Q",
            "held": "H",
            "waiting": "W",
            "running": "R",
            "exiting": "E",
            "suspended": "S",
            "begun": "B",
            "finished": "F",
        }
        if isinstance(state_count, dict):
            for key, value in state_count.items():
                code = mapping.get(str(key).lower(), str(key).upper()[:1])
                try:
                    counts[code] = int(str(value))
                except (TypeError, ValueError):
                    continue
            return counts
        tokens = str(state_count).replace("\n", " ").replace(",", "").split()
        i = 0
        while i < len(tokens) - 1:
            key = tokens[i].rstrip(":").lower()
            value = tokens[i + 1]
            i += 2
            if not key:
                continue
            code = mapping.get(key, key.upper()[:1])
            try:
                counts[code] = int(value)
            except ValueError:
                continue
        return counts


__all__ = ["PBSDataFetcher"]

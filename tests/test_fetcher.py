import asyncio
import json
import textwrap
from datetime import datetime, timezone

from pbs_tui.fetcher import PBSDataFetcher


def test_fetch_snapshot_force_sample_uses_bundled_data():
    fetcher = PBSDataFetcher(force_sample=True)
    snapshot = asyncio.run(fetcher.fetch_snapshot())
    assert snapshot.source == "sample"
    assert snapshot.jobs
    assert any("sample data" in message.lower() for message in snapshot.errors)


def test_parse_state_counts_extracts_known_states():
    counts = PBSDataFetcher._parse_state_counts("queued: 2 running: 1 held: 3")
    assert counts == {"Q": 2, "R": 1, "H": 3}


def test_parse_state_counts_ignores_noise():
    counts = PBSDataFetcher._parse_state_counts("queued: notanumber waiting: 4, unknown: 2")
    assert counts == {"W": 4, "U": 2}


def test_parse_state_counts_accepts_dict():
    counts = PBSDataFetcher._parse_state_counts({"Queued": "5", "Running": 3, "Held": 0})
    assert counts == {"Q": 5, "R": 3, "H": 0}


def test_parse_node_jobs_splits_entries():
    jobs = PBSDataFetcher._parse_node_jobs("0/123.a r/456.b, 789.c")
    assert jobs == ["0/123.a", "r/456.b", "789.c"]


def test_parse_jobs_xml_extracts_fields():
    xml = """
    <Data>
      <Job>
        <Job_Id>123.a</Job_Id>
        <Job_Name>analysis</Job_Name>
        <Job_Owner>user@host</Job_Owner>
        <queue>batch</queue>
        <job_state>R</job_state>
        <exec_host>nid0001</exec_host>
        <ctime>2024-05-11T10:00:00Z</ctime>
        <start_time>2024-05-11T10:05:00Z</start_time>
        <Resource_List>
          <walltime>01:00:00</walltime>
          <nodes>1:ppn=4</nodes>
        </Resource_List>
        <resources_used>
          <walltime>00:10:00</walltime>
        </resources_used>
      </Job>
    </Data>
    """
    fetcher = PBSDataFetcher(force_sample=True)
    job = fetcher._parse_jobs_xml(xml)[0]
    assert job.id == "123.a"
    assert job.user == "user"
    assert job.resources_requested["walltime"] == "01:00:00"
    assert job.resources_used["walltime"] == "00:10:00"
    assert job.runtime(datetime(2024, 5, 11, 10, 15, tzinfo=timezone.utc))


def test_parse_jobs_text_extracts_fields():
    text = textwrap.dedent(
        """
        Job Id: 456.b
            Job_Name = sim
            Job_Owner = alice@cluster
            queue = prod
            job_state = R
            exec_host = node001/0+node002/0
            ctime = Tue May 14 09:00:00 2024
            start_time = Tue May 14 09:05:00 2024
            Resource_List.walltime = 01:30:00
            Resource_List.nodes = 2:ppn=64
            resources_used.walltime = 00:45:00
        """
    ).strip()
    fetcher = PBSDataFetcher(force_sample=True)
    job = fetcher._parse_jobs_text(text)[0]
    assert job.id == "456.b"
    assert job.queue == "prod"
    assert job.exec_host == "node001/0+node002/0"
    assert job.resources_requested["nodes"] == "2:ppn=64"
    assert job.resources_used["walltime"] == "00:45:00"


def test_parse_jobs_json_extracts_fields():
    payload = {
        "Jobs": {
            "789.c": {
                "Job_Name": "render",
                "Job_Owner": "carol@cluster",
                "queue": "vis",
                "job_state": "Q",
                "Resource_List": {"walltime": "00:20:00", "nodes": "1:ppn=16"},
                "resources_used": {"walltime": "00:05:00"},
            }
        }
    }
    fetcher = PBSDataFetcher(force_sample=True)
    job = fetcher._parse_jobs_json(json.dumps(payload))[0]
    assert job.id == "789.c"
    assert job.queue == "vis"
    assert job.resources_requested["nodes"] == "1:ppn=16"
    assert job.resources_used["walltime"] == "00:05:00"


def test_parse_nodes_text_extracts_fields():
    text = textwrap.dedent(
        """
        nid0001
            state = free
            np = 128
            properties = gpu,ssd
            resources_available.ncpus = 128
            resources_available.mem = 512gb
            resources_assigned.ncpus = 0
            jobs = 0/111.a 1/222.b

        nid0002
            state = offline
        """
    ).strip()
    fetcher = PBSDataFetcher(force_sample=True)
    nodes = fetcher._parse_nodes_text(text)
    assert nodes[0].name == "nid0001"
    assert nodes[0].ncpus == 128
    assert nodes[0].resources_available["mem"] == "512gb"
    assert nodes[0].jobs == ["0/111.a", "1/222.b"]
    assert nodes[1].state == "offline"


def test_parse_nodes_json_extracts_fields():
    payload = {
        "nodes": {
            "nid0003": {
                "state": "job-exclusive",
                "resources_available": {"ncpus": 64, "mem": "128gb"},
                "resources_assigned": {"ncpus": 64},
                "jobs": ["0/999.a", "0/888.b"],
            }
        }
    }
    fetcher = PBSDataFetcher(force_sample=True)
    node = fetcher._parse_nodes_json(json.dumps(payload))[0]
    assert node.name == "nid0003"
    assert node.ncpus == 64
    assert node.resources_available["mem"] == "128gb"
    assert node.jobs == ["0/999.a", "0/888.b"]


def test_parse_queues_text_extracts_fields():
    text = textwrap.dedent(
        """
        Queue: prod
            enabled = True
            started = True
            total_jobs = 5
            state_count = Queued: 2 Running: 3
            resources_default.walltime = 02:00:00
            resources_max.walltime = 24:00:00
            comment = Production queue
        """
    ).strip()
    fetcher = PBSDataFetcher(force_sample=True)
    queue = fetcher._parse_queues_text(text)[0]
    assert queue.name == "prod"
    assert queue.enabled is True
    assert queue.started is True
    assert queue.job_states == {"Q": 2, "R": 3}
    assert queue.resources_default["walltime"] == "02:00:00"
    assert queue.comment == "Production queue"


def test_parse_queues_json_extracts_fields():
    payload = {
        "Queue": {
            "analysis": {
                "queue_name": "analysis",
                "enabled": False,
                "started": True,
                "total_jobs": 4,
                "state_count": {"Queued": 3, "Running": 1},
                "resources_default": {"walltime": "04:00:00"},
            }
        }
    }
    fetcher = PBSDataFetcher(force_sample=True)
    queue = fetcher._parse_queues_json(json.dumps(payload))[0]
    assert queue.name == "analysis"
    assert queue.enabled is False
    assert queue.started is True
    assert queue.job_states == {"Q": 3, "R": 1}
    assert queue.resources_default["walltime"] == "04:00:00"

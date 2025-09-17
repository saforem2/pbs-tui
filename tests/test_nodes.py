import pytest

from pbs_tui.app import job_node_summary
from pbs_tui.nodes import (
    extract_exec_host_nodes,
    extract_requested_nodes,
    normalize_node_tokens,
    parse_node_count_spec,
)

from .util import make_job


@pytest.mark.parametrize(
    "description, overrides, expected",
    [
        pytest.param(
            "exec_host_multiple",
            {
                "state": "R",
                "exec_host": "nodeA/0+nodeA/1+nodeB/0*2",
                "nodes": "2:ppn=1",
            },
            (2, "nodeA"),
        ),
        pytest.param(
            "exec_host_range",
            {
                "state": "R",
                "exec_host": "node[01-03]/0",
                "nodes": "node[01-03]",
            },
            (3, "node01"),
        ),
        pytest.param(
            "exec_host_empty",
            {"exec_host": "", "nodes": "node01+node02"},
            (2, "node01"),
        ),
        pytest.param(
            "exec_host_duplicates",
            {"state": "R", "exec_host": "nodeC/0+nodeC/0+nodeC/1"},
            (1, "nodeC"),
        ),
        pytest.param(
            "exec_host_malformed",
            {"state": "R", "exec_host": "nodeD/0++nodeE/1*+  +/junk"},
            (2, "nodeD"),
        ),
        pytest.param(
            "requested_named",
            {"nodes": "node01+node02:ppn=2"},
            (2, "node01"),
        ),
        pytest.param("requested_numeric_only", {"nodes": "2"}, (2, None)),
        pytest.param("requested_delimiters_only", {"nodes": "++"}, (None, None)),
        pytest.param("requested_mixed", {"nodes": "2+nodeX"}, (3, "nodeX")),
        pytest.param("requested_range", {"nodes": "node[01-03]"}, (3, "node01")),
        pytest.param("requested_digit_range", {"nodes": "[01-02]"}, (2, "01")),
        pytest.param(
            "fallback_nodes_resource",
            {"nodes": "3:ppn=64", "resources_requested": {"nodes": "3:ppn=64"}},
            (3, None),
        ),
        pytest.param(
            "fallback_select_over_nodect",
            {"resources_requested": {"select": "2:ncpus=36", "nodect": "5"}},
            (2, None),
        ),
        pytest.param(
            "fallback_nodect",
            {"resources_requested": {"nodect": "5"}},
            (5, None),
        ),
        pytest.param(
            "exec_host_overrides_nodes_count",
            {
                "state": "R",
                "exec_host": "nodeA/0+nodeB/1+nodeC/2",
                "nodes": "nodeA+nodeB",
            },
            (3, "nodeA"),
        ),
        pytest.param("missing_nodes", {}, (None, None)),
        pytest.param(
            "blank_nodes",
            {"nodes": "", "resources_requested": {}},
            (None, None),
        ),
        pytest.param(
            "invalid_nodes",
            {"nodes": "!!!", "resources_requested": {}},
            (None, None),
        ),
        pytest.param(
            "malformed_exec_host_valid_nodes",
            {
                "exec_host": "malformed!host!string",
                "nodes": "nodeA+nodeB",
                "resources_requested": {},
            },
            (2, "nodeA"),
        ),
    ],
)
def test_job_node_summary_cases(description, overrides, expected):
    job = make_job(**overrides)
    assert job_node_summary(job) == expected


@pytest.mark.parametrize(
    "token, expected",
    [
        ("node[01-02]", ["node01", "node02"]),
        ("node[2-1]", ["node2", "node1"]),
        ("node[1,3,5]", ["node1", "node3", "node5"]),
        ("node", ["node"]),
        ("node[ ]", ["node[ ]"]),
    ],
)
def test_normalize_node_tokens(token, expected):
    assert list(normalize_node_tokens(token)) == expected


@pytest.mark.parametrize(
    "spec, expected",
    [
        (None, None),
        ("", None),
        ("2", 2),
        ("node01+node02", 2),
        ("node[01-03]", 3),
        ("[01-02]", 2),
        ("2:ppn=4", 2),
        ("node01:ppn=4+node02", 2),
        ("++", None),
        ("node01+!!!+node02", 2),
    ],
)
def test_parse_node_count_spec(spec, expected):
    assert parse_node_count_spec(spec) == expected


@pytest.mark.parametrize(
    "spec, expected",
    [
        (None, []),
        ("", []),
        ("nodeA/0+nodeB/1", ["nodeA", "nodeB"]),
        ("nodeA/0+nodeA/1", ["nodeA"]),
        ("001/0+002/1", ["001", "002"]),
    ],
)
def test_extract_exec_host_nodes(spec, expected):
    assert extract_exec_host_nodes(spec) == expected


@pytest.mark.parametrize(
    "spec, expected",
    [
        (None, []),
        ("", []),
        ("nodeA+nodeB", ["nodeA", "nodeB"]),
        ("nodeA+nodeA", ["nodeA"]),
        ("node[01-02]", ["node01", "node02"]),
        ("[01-02]", []),
    ],
)
def test_extract_requested_nodes(spec, expected):
    assert extract_requested_nodes(spec) == expected

"""Utilities for parsing PBS node specifications.

This module centralises the heuristics used to interpret the many flavours of
PBS ``exec_host`` and ``nodes`` strings.  The general strategy is to split a
specification into tokens, normalise each token by trimming qualifiers such as
``/0`` or ``:ppn=4`` and expanding bracketed ranges, and finally deduplicate the
result while optionally filtering numeric identifiers.  The helpers below are
shared by both the interactive TUI and file exports so that all paths interpret
node information consistently.
"""

from __future__ import annotations

import re
from typing import Iterable, Iterator, Optional

__all__ = [
    "split_node_spec",
    "normalize_node_tokens",
    "extract_nodes",
    "extract_exec_host_nodes",
    "extract_requested_nodes",
    "parse_node_count_spec",
]


_NODE_COUNT_PATTERN = re.compile(r"^(\d+)")
_NODE_CLEAN_PATTERN = re.compile(r"[\*/:]")
_NODE_RANGE_PATTERN = re.compile(
    r"^(?P<prefix>[^\[\]]*)\[(?P<body>[^\]]+)\](?P<suffix>.*)$"
)


def split_node_spec(value: str) -> Iterable[str]:
    for part in value.replace(",", "+").split("+"):
        if segment := part.strip():
            yield segment


def normalize_node_tokens(token: str) -> Iterator[str]:
    base = _NODE_CLEAN_PATTERN.split(token.strip(), maxsplit=1)[0].strip()
    if not base:
        return
    match = _NODE_RANGE_PATTERN.match(base)
    if not match:
        yield base
        return

    prefix = match.group("prefix")
    suffix = match.group("suffix")
    body = match.group("body")
    emitted = False
    for piece in body.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "-" in piece:
            start, end = piece.split("-", 1)
            start = start.strip()
            end = end.strip()
            if start and end and start.isdigit() and end.isdigit():
                width = max(len(start), len(end))
                start_int = int(start)
                end_int = int(end)
                step = 1 if end_int >= start_int else -1
                for value in range(start_int, end_int + step, step):
                    emitted = True
                    yield f"{prefix}{value:0{width}d}{suffix}"
                continue
        emitted = True
        yield f"{prefix}{piece}{suffix}"
    if not emitted:
        yield base


def extract_nodes(spec: Optional[str], *, allow_numeric: bool) -> list[str]:
    if not spec:
        return []
    seen: set[str] = set()
    nodes: list[str] = []
    for part in split_node_spec(spec):
        for candidate in normalize_node_tokens(part):
            if not candidate:
                continue
            if not any(char.isalnum() for char in candidate):
                continue
            if not allow_numeric and candidate.isdigit():
                continue
            if candidate not in seen:
                seen.add(candidate)
                nodes.append(candidate)
    return nodes


def extract_exec_host_nodes(exec_host: Optional[str]) -> list[str]:
    return extract_nodes(exec_host, allow_numeric=True)


def extract_requested_nodes(nodes_spec: Optional[str]) -> list[str]:
    return extract_nodes(nodes_spec, allow_numeric=False)


def parse_node_count_spec(spec: Optional[str]) -> Optional[int]:
    if spec is None:
        return None
    spec = spec.strip()
    if not spec:
        return None
    total = 0
    found = False
    for part in split_node_spec(spec):
        if match := _NODE_COUNT_PATTERN.match(part):
            found = True
            total += int(match.group(1))
            continue
        if candidates := [
            node
            for node in normalize_node_tokens(part)
            if node and any(char.isalnum() for char in node)
        ]:
            non_numeric = [node for node in candidates if not node.isdigit()]
            if non_numeric:
                found = True
                total += len(non_numeric)
    return total if found else None

"""Utilities for parsing PBS node specifications."""

from __future__ import annotations

import re
from typing import Iterable, Optional

__all__ = [
    "_split_node_spec",
    "_clean_node_token",
    "_expand_node_token",
    "_extract_nodes",
    "_extract_exec_host_nodes",
    "_extract_requested_nodes",
    "_parse_node_count_spec",
]


_NODE_COUNT_PATTERN = re.compile(r"^(\d+)")
_NODE_CLEAN_PATTERN = re.compile(r"[\*/:]")
_NODE_RANGE_PATTERN = re.compile(
    r"^(?P<prefix>[^\[\]]*)\[(?P<body>[^\]]+)\](?P<suffix>.*)$"
)


def _split_node_spec(value: str) -> Iterable[str]:
    for part in value.replace(",", "+").split("+"):
        if segment := part.strip():
            yield segment


def _clean_node_token(token: str) -> str:
    """Return the node identifier portion of *token*."""

    return _NODE_CLEAN_PATTERN.split(token.strip(), maxsplit=1)[0].strip()


def _expand_node_token(token: str) -> list[str]:
    """Expand a cleaned token into concrete node names."""

    match = _NODE_RANGE_PATTERN.match(token)
    if not match:
        return [token] if token else []

    prefix = match.group("prefix")
    suffix = match.group("suffix")
    body = match.group("body")
    expanded: list[str] = []
    for piece in body.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "-" in piece:
            start, end = piece.split("-", 1)
            start = start.strip()
            end = end.strip()
            if not start or not end:
                expanded.append(piece)
                continue
            if start.isdigit() and end.isdigit():
                width = max(len(start), len(end))
                start_int = int(start)
                end_int = int(end)
                step = 1 if end_int >= start_int else -1
                for value in range(start_int, end_int + step, step):
                    expanded.append(f"{prefix}{value:0{width}d}{suffix}")
                continue
        expanded.append(f"{prefix}{piece}{suffix}")
    return expanded or [token]


def _extract_nodes(
    spec: Optional[str], *, allow_numeric: bool, expand_ranges: bool = True
) -> list[str]:
    if not spec:
        return []
    seen: set[str] = set()
    nodes: list[str] = []
    for part in _split_node_spec(spec):
        cleaned = _clean_node_token(part)
        if not cleaned:
            continue
        candidates = _expand_node_token(cleaned) if expand_ranges else [cleaned]
        for candidate in candidates:
            if not candidate:
                continue
            if not allow_numeric and candidate.isdigit():
                continue
            if candidate not in seen:
                seen.add(candidate)
                nodes.append(candidate)
    return nodes


def _extract_exec_host_nodes(exec_host: Optional[str]) -> list[str]:
    return _extract_nodes(exec_host, allow_numeric=True)


def _extract_requested_nodes(nodes_spec: Optional[str]) -> list[str]:
    return _extract_nodes(nodes_spec, allow_numeric=True)


def _parse_node_count_spec(spec: Optional[str]) -> Optional[int]:
    if spec is None:
        return None
    spec = spec.strip()
    if not spec:
        return None
    total = 0
    found = False
    for part in _split_node_spec(spec):
        if not (stripped := part.strip()):
            continue
        if match := _NODE_COUNT_PATTERN.match(stripped):
            found = True
            total += int(match.group(1))
        elif cleaned := _clean_node_token(stripped):
            if candidates := _expand_node_token(cleaned):
                found = True
                total += len(candidates)
    return total if found else None

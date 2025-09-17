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


def _normalize_node_token(token: str, *, expand_ranges: bool = True) -> list[str]:
    """Return concrete node names parsed from *token*."""

    base = _NODE_CLEAN_PATTERN.split(token.strip(), maxsplit=1)[0].strip()
    if not base:
        return []
    if not expand_ranges:
        return [base]

    match = _NODE_RANGE_PATTERN.match(base)
    if not match:
        return [base]

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
            if start and end and start.isdigit() and end.isdigit():
                width = max(len(start), len(end))
                start_int = int(start)
                end_int = int(end)
                step = 1 if end_int >= start_int else -1
                expanded.extend(
                    f"{prefix}{value:0{width}d}{suffix}"
                    for value in range(start_int, end_int + step, step)
                )
                continue
        expanded.append(f"{prefix}{piece}{suffix}")
    return expanded or [base]


def _clean_node_token(token: str) -> str:
    """Return the node identifier portion of *token*."""

    normalized = _normalize_node_token(token, expand_ranges=False)
    return normalized[0] if normalized else ""


def _expand_node_token(token: str) -> list[str]:
    """Expand a cleaned token into concrete node names."""

    return _normalize_node_token(token)


def _extract_nodes(
    spec: Optional[str], *, allow_numeric: bool, expand_ranges: bool = True
) -> list[str]:
    if not spec:
        return []
    seen: set[str] = set()
    nodes: list[str] = []
    for part in _split_node_spec(spec):
        candidates = _normalize_node_token(part, expand_ranges=expand_ranges)
        for candidate in candidates:
            if not allow_numeric and candidate.isdigit():
                continue
            if candidate not in seen:
                seen.add(candidate)
                nodes.append(candidate)
    return nodes


def _extract_exec_host_nodes(exec_host: Optional[str]) -> list[str]:
    return _extract_nodes(exec_host, allow_numeric=True)


def _extract_requested_nodes(nodes_spec: Optional[str]) -> list[str]:
    return _extract_nodes(nodes_spec, allow_numeric=False)


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
        else:
            candidates = _normalize_node_token(stripped)
            if candidates:
                found = True
                total += len(candidates)
    return total if found else None

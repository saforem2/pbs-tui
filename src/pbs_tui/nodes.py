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
_NODE_RANGE_PATTERN = re.compile(
    r"^(?P<prefix>[^\[\]]*)\[(?P<body>[^\]]+)\](?P<suffix>.*)$"
)
_SPLIT_TOP_LEVEL_PATTERN = re.compile(r"[+,](?![^\[]*\])")
_QUALIFIER_PATTERN = re.compile(r"[\*/:]")


def split_node_spec(value: str) -> Iterable[str]:
    """Yield top-level tokens from *value*, respecting bracketed ranges."""

    for token in _SPLIT_TOP_LEVEL_PATTERN.split(value.replace("+", ",")):
        if segment := token.strip():
            yield segment


def _strip_qualifiers(token: str) -> str:
    """Trim whitespace and drop suffixes introduced by ``*``, ``/`` or ``:``."""

    return _QUALIFIER_PATTERN.split(token.strip(), maxsplit=1)[0].strip()


def _expand_node_ranges(base: str) -> Iterator[str]:
    match = _NODE_RANGE_PATTERN.match(base)
    if not match or "[" in match.group("body") or "]" in match.group("body"):
        if base:
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
                emitted = True
                yield from (
                    f"{prefix}{value:0{width}d}{suffix}"
                    for value in range(start_int, end_int + step, step)
                )
                continue
        emitted = True
        yield f"{prefix}{piece}{suffix}"
    if not emitted and base:
        yield base


def normalize_node_tokens(token: str) -> Iterator[str]:
    if base := _strip_qualifiers(token):
        yield from _expand_node_ranges(base)


def extract_nodes(spec: Optional[str], *, allow_numeric: bool) -> list[str]:
    if not spec:
        return []

    candidates = (
        node
        for part in split_node_spec(spec)
        for node in normalize_node_tokens(part)
        if node and any(char.isalnum() for char in node)
    )

    filtered = (
        node for node in candidates if allow_numeric or not node.isdigit()
    )

    return list(dict.fromkeys(filtered))


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
    for part in split_node_spec(spec):
        if match := _NODE_COUNT_PATTERN.match(part):
            total += int(match.group(1))
            continue
        candidates = [
            node
            for node in normalize_node_tokens(part)
            if node and any(char.isalnum() for char in node)
        ]
        if not candidates:
            continue
        if non_numeric := [node for node in candidates if not node.isdigit()]:
            total += len(non_numeric)
        elif "[" in part:
            total += len(candidates)
    return total or None

from __future__ import annotations

import re
from typing import List, Optional, Tuple

from src.interpret import decode
from src.interpret._common import any_none

_NODE_SPEC_REGEX_STRING = r"(.+?)([0-9]+)$"
_NODE_SPEC_REGEX = re.compile(_NODE_SPEC_REGEX_STRING)

_NODE_SPEC_RANGE_REGEX_STRING = r"(.+?)\[([0-9]+)-([0-9]+)\]"
_NODE_SPEC_RANGE_REGEX = re.compile(_NODE_SPEC_RANGE_REGEX_STRING)


class NodeList:
    def __init__(self, _node_specs: List[_NodeSpec]) -> None:
        self._node_specs: List[_NodeSpec] = _node_specs

    def __str__(self) -> str:
        return ",".join([str(node_spec) for node_spec in self._node_specs])

    @classmethod
    def from_string(cls, _v: str) -> Optional[NodeList]:
        tokens = decode.delimited_list(",", _v)
        node_specs = [_NodeSpec.from_string(token) for token in tokens]
        if any_none(node_specs):
            out = None
        else:
            out = NodeList(node_specs)  # type: ignore
        return out


class _NodeSpec:
    def __init__(self, _prefix: str, _digits: int, _range: Tuple[int, int]) -> None:
        self._prefix: str = _prefix
        self._digits: int = _digits
        self._range: Tuple[int, int] = _range

    @property
    def lo(self) -> int:
        return self._range[0]

    @property
    def hi(self) -> int:
        return self._range[1]

    @property
    def span(self) -> int:
        return self.hi - self.lo

    def __len__(self) -> int:
        return self.span

    def __lt__(self, _other: _NodeSpec) -> bool:
        return self.to_tuple() < _other.to_tuple()

    def __str__(self) -> str:
        lo = self.lo
        hi = self.hi
        d = self._digits
        if self.span == 0:
            numbers = f"{lo:0{d}d}"
        else:
            numbers = f"[{lo:0{d}d}-{hi:0{d}d}]"
        return f"{self._prefix}{numbers}"

    def to_tuple(self) -> Tuple[str, int, Tuple[int, int]]:
        return (self._prefix, self._digits, self._range)

    @classmethod
    def from_string(cls, _v: str) -> Optional[_NodeSpec]:
        """
        "c0001" -> ("c", 4, (1, 1))
        "c[0001]" -> ("c", 4, (1, 1))
        "c[0001-0010]" -> ("c", 4, (1, 10))
        """

        match = _NODE_SPEC_REGEX.match(_v)
        if match and not any_none(match.groups()):
            try:
                prefix = match.group(1)
                value = match.group(2)
                return cls._from_parts(prefix, value, value)
            except:
                return None

        match_range = _NODE_SPEC_RANGE_REGEX.match(_v)
        if match_range and not any_none(match_range.groups()):
            try:
                prefix = match_range.group(1)
                lo = match_range.group(2)
                hi = match_range.group(3)
                return cls._from_parts(prefix, lo, hi)
            except:
                return None

        return None

    @classmethod
    def _from_parts(cls, prefix: str, lo: str, hi: str) -> _NodeSpec:
        digit_count = max(len(lo), len(hi))
        return cls(prefix, digit_count, (int(lo), int(hi)))

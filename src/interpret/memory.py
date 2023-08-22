from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple

from src.interpret import decode, _safe_convert

TEBI = "t"
GIBI = "g"
MEBI = "m"
KIBI = "k"
BYTE_CONVERSIONS = {
    TEBI: 1099511627776,
    GIBI: 1073741824,
    MEBI: 1048576,
    KIBI: 1024,
}


MEM_PER_CORE = "c"
MEM_PER_NODE = "n"
MULTIPLIER_COUNT_CODES = [MEM_PER_CORE, MEM_PER_NODE]

_MEMORY_SPEC_REGEX_STRING = r"([1-9][0-9]*)([a-z])([a-z])"
_MEMORY_SPEC_REGEX = re.compile(_MEMORY_SPEC_REGEX_STRING)


def mb_to_bytes(_v: str) -> int:



class MemorySpec:
    def __init__(
        self,
        _multiplier_count_code: str,
        _byte_per_multiplier_count: int,
        _si_prefix: str,
    ) -> None:
        self._multiplier_count_code: str = _multiplier_count_code
        self._byte_per_multiplier_count: int = _byte_per_multiplier_count
        self._si_prefix: str = _si_prefix

    def __call__(self, core_count: int, node_count: int) -> int:
        if self._multiplier_count_code == MEM_PER_CORE:
            return core_count * self._byte_per_multiplier_count
        elif self._multiplier_count_code == MEM_PER_NODE:
            return node_count * self._byte_per_multiplier_count
        else:
            assert False

    def __str__(self) -> str:
        _si_count = self._byte_per_multiplier_count // BYTE_CONVERSIONS[self._si_prefix]
        return f"{_si_count}{self._si_prefix}{self._multiplier_count_code}"

    @classmethod
    def from_string(cls, _v: str) -> Optional[MemorySpec]:
        """
        8gn -> MemorySpec("n", 8*1024**2)
        500mc -> MemorySpec("c", 500*1024**1)
        """
        match = _MEMORY_SPEC_REGEX.match(_v)
        if not match:
            return None

        si_count = match.group(1)
        si_prefix = match.group(2)
        multiplier_count_code = match.group(3)

        try:
            si_count = int(si_count)
        except:
            return None

        if si_prefix is None:
            return None
        if multiplier_count_code is None:
            return None

        byte_count = _convert_memory_to_bytes(si_prefix, si_count)
        if byte_count is None:
            return None

        memory_spec = cls(multiplier_count_code, byte_count, si_prefix)
        return memory_spec


def unlimited_memory_mbytes_to_bytes(_v: str) -> Optional[int]:
    out = decode.unlimited_int(_v)
    out = _convert_mbytes_to_bytes(out)
    return out


def memory_mbytes_to_bytes(_v: str) -> Optional[int]:
    out = _safe_convert.type_cast_int_unsafe_to_none(_v)
    out = _convert_mbytes_to_bytes(out)
    return out


def _convert_mbytes_to_bytes(_v: Optional[int]) -> Optional[int]:
    return _convert_memory_to_bytes(MEBI, _v)


def _convert_memory_to_bytes(
    _prefix: str, _prefix_count: Optional[int]
) -> Optional[int]:
    if _prefix_count is None:
        return None

    try:
        out = BYTE_CONVERSIONS[_prefix] * _prefix_count
    except:
        return None

    return out

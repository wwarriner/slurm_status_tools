from __future__ import annotations

import re
from typing import Any, List, NamedTuple, Optional, Tuple

from src.functional import MonadicFunction as M
from src.interpret import _common, _safe_convert, decode


class ExitCode(NamedTuple):
    exit_code: int
    exit_signal: int

    def __str__(self) -> str:
        return f"{self.exit_code}:{self.exit_signal}"

    def items(self) -> List[Tuple[str, int]]:
        return _to_items(self)

    @classmethod
    def from_string(cls, _v: str) -> Optional[ExitCode]:
        """
        ex: "127:0"

        exitcode, EXITCODE
        - EXITCODE -> DL[":",int] -> NamedTuple:
            - exit: int
            - signal: int
        """
        v = decode.delimited_list(":", _v)

        f = M(_safe_convert.type_cast_int_unsafe_to_none)
        f.lcompose(_safe_convert.clip_negative)
        v = [f(item) for item in v]

        if _common.any_none(v):
            return None

        try:
            code: int = v[0]  # type: ignore
            signal: int = v[1]  # type: ignore
            out = ExitCode(code, signal)
        except:
            out = None

        return out


class NTasksPerNBSC(NamedTuple):
    tasks_per_node: Optional[int]
    tasks_per_baseboard: Optional[int]
    tasks_per_socket: Optional[int]
    tasks_per_core: Optional[int]

    def __str__(self) -> str:
        return _to_bsct_style_string(
            (
                self.tasks_per_node,
                self.tasks_per_baseboard,
                self.tasks_per_socket,
                self.tasks_per_core,
            )
        )

    def items(self) -> List[Tuple[str, int]]:
        return _to_items(self)

    @classmethod
    def from_string(cls, _v: str) -> Optional[NTasksPerNBSC]:
        """
        ex: "0:*:*:24"

        ntasksper[n:b:s:c], NTASKSPERNBSC
        - NTASKSPERNBSC -> DL[":", NONNEGATIVE_DONT_CARE_INT] -> NamedTuple:
            - n: tasks_per_node, int
            - b: tasks_per_baseboard, int
            - s: tasks_per_socket, int
            - c: tasks_per_core, int
        """
        v = _bsct_style_from_string(_v)
        if v is None:
            out = None
        else:
            out = NTasksPerNBSC(*v)
        return out

    def _prefix(self, _v: str) -> str:
        return "exit_" + _v


class ReqBSCT(NamedTuple):
    baseboard_count: Optional[int]
    socket_per_baseboard_count: Optional[int]
    core_per_socket_count: Optional[int]
    thread_per_core_count: Optional[int]

    def __str__(self) -> str:
        return _to_bsct_style_string(
            (
                self.baseboard_count,
                self.socket_per_baseboard_count,
                self.core_per_socket_count,
                self.thread_per_core_count,
            )
        )

    def items(self) -> List[Tuple[str, int]]:
        return _to_items(self)

    @classmethod
    def from_string(cls, _v: str) -> Optional[ReqBSCT]:
        """
        ex: "0:*:*:24"

        req[b:s:c:t], REQBSCT
        - REQBSCT -> DL[":", NONNEGATIVE_DONT_CARE_INT] -> NamedTuple:
            - tasks_per_node: int
            - tasks_per_baseboard: int
            - tasks_per_socket: int
            - tasks_per_core: int
        """
        v = _bsct_style_from_string(_v)
        if v is None:
            out = None
        else:
            out = ReqBSCT(*v)
        return out


_GRES_REGEX_STRING = r"((?:.*:)+)(\d+)(?:\(S:(.*)\))?"
_GRES_REGEX = re.compile(_GRES_REGEX_STRING)


class GresSpec(NamedTuple):
    key: str
    count: int
    sockets: decode.CommaHyphenIntRangeList

    def __str__(self) -> str:
        return f"gres/{self.key}:{self.count}S()"

    def items(self) -> List[Tuple[str, int]]:
        return _to_items(self)

    @classmethod
    def from_string(cls, _v: str) -> Optional[GresSpec]:
        match = _GRES_REGEX.match(_v)
        if not match:
            return None

        key = match.group(1)
        count = match.group(2)
        sockets = match.group(3)

    def _format_sockets(self) -> str:
        return f"gres/{self.key}:{self.count}S({self.sockets})"


def _bsct_style_from_string(
    _v: str,
) -> Optional[Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]]:
    v = decode.delimited_list(":", _v)
    v = [decode.dont_care_int(item) for item in v]
    v = [
        _safe_convert.clip_negative(item) if isinstance(item, int) else item
        for item in v
    ]
    try:
        out = (v[0], v[1], v[2], v[3])
    except:
        out = None
    return out


def _to_bsct_style_string(
    _v: Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]
) -> str:
    out = [_common.DONT_CARE if item is None else str(item) for item in _v]
    out = ":".join(out)
    return out


def _to_items(self) -> List[Tuple[str, Any]]:
    d = self._asdict()
    return list(zip(d.keys(), d.values()))

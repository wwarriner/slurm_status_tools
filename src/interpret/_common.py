from __future__ import annotations

from typing import Any, Iterable, Optional

NA = "N/A"
NS = "n/s"
DONT_CARE = "*"
UNLIMITED = "UNLIMITED"
YES = "yes"
NO = "no"


def any_none(_value: Iterable[Optional[Any]]) -> bool:
    for v in _value:
        if v is None:
            return True
    return False

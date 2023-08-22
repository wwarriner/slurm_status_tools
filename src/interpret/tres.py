from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from src.interpret import decode
from src.interpret._common import any_none

# TODO: https://slurm.schedmd.com/tres.html
# consider each type separately
# BB - str
# Billing - int
# CPU - int
# Energy - int
# FS - str
# GRES - needs special handling TODO
# IC - str
# License - str
# Mem - int | mem-str (400G, 1T)
# Node - int
# Pages - int
# VMem - int


# TODO WORKING HERE


class TresSpec:
    allowed = [
        "bb",
        "billing",
        "cpu",
        "energy",
        "fs",
        "gres",
        "ic",
        "license",
        "mem",
        "node",
        "pages",
        "vmem",
    ]
    _GRES = "gres"
    _LISTS = ["bb", "fs", "ic", "license"]
    _NUMERICS = ["billing", "cpu", "energy", "mem", "node", "pages", "vmem"]

    def __init__(self, _parts: Dict[str, Any]) -> None:
        self._parts = _parts

    def __str__(self) -> str:
        tokens = []
        for k, v in self._parts.items():
            if k == "gres":
                token = self._format_gres(k, v)  # type: ignore
            elif isinstance(v, list):
                token = self._format_list(k, v)
            else:
                token = f"{k}={v}"
            tokens.append(token)

        return ",".join(tokens)

    def items(self) -> List[Tuple[str, Any]]:
        items: List[Tuple[str, Any]] = []
        for k, v in self._parts.items():
            if k == "gres":
                gres_keys = [self._format_gres_key(k, gk) for gk in v.keys()]
                gres_values = list(v.values())
                new_items = list(zip(gres_keys, gres_values))
            elif isinstance(v, list):
                new_items = [(k, self._format_list_values(v))]
            else:
                new_items = [(k, v)]
            items.extend(new_items)

        return items

    @classmethod
    def from_string(cls, _v: str) -> Optional[TresSpec]:
        tokens = decode.delimited_list(",", _v)
        token_parts = [decode.separated_key_value("=", token) for token in tokens]

        keys = [p[0] for p in token_parts]
        keys = [k.casefold() for k in keys]

        values = [p[1] for p in token_parts]
        parts = {k: v for k, v in zip(keys, values)}
        if any_none(parts.keys()) or any_none(parts.values()):
            return None

        tres_parts: Dict[str, Any] = {}
        gres: Dict[str, Optional[int]] = {}
        lists: Dict[str, List[str]] = {}
        for k, v in parts.items():
            if k.startswith(cls._GRES):
                gres_key = k.split("/", maxsplit=1)[-1]

                try:
                    gres_value = int(v)
                except:
                    gres_value = None

                gres[gres_key] = gres_value

                if cls._GRES not in tres_parts:
                    tres_parts[cls._GRES] = gres
            elif k in cls._LISTS:
                if k in lists:
                    lists[k].append(v)
                else:
                    lists[k] = []

                lists[k].append(v)

                if k not in tres_parts:
                    tres_parts[k] = lists[k]
            elif k in cls._NUMERICS:
                if k in tres_parts:
                    curr_v = tres_parts[k]
                else:
                    curr_v = 0

                try:
                    curr_v = curr_v + int(v)
                    numeric = curr_v
                except:
                    numeric = None

                tres_parts[k] = numeric
            else:
                assert False

        tres_spec: TresSpec = TresSpec(tres_parts)
        return tres_spec

    @staticmethod
    def _format_list(k: str, v: List[str]) -> str:
        return f"{k}={TresSpec._format_list_values(v)}"

    @staticmethod
    def _format_list_values(v: List[str]) -> str:
        return ",".join(v)

    @staticmethod
    def _format_gres(k: str, d: Dict[str, int]) -> str:
        return ",".join([TresSpec._format_gres_item(k, dk, dv) for dk, dv in d.items()])

    @staticmethod
    def _format_gres_item(k: str, dk: str, dv: int) -> str:
        gres_key = TresSpec._format_gres_key(k, dk)
        return f"{gres_key}={dv}"

    @staticmethod
    def _format_gres_key(k: str, dk: str) -> str:
        return f"{k}/{dk}"

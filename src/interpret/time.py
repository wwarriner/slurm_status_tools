import datetime as dt
import re
from typing import Optional

_S = r"(?P<seconds>\d{2})"
_M = r"(?P<minutes>\d{2})"
_H = r"(?P<hours>\d{2})"
_D = r"(?P<days>\d+)"

_M_REGEX_STRING = f"^{_M}$"
_MS_REGEX_STRING = f"^{_M}:{_S}$"
_HMS_REGEX_STRING = f"^{_H}:{_M}:{_S}$"
_DH_REGEX_STRING = f"^{_D}-{_H}$"
_DHM_REGEX_STRING = f"^{_D}-{_H}:{_M}$"
_DHMS_REGEX_STRING = f"^{_D}-{_H}:{_M}:{_S}$"
_DURATION_REGEX_STRINGS = [
    _M_REGEX_STRING,
    _MS_REGEX_STRING,
    _HMS_REGEX_STRING,
    _DH_REGEX_STRING,
    _DHM_REGEX_STRING,
    _DHMS_REGEX_STRING,
]  # do not change order! see: https://slurm.schedmd.com/sbatch.html#OPT_time
_DURATION_REGEX = [re.compile(s) for s in _DURATION_REGEX_STRINGS]


def duration_timedelta(_v: str) -> Optional[dt.timedelta]:
    """
    "2-03:04:05" -> dt.timedelta(days=2, seconds=11045)
    """
    for regex in _DURATION_REGEX:
        match = regex.match(_v)
        if match is None or match.group() == "":
            continue
        parts = {k: int(v) for k, v in match.groupdict().items()}
        td = dt.timedelta(**parts)
        return td
    return None


def seconds_timedelta(_v: str) -> Optional[dt.timedelta]:
    try:
        return dt.timedelta(seconds=int(_v))
    except:
        return None


def minutes_timedelta(_v: str) -> Optional[dt.timedelta]:
    try:
        return dt.timedelta(minutes=int(_v))
    except:
        return None


_TIMEPOINT_FORMAT_STRING = r"%Y-%m-%dT%H:%M:%S"


def timepoint_datetime(_v: str) -> Optional[dt.datetime]:
    """ """
    try:
        return dt.datetime.strptime(_v, _TIMEPOINT_FORMAT_STRING)
    except:
        return None

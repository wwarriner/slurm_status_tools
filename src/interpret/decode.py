from __future__ import annotations

from typing import List, NamedTuple, Optional, Tuple

from src.interpret import _common, _safe_convert

"""
These functions decode strings into collections of strings.
"""


def na_str(_v: str) -> Optional[str]:
    out = _safe_convert.convert_value_unsafe_to_none(_common.NA, _v)
    return out


def ns_int(_v: str) -> Optional[int]:
    out = _safe_convert.convert_value_unsafe_to_none(_common.NS, _v)
    out = _safe_convert.type_cast_value_unsafe_to_none(int, out)
    return out


def dont_care_int(_v: str) -> Optional[int]:
    out = _safe_convert.convert_value_unsafe_to_safe(_common.DONT_CARE, None, _v)
    out = _safe_convert.type_cast_value_unsafe_to_none(int, out)
    out = _safe_convert.restrict_negative_value_to_none(out)
    return out


def unlimited_int(_v: str) -> Optional[int]:
    out = _safe_convert.convert_value_unsafe_to_safe(_common.UNLIMITED, None, _v)
    out = _safe_convert.type_cast_value_unsafe_to_none(int, out)
    out = _safe_convert.restrict_negative_value_to_none(out)
    return out


def yes_no_bool(_v: str) -> Optional[bool]:
    out = _safe_convert.convert_value_to_bool_safe(_common.YES, _common.NO, _v)
    return out


def int_bool(_v: str) -> Optional[bool]:
    out = _safe_convert.convert_value_to_bool_safe("0", "1", _v)
    return out


def comma_separated_list(_v: str) -> List[str]:
    out = delimited_list(",", _v)
    return out


def comma_separated_key_value_list(_v: str) -> List[Tuple[str, str]]:
    out = delimited_list(",", _v)
    out = [separated_key_value("=", item) for item in out]
    return out


# TODO test
class CommaHyphenIntRangeList(NamedTuple):
    ranges: List[Tuple[int, int]]

    def __str__(self) -> str:
        items: List[str] = []
        for r in self.ranges:
            if r[0] == r[1]:
                items.append(f"{r[0]}")
            else:
                items.append(f"{r[0]}-{r[1]}")
        return ",".join(items)

    @classmethod
    def from_string(cls, _v: str) -> Optional[CommaHyphenIntRangeList]:
        items = delimited_list(",", _v)

        range_strings: List[Tuple[str, str]] = []
        for item in items:
            range_strings.append(ranged("-", item))
        if _common.any_none(range_strings):
            return None

        ranges: List[Optional[Tuple[int, int]]] = []
        for range_str in range_strings:
            try:
                v0 = int(range_str[0])
            except:
                v0 = None

            try:
                v1 = int(range_str[1])
            except:
                v1 = None

            if v0 is not None and v1 is not None:
                r = (v0, v1)
            elif v0 is not None:
                r = (v0, v0)
            elif v1 is not None:
                r = (v1, v1)
            else:
                r = None

            ranges.append(r)

        if _common.any_none(ranges):
            return None

        out = CommaHyphenIntRangeList(ranges)  # type: ignore
        return out


# TODO implementation details: hide these
def delimited_list(_delimiter: str, _v: str) -> List[str]:
    """
    _delimiter must be punctuation or a separator (unicode P?|Z?)
    _v elements must not contain punctuation or separators (unicode P?|Z?)

    x,y,... -> [x, y, ...]
    '' -> ['']
    """
    return _v.split(_delimiter)


def ranged(_separator: str, _v: str) -> Tuple[str, str]:
    """
    ("-", "x-y") -> ("x", "y")
    ("-", "-y") -> None
    ("-", "x+y") -> None
    """
    r = _v.split(_separator, maxsplit=1)
    if len(r) < 2:
        r.append("")
    return (r[0], r[1])


def enclosed(_brackets: Tuple[str, str], _v: str) -> Optional[str]:
    """
    _brackets must be punctuation or separator (unicode P?|Z?)
    _v must not contain _brackets

    l...r -> ...
    """
    if not (_v.startswith(_brackets[0]) and _v.endswith(_brackets[1])):
        return None

    left = _v.find(_brackets[0]) + 1
    right = _v.rfind(_brackets[1])
    return _v[left:right]


def separated_key_value(_separator: str, _v: str) -> Tuple[str, str]:
    v = _v.split(_separator, maxsplit=1)
    k = v[0]
    v = v[1]
    return (k, v)


# TODO NOTES
"""
JOBS
account, LINUXID
admincomment, str
allocnode:sid, ALLOCNODE_SID
arraytaskthrottle, int
batchflag, bool
burstbuffer, _unsupported
clusters, _unsupported
clusterfeatures, _unsupported
command, PurePath
comment, str
contiguous, bool
cpus/task, int
dependency, NULLABLE[DEPENDENCY]
features, FEATURES
gres,DL[",",GRES_SPEC]
groupid, LINUXID
jobid, int
jobname, str
jobstate, str
licenses, LICENSES
mcs_label, NULLABLE[str]
mincpusnode, int
network, _unsupported
nice, int
numcpus, int
numnodes, int
numtasks, int
oversubscribe, OVERSUBSCRIBE
partition, str
power, _unsupported
priority, int
qos, str
reason, str
reboot, int
requeue, int
restarts, int
stderr, PurePath
stdin, PurePath
stdout, PurePath
userid, LINUXID
workdir, PurePath

state?????


corespec, NONNEGATIVE ^ DONT_CARE_INT
socks/node, NONNEGATIVE ^ DONT_CARE_INT

delayboot, DURATION
runtime, DURATION
timelimit, DURATION
timemin, DURATION

accruetime, TIMEPOINT
deadline, TIMEPOINT
eligibletime, TIMEPOINT
endtime, TIMEPOINT
lastschedeval, TIMEPOINT
preempttime, TIMEPOINT
presustime, TIMEPOINT
starttime, TIMEPOINT
submittime, TIMEPOINT
suspendtime, TIMEPOINT

secspresuspend, SECONDS

tres, TRES

batchhost, NODELIST
excnodelist, NODELIST
nodelist, NODELIST
reqnodelist, NODELIST

minmemorycpu, MEMORY_SPEC
minmemorynode, MEMORY_SPEC
mintmpdisknode, MEMORY_SPEC
"""

"""
NODES
activefeatures, FEATURES
arch, str
availablefeatures, FEATURES
boards, int
boottime, TIMEPOINT
corespeccount, int
corespersocket, int
cpualloc, int
cpubind, str
cpuload, float
cpus, int
cpuspeclist, DL[",", int]
cputot, int
feature, FEATURE
gres, DL[",",GRES_SPEC]
mcs_label, NULLABLE[str]
nodeaddr, str
nodehostname, str
nodename, str
os, str
owner, NULLABLE[str]
partitions, DL[",", str]
port, RANGE[int]??
reason, str
slurmdstarttime, TIMEPOINT
sockets, int
socketsperboard, int
state, str
threadspercore, int
version, str
weight, int


allocmem, MEMORY_MB
freemem, MEMORY_MB
memspeclimit, MEMORY_MB
realmemory, MEMORY_MB
tmpdisk, MEMORY_MB

capwatts, NS_STR
consumedjoules, NS_STR
currentwatts, NS_STR
extsensorsjoules, NS_STR
extsensorstemp, NS_STR
extsensorswatts, NS_STR
lowestjoules, NS_STR

tresweights, COMPUTABLE_WEIGHTS

alloctres, TRES
cfgtres, TRES
"""


"""
PARTITIONS
allocnodes, str
allowaccounts, str
allowgroups, D[",",str]
allowqos, str
alternate, str
cpubind, str
defaulttime, str
denyaccounts, DL[",",str]
denyqos, DL[",",str]
gracetime, int
jobdefaults, ?? NOT DOCUMENTED
minnodes, int
oversubscribe, str
overtimelimit, str
partitionname, str
preemptymode, str
priorityjobfactor, int
prioritytier, int
qos, D[",",str]
selecttypeparameters, str
state, str
totalcpus, int
totalnodes, int

maxcpuspernode, NONNEGATIVE ^ UNLIMITED_INT
maxmempercpu, NONNEGATIVE ^ UNLIMITED_INT
maxnodes, NONNEGATIVE ^ UNLIMITED_INT

defmempercpu, UNLIMITED_MEMORY_MB
defmempernode, UNLIMITED_MEMORY_MB
maxmempernode, UNLIMITED_MEMORY_MB

default, BOOL_YES_NO
disablerootjobs, BOOL_YES_NO
exclusiveuser, BOOL_YES_NO
lln, BOOL_YES_NO
hidden, BOOL_YES_NO
reqresv, BOOL_YES_NO
rootonly, BOOL_YES_NO

maxtime, DURATION

tresbillingweights, COMPUTABLE_WEIGHTS

nodes, NODELIST
"""


"""
QOSES
description, str
flags, DL[",",str]
grpjobs, int
grpjobsaccrue, int
grpsubmitjobs, int
maxjobsaccrueperaccount, int
maxjobsaccrueperuser, int
maxjobsperaccount, int
maxjobsperuser, int
minpriothreshold, int
maxsubmitjobsperaccount, int
maxsubmitjobsperuser, int
name, str
preempt, DL[",",str]
preemptmode, str
priority, int
usagefactor, float
usagethreshold, float

gracetime, DURATION
grpwall, DURATION
maxwall, DURATION

grptresmins, MINUTES
grptresrunmins, MINUTES
maxtresmins, MINUTES

grptres, TRES
maxtresperaccount, TRES
maxtresperjob, TRES
maxtrespernode, TRES
maxtresperuser, TRES
mintresperjob, TRES
"""


"""
TRES types
cpu, int
mem, MEMORY_SPEC
gres/gpu, int


"""


"""
SPECIAL PARSERS

CASTS

- BOOL[LITERAL, LITERAL] -> bool:
    - string literals for (True, False)
- NULLABLE[TYPE, LITERAL] -> Union[TYPE, None]:
    - Literal determines which values to transform to None
- RANGE -> List[int]:
    - expands "x-y" where x<=y into consecutive integers from x to y, inclusive
      as DL[",",int]
- NONNEGATIVE_FLOAT -> float>=0.0
- DURATION -> NULLABLE[dt.timedelta]:
    - [%d-]%H:%M:%S (%d interpreted as arbitrarily large integer)
    - Literal["UNLIMITED"] -> None
- SECONDS -> NULLABLE[dt.timedelta]:
    - %S (%S interpreted as arbitrarily large integer)
    - Literal["N/A"] -> None]
- MINUTES -> NULLABLE[dt.timedelta]:
    - %M (%M interpreted as arbitrarily large integer)
- TIMEPOINT -> NULLABLE[dt.datetime]:
    - %Y-%m-%dT%H:%M:%S
    - Literal["N/A"] -> None
- MEMORY_MB -> int:
    - convert to bytes, multiply by 1024*1024
- UNIT_SUFFIXED[TYPE, Dict[str, TYPE]] -> TYPE:
    - collapses value onto "base" unit
    - suffixes and conversion factors given in Dict[str, TYPE]
- UNCONSTRAINED_INT[LITERAL] -> int:
    - int
    - LITERAL assign to 0, represents no constraint

DECODERS

- ENCLOSED[CAST] -> "[" + CAST + "]"
- DELIMITED_LIST[DELIMITER, CAST[TYPE]] (DL) -> List[TYPE]:
    - splits into list of type elements
- NODELIST -> DERL[","] -> List[NODE_SPEC]
- NODE_SPEC -> str

COMPOSITIONS

- K=V[TYPE] -> DL["=",CAST[TYPE]]
- COMPUTABLE_WEIGHTS -> DL[",",K=V[NONNEGATIVE_FLOAT]]
- TRES -> DL[",",K=V[UNKNOWN]] -> dict[str, UNKNOWN]:
    - UNKNOWN type because there may be different types based on the TRES
- UNCONSTRAINED_MEMORY_MB[LITERAL] -> UNCONSTRAINED_INT[LITERAL]:
    - convert to bytes, multiply by 1024*1024

18. GRES_SPEC -> "gres:gpu:#"
7. LINUXID -> NamedTuple:
    - str: name
    - int: id
- NULLABLE[str] -> Union[str, None]:
    - Literal["N/A"] -> None
9. NULLABLE[DEPENDENCY] -> Union[List[DEPENDENCY_SPEC], None]:
    - Literal["(null)"] -> None
10. DEPENDENCY_SPEC -> NamedTuple:
    - str: Literal["after", "afterany", "afternotok", "afterok", "singleton"]
    - NULLABLE[DELIMITED_LIST[int]] -> Union[List[int], None]: job ids (if not "singleton")
12. NAMED_COUNT -> NamedTuple:
    - name: str
    - count: int
12. ALLOCNODE_SID -> DL[":",Union[str,int]] -> NamedTuple:
    - allocnode: str
    - system_id: int
15. FEATURES -> NULLABLE[??]
    - complex, do this another time...for now just use a str
16. OVERSUBSCRIBE -> LITERAL["yes", "no", "ok"]
    - docs are inconsistent with implementation...
17. LICENSES -> DL[",",NAMED_COUNT] -> List[NamedTuple]

SPECIALIZATIONS

- BOOL_YES_NO -> BOOL[LITERAL["YES"], LITERAL["NO"]]


- MEMORY_SPEC -> UNIT_SUFFIXED[int, LITERAL["k", "m", "g", "t", "p", "e"]] -> int
13. MEMORY_SOURCE_SPEC -> UNIT_SUFFIXED[MEMORY_SPEC, LITERAL["n", "c"]] -> int:
    - conversion factors are dynamic from job
    - n: _v*nodes
    - c: _v*cores/node*nodes

"""

"""
glossary

GRES - Generic Resources
TRES - Trackable Resources
n/s - Not Supported
n/a - Not Available
(null) - No value
"""

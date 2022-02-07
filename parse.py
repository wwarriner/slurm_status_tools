import datetime as dt
import math
import multiprocessing as mp
import re
import subprocess
from pathlib import Path, PurePath
from typing import Dict, List, Optional

import pandas as pd

NODE = "node"
JOB = "job"
PARTITION = "partition"

GPU_SCONTROL_JOB_REGEX = re.compile(r"IDX:([0-9,-]+)")
GPU_SCONTROL_NODE_REGEX = re.compile(r"gpu:.*?:([0-9]+)?")

NAME_N = "NodeName"
REASON_N = "Reason"
CPUTOT_N = "CPUTot"
CPUALLOC_N = "CPUAlloc"
REALMEMORY_MB_N = "RealMemory"
MEMSPECLIMIT_MB_N = "MemSpecLimit"
ALLOCMEM_MB_N = "AllocMem"
GRES_N = "Gres"
PARTITIONS_N = "Partitions"

NODES_J = "Nodes"
GRES_IDX_J = "GRES_IDX"

PARTITION_NAME_P = "PartitionName"
QOS_P = "QoS"
MAXNODES_P = "MaxNodes"
MAXTIME_P = "MaxTime"
NODES_P = "Nodes"
PRIORITYTIER_P = "PriorityTier"
TOTALCPUS_P = "TotalCPUs"
TOTALNODES_P = "TotalNodes"

DURATION_REGEX_STRING = (
    r"((?P<days>\d+)-)?((?P<hours>\d+))?:((?P<minutes>\d+))?:((?P<seconds>\d+))?"
)
DURATION_REGEX = re.compile(DURATION_REGEX_STRING)


SEP = "|"


def snapshot_interface(
    generate_test: bool, run_test: bool, test_folder: Optional[PurePath] = None
) -> "Snapshot":
    snapshot = Snapshot()
    if run_test or generate_test:
        if test_folder is None:
            test_folder = PurePath("test")
        snapshot.test_folder = test_folder
        if generate_test:
            snapshot.take()
            snapshot.write_test()
        if run_test:
            snapshot.read_test()
    else:
        snapshot.take()

    return snapshot


def snapshot_scontrol(source: str, *flags) -> str:
    """
    Takes a snapshot of the output of one call to `scontrol -o show *`. Returns
    values as a string. Source must be one of `job` or `node`. Additional flags
    such as `-d` may be supplied.
    """
    assert source in (NODE, JOB, PARTITION)

    result = subprocess.run(
        args=["scontrol", "-o", "show", source, *flags],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        raise RuntimeError()
    else:
        return result.stdout.decode(encoding="utf-8")


def parse_scontrol(lines: List[str], sep: str = SEP) -> pd.DataFrame:
    """
    Parses output of scontrol -o *. The command returns one record per line
    (node or job). Each line has quasi-flag-style args that look like the
    following:
    ```
    a=foo b=bar c=hello world d=something=actual value
    ```

    The model is that each arg is composed of a field and a value as
    <field>=<value>. Values have spaces, but the field-value pairs are space
    separated and values can contain equals symbols, so we have to take care in
    parsing.

    The approach to parsing this involves parsing in reverse order. We loop over
    the contents of a line, popping them from the back end and putting them in
    both the field and value. When we hit an equals character, we reset the
    field and change state (equals_hit=True). When we hit a space while
    equals_hit=True, we assume we've just finished collecting the actual field,
    so we turn the field and value into strings, remove the field and leading
    `=` from the value string, and strip leading and trailing whitespace. Then
    we add the results to a dict with field as key and value as value. We reset
    state and continue parsing the line.

    This method will not work for a value that has a space followed by an equals
    sign. But then the problem becomes ill-posed because we can no longer
    distinguish when a field-value pair ends, so we hope dearly that this never
    happens. If it does we'll have to stop using the `-o` flag.

    The input sep is used to create a delimited string when a field appears
    multiple times. Notably this occurs for "Nodes" and "GRES_IDX" in `scontrol
    -o show jobs`.
    """
    all_data: List[Dict[str, str]] = []
    for line_s in lines:
        line_data: Dict[str, List[str]] = {}
        line: List[str] = list(line_s)
        field: List[str] = []
        value: List[str] = []
        equals_hit = False
        while line:
            c: str = line.pop()
            field.append(c)
            value.append(c)
            if c == "=":
                field = []
                equals_hit = True
            if (c == " " or not line) and equals_hit:
                field_s: str = "".join(field[::-1])
                field_s = field_s.strip()

                value_s: str = "".join(value[::-1])
                value_s = value_s.replace(field_s, "", 1)
                value_s = value_s.replace("=", "", 1)
                value_s = value_s.strip()

                if field_s in line_data:
                    line_data[field_s].append(value_s)
                else:
                    line_data[field_s] = [value_s]

                field = []
                value = []
                equals_hit = False

        line_data.pop("", None)
        line_df_data: Dict[str, str] = {k: sep.join(v) for k, v in line_data.items()}
        all_data.append(line_df_data)
        # TODO deal with the case where multiple nodes are requested. Will get multiple of some columns!

    df = pd.DataFrame(all_data)
    df = df.fillna("")
    return df


def parse_gpu_scontrol_node(gres_s: str) -> int:
    """
    Parses count of gpus from the `gres` field from `scontrol -o show node`. The
    form is a comma separated list of `gpu:<name>:<count>`. Returns an integer.
    """
    gpus_total = 0
    if "(null)" in gres_s:
        return gpus_total
    gres_l = gres_s.split(",")
    for gres in gres_l:
        matches = re.findall(pattern=GPU_SCONTROL_NODE_REGEX, string=gres)
        values = [int(m) for m in matches]
        gpus_total += sum(values)
    return gpus_total


def parse_gpu_scontrol_node_all(s: pd.Series) -> pd.Series:
    out = s.apply(parse_gpu_scontrol_node)
    assert isinstance(out, pd.Series)
    return out


def parse_gpu_scontrol_job(gres_s: str) -> int:
    """
    Parses count of gpus from the `GRES_IDX` field from `scontrol -o show job`.
    The form is a comma separated list of `gpu(IDX:<csl of #>)`. Note the nested
    comma separated list. Returns an integer.
    """
    gpus_total = 0
    if not isinstance(gres_s, str):
        return gpus_total
    gres_l = gres_s.split(",")
    for gres in gres_l:
        matches = re.findall(pattern=GPU_SCONTROL_JOB_REGEX, string=gres)
        values = [len(_parse_csl(m)) for m in matches]
        gpus_total += sum(values)
    return gpus_total


# def list_of_delim_to_dict


def parse_delimited_gpu_scontrol_job(
    node_s: str, gres_s: str, sep: str = SEP
) -> Dict[str, int]:
    """
    Parses delimited list of "Nodes" and "GRES_IDX" fields for a job with
    multiple nodes. Result comes from  `scontrol -o show job`. For sep="|",
    lists have the form

    c0123|c[0125-0126]|c0199
    gpu(IDX:0)|gpu(IDX:0)|gpu(IDX:0,2-5)

    This function returns a dict whose keys are node names and values are gpu
    counts.
    """
    all_nodes = []
    all_gpus = []
    nodelists = node_s.split(sep=sep)
    greslists = gres_s.split(sep=sep)
    for nodelist, gres in zip(nodelists, greslists):
        gpu_count = parse_gpu_scontrol_job(gres_s=gres)
        if "[" in nodelist:  # csl-style range
            nodes = _parse_nodelist(nodelist, sep=sep)
            nodes = nodes.split(sep=sep)
            gpus = [gpu_count] * len(nodes)
        else:  # single
            nodes = [nodelist]
            gpus = [gpu_count]
        all_nodes.extend(nodes)
        all_gpus.extend(gpus)

    out = {}
    for n, g in zip(all_nodes, all_gpus):
        out[n] = g
    return out


def parse_gpu_scontrol_job_all(
    node_s: pd.Series, gres_s: pd.Series, sep: str = SEP
) -> pd.DataFrame:
    ds = []
    for n, g in zip(node_s, gres_s):
        d = parse_delimited_gpu_scontrol_job(node_s=n, gres_s=g, sep=sep)
        ds.append(d)

    all_gpus = {}
    for d in ds:
        for n, g in d.items():
            if n not in all_gpus:
                all_gpus[n] = 0
            all_gpus[n] += g

    out = pd.DataFrame.from_dict(all_gpus, orient="index", columns=[GRES_IDX_J])
    out.index = out.index.set_names([NODES_J])
    return out


def available(df: pd.DataFrame) -> pd.Series:
    """
    Nodes that have a reason are unavailable. If the column contains `na` then
    there is NO reason, so they are available, so we negate.
    """
    out = (df[REASON_N].isna()) | (df[REASON_N] == "")
    return out


# def get_unique_from_delimited(v: List[str], sep=",") -> List[str]:
#     """
#     Input is a list of delimited strings. Output is a list of all unique strings
#     found across input.
#     """

#     out = set()
#     for s in v:
#         values = s.split(sep=sep)
#         out |= set(values)
#     out = sorted(list(out))
#     return out


def _parse_csl(csl: str) -> List[int]:
    """
    Utility to parse comma-separated lists of integers that can contain
    hyphenated ranges. Returns an explicit list of integers.

    NOTE: We could save memory by turning this into a generator. It would work
    by returning list values. When the list is empty we pop the next token.
    Tokens would be single values or a range, with its trailing comma (if there
    is one). We almost certainly won't need to do this.
    """
    if csl == "":
        return []
    values = []
    ranges = csl.split(",")
    for r in ranges:
        try:
            v = int(r)
            values.append(v)
        except:
            extremes = [int(x) for x in r.split("-")]
            max_v = max(extremes)
            min_v = min(extremes)
            v = list(range(min_v, max_v + 1))
            values.extend(v)
    return values


def _parse_nodelist(nodelist: str, sep: str = SEP, digit_count: int = 4) -> str:
    f = "{:0" + str(digit_count) + "d}"
    n = nodelist.lstrip("c[")
    n = n.rstrip("]")
    ni = _parse_csl(csl=n)
    ns = ["c" + f.format(x) for x in ni]
    n = sep.join(ns)
    return n


def duration_to_dh(duration: str) -> str:
    d = DURATION_REGEX.match(duration)
    if d is None:
        out = "unknown duration"
    else:
        units = ("days", "hours", "minutes", "seconds")
        values = [d.group(k) for k in units]
        parts_of_td = {k: float(x) for k, x in zip(units, values) if x is not None}
        td = dt.timedelta(**parts_of_td)

        days = td.days
        seconds = td.seconds
        hours, _ = divmod(seconds, 3600)
        out = f"{hours: >2d} hours"
        if 0 < days:
            out = f"{days: >d} days, " + out
    return out


def duration_to_h(duration: str) -> str:
    d = DURATION_REGEX.match(duration)
    if d is None:
        out = "unknown duration"
    else:
        units = ("days", "hours", "minutes", "seconds")
        values = [d.group(k) for k in units]
        parts_of_td = {k: float(x) for k, x in zip(units, values) if x is not None}
        td = dt.timedelta(**parts_of_td)

        days = td.days
        seconds = td.seconds + days * 86400
        hours, _ = divmod(seconds, 3600)
        out = f"{hours:d}"

    return out


def _fillna_extended(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna("")
    df = df.replace("N/A", "")
    df = df.replace("n/a", "")
    return df


class Snapshot:
    _SOURCES = {
        NODE: {
            PARSER: parse_scontrol,
            ARGS: {COMMAND: SCONTROL, FLAGS: ("-o", SHOW, NODE,)},
        },
        JOB: {
            PARSER: parse_scontrol,
            ARGS: {COMMAND: SCONTROL, FLAGS: ("-o", SHOW, JOB, "-d")},
        },
        PARTITION: {
            PARSER: parse_scontrol,
            ARGS: {COMMAND: SCONTROL, FLAGS: ("-o", SHOW, PARTITION,)},
        },
        QOS: {
            PARSER: parse_pipe_separated,
            ARGS: {COMMAND: SACCTMGR, FLAGS: (SHOW, QOS, "-P",)},
        },
    }

    def __init__(self, test_folder: Optional[PurePath] = None):
        self._test_folder = test_folder
        self._data = None
        self._dataframes = None

    @property
    def sources(self) -> List[str]:
        return list(self._SOURCES.keys())

    @property
    def test_folder(self) -> Optional[PurePath]:
        return self._test_folder

    @test_folder.setter
    def test_folder(self, value: PurePath) -> None:
        self._test_folder = value

    def __getitem__(self, source: str) -> pd.DataFrame:
        if self._dataframes is None:
            self._parse_dataframes()
        assert self._dataframes is not None
        return self._dataframes[source]

    def take(self):
        """
        Takes a snapshot of scontrol -o show *sources.
        """
        process_count = self._process_count
        with mp.Pool(process_count) as pool:
            results = {}
            for source in self.sources:
                results[source] = pool.apply_async(
                    func=snapshot_command_output, kwds=self._SOURCES[source][ARGS],
                )
            pool.close()
            pool.join()
        out = {k: r.get() for k, r in results.items()}
        self._data = out

    def has_test(self) -> bool:
        """
        Checks whether a complete test case exists.
        """
        assert self._test_folder is not None
        exists = []
        for source in self.sources:
            filepath = self._build_test_path(source=source)
            exists.append(Path(filepath).is_file())
        if len(exists) == 0:
            return False
        elif all(exists):
            return True
        else:
            return False

    def read_test(self):
        """
        Reads a snapshot from test_folder.
        """
        assert self._test_folder is not None
        out = {}
        for source in self.sources:
            filepath = self._build_test_path(source=source)
            with open(filepath, "r") as f:
                data = f.read()
            out[source] = data
        self._data = out

    def write_test(self):
        """
        Writes a snapshot to test_folder.
        """
        assert self._data is not None
        assert self._test_folder is not None
        Path(self._test_folder).mkdir(parents=True, exist_ok=True)
        for source, data in self._data.items():
            filepath = self._build_test_path(source=source)
            with open(filepath, "w") as f:
                f.write(data)

    def _parse_dataframes(self) -> None:
        """
        Converts to a dict of dataframes, one entry per source in self.sources.
        """
        assert self._data is not None
        self._dataframes = {
            k: self._SOURCES[k][PARSER](v.splitlines()) for k, v in self._data.items()
        }

    @property
    def _process_count(self) -> int:
        return len(self.sources)

    def _build_test_path(self, source: str) -> PurePath:
        assert self._test_folder is not None
        filename = source + ".txt"
        filepath = PurePath(self._test_folder) / filename
        return filepath

    @staticmethod
    def _assign(data: Dict[str, str], source: str, value: str) -> None:
        data[source] = value

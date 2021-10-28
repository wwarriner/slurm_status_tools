import argparse
import multiprocessing as mp
import re
import subprocess
from pathlib import Path, PurePath
from typing import Any, Dict, List, Optional, Union

import pandas as pd

PathLike = Union[Path, PurePath, str]


# TODO MemSpecLimit - how to communicate? how to account?


NODE = "node"
JOB = "job"

NAME = "name"
MAGNITUDE = "magnitude"
P_OF_TOTAL = "p_of_total"
P_OF_AVAIL = "p_of_avail"

TOTAL = "Total"
UNAVAILABLE = "Unavailable"
AVAILABLE = "Available"
IDLE = "Idle"
ALLOCATED = "Allocated"

TB_PER_MB = 1e-6

GPU_SCONTROL_JOB_REGEX = re.compile(r"IDX:([0-9,-]+)")
GPU_SCONTROL_NODE_REGEX = re.compile(r"gpu:.*?:([0-9]+)?")


class Snapshot:
    _SOURCE_ARGS = {NODE: ("node",), JOB: ("job", "-d")}

    def __init__(self, test_folder: PurePath):
        self._test_folder = test_folder
        self._data = {}

    @property
    def sources(self) -> List[str]:
        return [x for x in self._SOURCE_ARGS.keys()]

    def take(self):
        """
        Takes a snapshot of scontrol -o show *sources.
        """
        out = {}
        process_count = self._process_count
        with mp.Pool(process_count) as pool:
            for source in self.sources:
                pool.apply_async(
                    func=snapshot_scontrol,
                    args=self._SOURCE_ARGS[source],
                    callback=lambda x: self._assign(out, source, x),
                )
        self._data = out

    def has_test(self) -> bool:
        """
        Checks whether a complete test case exists.
        """
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
        out = {}
        for source in self.sources:
            filepath = self._build_test_path(source=source)
            with open(filepath, "w") as f:
                data = f.read()
            out[source] = data
        self._data = out

    def write_test(self):
        """
        Writes a snapshot to test_folder.
        """
        Path(self._test_folder).mkdir(parents=True, exist_ok=True)
        for source, data in self._data.items():
            filepath = self._build_test_path(source=source)
            with open(filepath, "w") as f:
                f.write(data)

    def to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """
        Converts to a dict of dataframes, one entry per source in self.sources.
        """
        dfs = {k: parse_scontrol(v.splitlines()) for k, v in self._data.items()}
        return dfs

    @property
    def _process_count(self) -> int:
        return len(self.sources)

    def _build_test_path(self, source: str) -> PurePath:
        filename = source + ".txt"
        filepath = PurePath(self._test_folder) / filename
        return filepath

    @staticmethod
    def _assign(data: Dict[str, str], source: str, value: str) -> None:
        data[source] = value


def snapshot_scontrol(source: str, *flags) -> str:
    """
    Takes a snapshot of the output of one call to `scontrol -o show *`. Returns values
    as a string. Source must be one of `job` or `node`. Additional flags such as `-d`
    may be supplied.
    """
    assert source in (NODE, JOB)

    result = subprocess.run(
        args=["scontrol", "-o", "show", source, *flags],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        raise RuntimeError()
    else:
        return result.stdout.decode(encoding="utf-8")


def parse_scontrol(lines: List[str]) -> pd.DataFrame:
    """
    Parses output of scontrol -o *. The command returns one record per line (node or
    job). Each line has quasi-flag-style args that look like the following:
    ```
    a=foo b=bar c=hello world d=something=actual value
    ```

    The model is that each arg is composed of a field and a value as <field>=<value>.
    Values have spaces, but the field-value pairs are space separated and values can
    contain equals symbols, so we have to take care in parsing.

    The approach to parsing this involves parsing in reverse order. We loop over the
    contents of a line, popping them from the back end and putting them in both the
    field and value. When we hit an equals character, we reset the field and change
    state (equals_hit=True). When we hit a space while equals_hit=True, we assume we've
    just finished collecting the actual field, so we turn the field and value into
    strings, remove the field and leading `=` from the value string, and strip leading
    and trailing whitespace. Then we add the results to a dict with field as key and
    value as value. We reset state and continue parsing the line.

    This method will not work for a value that has a space followed by an equals sign.
    But then the problem becomes ill-posed because we can no longer distinguish when a
    field-value pair ends, so we hope dearly that this never happens. If it does we'll
    have to stop using the `-o` flag.
    """
    all_data = []
    for line_s in lines:
        line_data: Dict[str, str] = {}
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
            if c == " " and equals_hit:
                field_s: str = "".join(field[::-1])
                value_s: str = "".join(value[::-1])
                value_s = value_s.replace(field_s, "", 1)
                value_s = value_s.replace("=", "", 1)

                field_s = field_s.strip()
                value_s = value_s.strip()

                line_data[field_s] = value_s

                field = []
                value = []
                equals_hit = False

        line_data.pop("", None)
        all_data.append(line_data)

    df = pd.DataFrame(all_data)
    return df


def summarize(snapshot: Snapshot) -> str:
    dfs = snapshot.to_dataframes()
    df_node = dfs[NODE]

    v_cpu = summarize_total(df=df_node, total_col="CPUTot", alloc_col="CPUAlloc")
    v_mem = summarize_total(
        df=df_node,
        total_col="RealMemory",
        alloc_col="AllocMem",
        magnitude_scale=TB_PER_MB,
    )
    v_gpu = summarize_total(
        df=df_node, total_col="Gres", parse_value_fn=_parse_gpu_scontrol_node,
    )
    total_sum = _get_magnitude(summary=v_gpu, name=TOTAL)
    available_sum = _get_magnitude(summary=v_gpu, name=AVAILABLE)
    df_job = dfs[JOB]
    a_gpu = summarize_allocated(
        df=df_job,
        alloc_col="GRES_IDX",
        total_sum=total_sum,
        available_sum=available_sum,
        parse_value_fn=_parse_gpu_scontrol_job,
    )
    v_gpu.extend(a_gpu)

    s = []
    s.append(summary_to_string(v_cpu, units="cores", dtype=int))
    s.append(summary_to_string(v_mem, units="TB", dtype=float))
    s.append(summary_to_string(v_gpu, units="gpus", dtype=int))

    out = "\n\n".join(s)
    return out


def summarize_total(
    df: pd.DataFrame,
    total_col: str,
    alloc_col: Optional[str] = None,
    magnitude_scale: float = 1.0,
    parse_value_fn=lambda x: x,
    alloc_parse_value_fn=None,
) -> List[Dict[str, Any]]:
    """
    Summarizes values from a data frame based on a supplied all_col. Returns a list of
    dicts in the following order: "Total", "Unavailable", "Available". Each includes a
    magnitude field and a percentage of total field.

    Magnitudes are multiplied by magnitude_scale. This exists to allow for unit conversions.

    An optional parse_value_fn may be supplied that will be mapped over the total_col.
    Same for alloc_parse_value_fn and alloc_col. If alloc_parse_value_fn is None or not
    supplied, then parse_value_fn will be used as alloc_parse_value_fn.

    If alloc_col is supplied, also returns the contents of `summarize_alloc` appended to
    the list of dicts.
    """
    all_values = df[total_col].apply(parse_value_fn)
    total_sum = all_values.astype(dtype=float).sum()
    assert isinstance(total_sum, float)
    unavailable = _get_unavailable(df=df)
    unavailable_sum = all_values[unavailable].astype(dtype=float).sum()
    available_sum = total_sum - unavailable_sum
    assert isinstance(available_sum, float)
    values = [
        {NAME: "Total", MAGNITUDE: total_sum * magnitude_scale},
        {
            NAME: UNAVAILABLE,
            MAGNITUDE: unavailable_sum * magnitude_scale,
            P_OF_TOTAL: _to_percent(unavailable_sum / total_sum),
        },
        {
            NAME: AVAILABLE,
            MAGNITUDE: available_sum * magnitude_scale,
            P_OF_TOTAL: _to_percent(available_sum / total_sum),
        },
    ]
    if alloc_col is not None:
        if alloc_parse_value_fn is None:
            alloc_parse_value_fn = parse_value_fn
        values.extend(
            summarize_allocated(
                df=df,
                alloc_col=alloc_col,
                total_sum=total_sum,
                available_sum=available_sum,
                magnitude_scale=magnitude_scale,
                parse_value_fn=alloc_parse_value_fn,
            )
        )
    return values


def summarize_allocated(
    df: pd.DataFrame,
    alloc_col: str,
    total_sum: float,
    available_sum: float,
    magnitude_scale: float = 1.0,
    parse_value_fn=lambda x: x,
) -> List[Dict[str, Any]]:
    """
    Summarizes values from a data frame based on a supplied alloc_cal. Returns a list of
    dicts in the following order: "Idle", "Allocated". Each includes a magnitude field,
    a percentage of total field, and a percentage of available field. Since the
    percentages must be calculated, the Total magnitude and Available magnitude from
    `summarize` must be supplied. See `summarize` for information on magnitude_scale and
    parse_value_fn.
    """
    allocated_values = df[alloc_col].apply(parse_value_fn)
    allocated_sum = allocated_values.astype(dtype=int).sum()
    idle_sum = available_sum - allocated_sum
    values = [
        {
            NAME: IDLE,
            MAGNITUDE: idle_sum * magnitude_scale,
            P_OF_TOTAL: _to_percent(idle_sum / total_sum),
            P_OF_AVAIL: _to_percent(idle_sum / available_sum),
        },
        {
            NAME: ALLOCATED,
            MAGNITUDE: allocated_sum * magnitude_scale,
            P_OF_TOTAL: _to_percent(allocated_sum / total_sum),
            P_OF_AVAIL: _to_percent(allocated_sum / available_sum),
        },
    ]
    return values


def summary_to_string(
    values: List[Dict[str, Any]], units: str, dtype: type = float
) -> str:
    """
    Formats output of `summarize` as table-esque string. Uses different number
    formatting strategies depending on dtype. Only int and float are currently
    supported.
    """
    out = []

    if dtype == float:
        magnitude_format = " >10.2f"
    elif dtype == int:
        magnitude_format = " >10d"
    else:
        assert False
    percent_format = " >5.1f"

    for v in values:
        s = []
        name = v["name"]
        s.append(f"{name: <15s}")
        magnitude = v.get(MAGNITUDE)
        if magnitude is not None:
            s.append(f"{dtype(magnitude):{magnitude_format}}")
            s.append(f"{units: <6s}")
        p_of_total = v.get(P_OF_TOTAL)
        if p_of_total is not None:
            s.append(f"({p_of_total:{percent_format}}% of total)")
        p_of_avail = v.get(P_OF_AVAIL)
        if p_of_avail is not None:
            s.append(f"({p_of_avail:{percent_format}}% of available)")
        out.append(" ".join(s))
    return "\n".join(out)


def _get_unavailable(df: pd.DataFrame) -> pd.Series:
    """
    Nodes that have a reason are unavailable. If the column contains `na` then there is
    NO reason, so they are available, so we negate.
    """
    return ~df["Reason"].isna()


def _parse_csl(csl: str) -> List[int]:
    """
    Utility to parse comma-separated lists of integers that can contain hyphenated
    ranges. Returns an explicit list of integers.

    NOTE: We could save memory by turning this into a generator. It would work by
    returning list values. When the list is empty we pop the next token. Tokens would be
    single values or a range, with its trailing comma (if there is one). We almost
    certainly won't need to do this.
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


def _parse_gpu_scontrol_node(gres_s: str) -> int:
    """
    Parses count of gpus from the `gres` field from `scontrol -o show node`. The form is
    a comma separated list of `gpu:<name>:<count>`. Returns an integer.
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


def _parse_gpu_scontrol_job(gres_s: str) -> int:
    """
    Parses count of gpus from the `gres` field from `scontrol -o show job`. The form is
    a comma separated list of `gpu(IDX:<csl of #>)`. Note the nested comma separated
    list. Returns an integer.
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


def _get_magnitude(summary: List[Dict[str, Any]], name: str) -> Any:
    """
    Gets the named magnitude from a summary.
    """
    return next(filter(lambda x: x[NAME] == name, summary))[MAGNITUDE]


def _to_percent(v):
    return v * 1e2


def interface() -> None:
    parser = argparse.ArgumentParser(
        description="Tool for quickly checking overall load on a SLURM cluster. Reports CPUs, Memory and GPUs (if under gres). Requires no arguments to run."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Runs a test case using files created from the --generate-test-case flag. If those files do not exist, operates as though that flag were also used, and then runs the test.",
    )
    parser.add_argument(
        "--generate-test-case",
        action="store_true",
        help="Generates a snapshot test-case and saves to disk in the ./test/ subfolder.",
    )
    args = parser.parse_args()

    test = args.test
    generate_test_case = args.generate_test_case

    snapshot = Snapshot(test_folder=PurePath("test"))
    any_test = test or generate_test_case
    if any_test:
        if generate_test_case:
            snapshot.take()
            snapshot.write_test()
        if test:
            snapshot.read_test()
    else:
        snapshot.take()

    out = summarize(snapshot=snapshot)

    print(out)


if __name__ == "__main__":
    mp.freeze_support()
    interface()

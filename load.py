import argparse
import multiprocessing as mp
from pathlib import Path, PurePath
from typing import Any, Dict, List, Optional, Union

import pandas as pd

import parse

PathLike = Union[Path, PurePath, str]


# TODO MemSpecLimit - how to communicate? how to account?
# TODO running_jobs=$(squeue -h --state running | wc -l)
# TODO pending_jobs=$(squeue -h --state pending | wc -l)


NAME = "name"
MAGNITUDE = "magnitude"
P_OF_TOTAL = "p_of_total"
P_OF_AVAIL = "p_of_avail"

TOTAL = "Total"
UNAVAILABLE = "Unavailable"
AVAILABLE = "Available"
IDLE = "Idle"
ALLOCATED = "Allocated"


def summarize(snapshot: parse.Snapshot) -> str:
    TB_PER_MB = 1e-6

    dfs = snapshot.to_dataframes()
    df_node = dfs[parse.NODE]

    v_cpu = summarize_total(df=df_node, total_col="CPUTot", alloc_col="CPUAlloc")
    v_mem = summarize_total(
        df=df_node,
        total_col="RealMemory",
        alloc_col="AllocMem",
        magnitude_scale=TB_PER_MB,
    )
    v_gpu = summarize_total(
        df=df_node, total_col="Gres", parse_value_fn=parse.parse_gpu_scontrol_node,
    )
    total_sum = _get_magnitude(summary=v_gpu, name=TOTAL)
    available_sum = _get_magnitude(summary=v_gpu, name=AVAILABLE)
    df_job = dfs[parse.JOB]
    a_gpu = summarize_allocated(
        df=df_job,
        alloc_col="GRES_IDX",
        total_sum=total_sum,
        available_sum=available_sum,
        parse_value_fn=parse.parse_gpu_scontrol_job,
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
    available = parse.available(df=df)
    available_sum = all_values[available].astype(dtype=float).sum()
    unavailable_sum = total_sum - available_sum
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

    snapshot = parse.snapshot_interface(generate_test=generate_test_case, run_test=test)

    out = summarize(snapshot=snapshot)

    print(out)


if __name__ == "__main__":
    mp.freeze_support()
    interface()

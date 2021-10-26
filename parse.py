import multiprocessing as mp
import re
from pathlib import Path, PurePath
from typing import Any, Dict, List, Optional, Union

import pandas as pd

PathLike = Union[Path, PurePath, str]


# TODO MemSpecLimit - how to communicate? how to account?


GPU_REGEX = re.compile(r"IDX:([0-9,-]+)")
GRES_REGEX = re.compile(r"gpu:.*?:([0-9]+)?")


def parse_scontrol(lines: List[str]) -> pd.DataFrame:
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


def get_unavailable(df: pd.DataFrame) -> pd.Series:
    return ~df["Reason"].isna()


def parse_csl(csl: str) -> List[int]:
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


def parse_gres_scontrol(gres_s: str) -> int:
    gpus_total = 0
    if "(null)" in gres_s:
        return gpus_total
    gres_l = gres_s.split(",")
    for gres in gres_l:
        matches = re.findall(pattern=GRES_REGEX, string=gres)
        values = [int(m) for m in matches]
        gpus_total += sum(values)
    return gpus_total


def parse_gpu(gres_s: str) -> int:
    gpus_total = 0
    if not isinstance(gres_s, str):
        return gpus_total
    gres_l = gres_s.split(",")
    for gres in gres_l:
        matches = re.findall(pattern=GPU_REGEX, string=gres)
        values = [len(parse_csl(m)) for m in matches]
        gpus_total += sum(values)
    return gpus_total


def summarize(
    df: pd.DataFrame,
    all_key: str,
    alloc_key: Optional[str] = None,
    magnitude_scale: float = 1.0,
    parse_value_fn=lambda x: x,
) -> List[dict]:
    all_values = df[all_key].apply(parse_value_fn)
    all_sum = all_values.astype(dtype=float).sum()
    assert isinstance(all_sum, float)
    unavailable = get_unavailable(df=df)
    unavailable_sum = all_values[unavailable].astype(dtype=float).sum()
    available_sum = all_sum - unavailable_sum
    assert isinstance(available_sum, float)
    values = [
        {"name": "Total", "magnitude": all_sum * magnitude_scale},
        {
            "name": "Unavailable",
            "magnitude": unavailable_sum * magnitude_scale,
            "p_of_total": to_percent(unavailable_sum / all_sum),
        },
        {
            "name": "Available",
            "magnitude": available_sum * magnitude_scale,
            "p_of_total": to_percent(available_sum / all_sum),
        },
    ]
    if alloc_key is not None:
        values.extend(
            summarize_allocated(
                df=df,
                alloc_key=alloc_key,
                all_sum=all_sum,
                available_sum=available_sum,
                magnitude_scale=magnitude_scale,
                parse_value_fn=parse_value_fn,
            )
        )
    return values


def summarize_allocated(
    df: pd.DataFrame,
    alloc_key: str,
    all_sum: float,
    available_sum: float,
    magnitude_scale: float = 1.0,
    parse_value_fn=lambda x: x,
) -> List[dict]:
    allocated_values = df[alloc_key].apply(parse_value_fn)
    allocated_sum = allocated_values.astype(dtype=int).sum()
    idle_sum = available_sum - allocated_sum
    values = [
        {
            "name": "Idle",
            "magnitude": idle_sum * magnitude_scale,
            "p_of_total": to_percent(idle_sum / all_sum),
            "p_of_avail": to_percent(idle_sum / available_sum),
        },
        {
            "name": "Allocated",
            "magnitude": allocated_sum * magnitude_scale,
            "p_of_total": to_percent(allocated_sum / all_sum),
            "p_of_avail": to_percent(allocated_sum / available_sum),
        },
    ]
    return values


def values_to_str(values: List[dict], units: str, dtype: type = float) -> str:
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
        magnitude = v.get("magnitude")
        if magnitude is not None:
            s.append(f"{dtype(magnitude):{magnitude_format}}")
            s.append(f"{units: <6s}")
        p_of_total = v.get("p_of_total")
        if p_of_total is not None:
            s.append(f"({p_of_total:{percent_format}}% of total)")
        p_of_avail = v.get("p_of_avail")
        if p_of_avail is not None:
            s.append(f"({p_of_avail:{percent_format}}% of available)")
        out.append(" ".join(s))
    return "\n".join(out)


def to_percent(v):
    return v * 1e2


def read(path: PathLike) -> pd.DataFrame:
    lines = read_lines(path=path)
    df = parse_scontrol(lines=lines)
    return df


def read_lines(path: PathLike) -> List[str]:
    with open(path, "r") as f:
        lines = f.readlines()
    return lines


if __name__ == "__main__":
    mp.freeze_support()

    with mp.Pool(processes=2) as pool:
        TEST_FILE_NODE = PurePath("test_scontrol_node.txt")
        res_node = pool.apply_async(func=read, args=(TEST_FILE_NODE,))

        TEST_FILE_JOB = PurePath("test_scontrol_job.txt")
        res_job = pool.apply_async(func=read, args=(TEST_FILE_JOB,))

        df_node = res_node.get(timeout=10)
        df_job = res_job.get(timeout=10)

    TB_PER_MB = 1e-6
    v_cpu = summarize(df=df_node, all_key="CPUTot", alloc_key="CPUAlloc")
    v_mem = summarize(
        df=df_node,
        all_key="RealMemory",
        alloc_key="AllocMem",
        magnitude_scale=TB_PER_MB,
    )
    v_gpu = summarize(df=df_node, all_key="Gres", parse_value_fn=parse_gres_scontrol)
    df_job["gpualloctotal"] = df_job["GRES_IDX"].apply(parse_gpu)
    all_sum = next(filter(lambda x: x["name"] == "Total", v_gpu))["magnitude"]
    available_sum = next(filter(lambda x: x["name"] == "Available", v_gpu))["magnitude"]
    a_gpu = summarize_allocated(
        df=df_job,
        alloc_key="gpualloctotal",
        all_sum=all_sum,
        available_sum=available_sum,
    )
    v_gpu.extend(a_gpu)

    s = []
    s.append(values_to_str(v_cpu, units="cores", dtype=int))
    s.append(values_to_str(v_mem, units="TB", dtype=float))
    s.append(values_to_str(v_gpu, units="gpus", dtype=int))
    print("\n\n".join(s))

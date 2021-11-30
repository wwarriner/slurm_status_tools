import itertools
from pathlib import Path, PurePath
from typing import Union

import pandas as pd

import parse

PathLike = Union[Path, PurePath, str]

"""
_hardware = Amount of resource that exists.
_pool = Amount of resource SLURM can use for allocating to jobs.
_allocated = Amount of resource currently allocated to jobs.
_offline = Amount of resource offline.
"""

NAME = "name"
AVAILABLE = "available"
REASON = "reason"
PARTITIONS = "partitions"

CORE = "core"
MEMORY_GB = "memory_gb"
GPU = "gpu"
RESOURCES = (CORE, MEMORY_GB, GPU)
NODE = "node"

HARDWARE = "hardware"
POOL = "pool"
ALLOCATED = "allocated"
IDLE = "idle"
# STATES = (HARDWARE, POOL, ALLOCATED, IDLE)
STATES = (POOL, ALLOCATED, IDLE)

COUNT = "count"
TOTAL = "total"
UNAVAILABLE = "unavailable"
RESOURCE = "resource"
SUBSET = "subset"

CORE_COUNT_HARDWARE = "_".join([CORE, COUNT, HARDWARE])
CORE_COUNT_POOL = "_".join([CORE, COUNT, POOL])
CORE_COUNT_ALLOCATED = "_".join([CORE, COUNT, ALLOCATED])
CORE_COUNT_IDLE = "_".join([CORE, COUNT, IDLE])
MEMORY_GB_COUNT_HARDWARE = "_".join([MEMORY_GB, COUNT, HARDWARE])
MEMORY_GB_COUNT_POOL = "_".join([MEMORY_GB, COUNT, POOL])
MEMORY_GB_COUNT_ALLOCATED = "_".join([MEMORY_GB, COUNT, ALLOCATED])
MEMORY_GB_COUNT_IDLE = "_".join([MEMORY_GB, COUNT, IDLE])
GPU_COUNT_HARDWARE = "_".join([GPU, COUNT, HARDWARE])
GPU_COUNT_POOL = "_".join([GPU, COUNT, POOL])
GPU_COUNT_ALLOCATED = "_".join([GPU, COUNT, ALLOCATED])
GPU_COUNT_IDLE = "_".join([GPU, COUNT, IDLE])

PARTITION = "Partition"
TIME_LIMIT_DH = "Time Limit"
TIME_LIMIT_H = "Time Limit (h)"
PRIORITY_TIER = "Priority Tier"
NODES_AVAILABLE = "Nodes"
NODES_PER_USER = "Nodes Per User"
NODE_LIST = "Node List"


MB_TO_GB = 1.0 / 1024.0


class Partitions:
    def __init__(self, snapshot: parse.Snapshot):
        df_state = pd.DataFrame()

        dfs = snapshot.to_dataframes()
        df_partition = dfs[parse.PARTITION]

        df_state[PARTITION] = df_partition[parse.PARTITION_NAME_P]
        df_state[NODES_AVAILABLE] = df_partition[parse.TOTALNODES_P]
        df_state[NODES_PER_USER] = df_partition[parse.MAXNODES_P]
        df_state[TIME_LIMIT_DH] = df_partition[parse.MAXTIME_P].apply(
            parse.duration_to_dh
        )
        df_state[PRIORITY_TIER] = df_partition[parse.PRIORITYTIER_P]
        # df_state[TIME_LIMIT_H] = df_partition[parse.MAXTIME_P].apply(parse.duration_to_h)
        # df_state[NODES] = df_partition[parse.NODES_P]

        df_state[PRIORITY_TIER] = df_state[PRIORITY_TIER].astype(int)
        df_state = df_state.sort_values(by=PRIORITY_TIER, ascending=False)
        df_state[PRIORITY_TIER] = df_state[PRIORITY_TIER].astype(str)

        self._df = df_state

    def to_df(self) -> pd.DataFrame:
        return self._df


class Nodes:
    def __init__(self, snapshot: parse.Snapshot):
        dfs = snapshot.to_dataframes()
        df_job = dfs[parse.JOB]
        df_node = dfs[parse.NODE]
        df_node = self._merge_gpu_job_info(df_job=df_job, df_node=df_node)

        df_state = pd.DataFrame()
        df_state[NAME] = df_node[parse.NAME_N]
        df_state[AVAILABLE] = parse.available(df_node)
        df_state[REASON] = df_node[parse.REASON_N]
        df_state[PARTITIONS] = df_node[parse.PARTITIONS_N]

        fields = itertools.product(RESOURCES, STATES)
        for resource, state in fields:
            field_name = self._to_field_name(resource, state)
            series = self._extract_series(df=df_node, resource=resource, state=state)
            df_state[field_name] = series

        self._df = df_state

    def to_df(self) -> pd.DataFrame:
        return self._df

    @staticmethod
    def _to_field_name(resource: str, state: str) -> str:
        return "_".join([resource, COUNT, state])

    @staticmethod
    def _extract_series(df: pd.DataFrame, resource: str, state: str) -> pd.Series:
        # TODO can we make this table-driven instead of spaghetti?
        if resource == CORE:
            hardware = df[parse.CPUTOT_N].astype(int)
            allocated = df[parse.CPUALLOC_N].astype(int)
            if state in (HARDWARE, POOL):
                out = hardware
            elif state == ALLOCATED:
                out = allocated
            elif state == IDLE:
                out = hardware - allocated
            else:
                assert False
        elif resource == MEMORY_GB:
            hardware = Nodes._normalize_mem(df[parse.REALMEMORY_MB_N])
            reserved = Nodes._normalize_mem(df[parse.MEMSPECLIMIT_MB_N])
            allocated = Nodes._normalize_mem(df[parse.ALLOCMEM_MB_N])
            if state == HARDWARE:
                out = hardware
            elif state == POOL:
                out = hardware - reserved
            elif state == ALLOCATED:
                out = allocated
            elif state == IDLE:
                out = hardware - reserved - allocated
            else:
                assert False
        elif resource == GPU:
            hardware = parse.parse_gpu_scontrol_node_all(df[parse.GRES_N])
            allocated = df[GPU_COUNT_ALLOCATED]
            if state in (HARDWARE, POOL):
                out = hardware
            elif state == ALLOCATED:
                out = allocated
            elif state == IDLE:
                out = hardware - allocated
            else:
                assert False
        else:
            assert False
        return out

    @staticmethod
    def _merge_gpu_job_info(
        df_job: pd.DataFrame, df_node: pd.DataFrame
    ) -> pd.DataFrame:
        gpus = parse.parse_gpu_scontrol_job_all(
            df_job[parse.NODES_J], df_job[parse.GRES_IDX_J], sep=parse.SEP
        )
        gpus = gpus.rename(columns={parse.GRES_IDX_J: GPU_COUNT_ALLOCATED})
        df_node = df_node.merge(
            how="left", right=gpus, left_on=parse.NAME_N, right_on=parse.NODES_J
        )
        df_node[GPU_COUNT_ALLOCATED] = df_node[GPU_COUNT_ALLOCATED].replace(
            float("nan"), value=0
        )
        df_node[GPU_COUNT_ALLOCATED] = df_node[GPU_COUNT_ALLOCATED].astype(int)
        return df_node

    @staticmethod
    def _normalize_mem(s_mb: pd.Series) -> pd.Series:
        s_mb = s_mb.replace("", "0.0")
        s_mb = s_mb.astype(float)
        s_out = s_mb * MB_TO_GB
        return s_out


class NodesSummary:
    ALL_GROUPING = "all"
    PARTITIONS_GROUPING = "partitions"
    GROUPINGS = (ALL_GROUPING, PARTITIONS_GROUPING)

    def __init__(self, nodes: Nodes, grouping: str):
        """
        """
        self._nodes = nodes
        df = nodes.to_df()

        empty_partition = df[PARTITIONS] == ""
        df = df[~empty_partition]

        if grouping == self.PARTITIONS_GROUPING:
            partitions = df.groupby(by=PARTITIONS)
            summarized_dfs = {}
            for label, partition_df in partitions:
                summarized_df = self._summarize(
                    df=partition_df
                )  # TODO error here, check what part looks like
                summarized_dfs[label] = summarized_df
            df_out = pd.concat(summarized_dfs.values(), keys=summarized_dfs.keys())
            df_out = df_out.reset_index(drop=False)
            df_out = df_out.drop("level_1", axis="columns")
            df_out = df_out.rename(mapper={"level_0": PARTITIONS}, axis="columns")

            self._df = df_out
        elif grouping == self.ALL_GROUPING:
            self._df = self._summarize(df=df)
        else:
            assert False

    def to_df(self) -> pd.DataFrame:
        return self._df

    def _summarize(self, df: pd.DataFrame):
        """

        """
        COUNT_INFIX = "_" + COUNT + "_"
        POOL_SUFFIX = "_" + POOL
        TOTAL_SUFFIX = "_" + TOTAL

        compute_nodes = df[NAME].str.contains(r"^c\d+$")
        df = df[compute_nodes]

        df_count = df.filter(like=COUNT_INFIX, axis="columns")

        # total sums of hardware
        COUNT_POOL_FILTER_REGEX = "_".join(["", COUNT, POOL]) + "$"
        df_total = df_count.filter(regex=COUNT_POOL_FILTER_REGEX, axis="columns")
        df_total = df_total.sum()
        df_total.index = df_total.index.str.replace(POOL_SUFFIX, TOTAL_SUFFIX)

        # total count of nodes
        NODE_COUNT_TOTAL = "_".join([NODE, COUNT, TOTAL])
        df_total.loc[NODE_COUNT_TOTAL] = len(df_count)

        # available subset
        df_agg_orig = df_count.copy()
        df_agg_orig[NAME] = df[NAME]
        df_agg_orig[AVAILABLE] = df[AVAILABLE]
        df_agg = df_agg_orig.groupby(by=AVAILABLE)
        if True not in df_agg.groups:
            df_agg = df_agg_orig.iloc[:0, :].copy()
        else:
            df_agg = pd.DataFrame(df_agg.get_group(True))

        # sums of hardware
        count_cols = list(df_count.columns)
        df_agg_sum = df_agg[count_cols].sum()
        df_agg_sum = pd.concat([df_agg_sum, df_total], axis="index")
        df_agg_sum = df_agg_sum.sort_index()

        # count of nodes
        df_agg_count = df_agg[[NAME]].count()
        NODE_COUNT_POOL = "_".join([NODE, COUNT, POOL])
        df_agg_count = df_agg_count.rename(index={NAME: NODE_COUNT_POOL})

        # combine
        df_agg = pd.concat([df_agg_count, df_agg_sum], axis="index")
        df_agg = df_agg.sort_index()

        # pivot
        INDEX = "index"
        VALUE = "value"
        df_agg = df_agg.reset_index(drop=False, name=VALUE)
        df_agg[[RESOURCE, SUBSET]] = df_agg[INDEX].str.split(
            COUNT_INFIX, 1, expand=True
        )
        df_agg = df_agg.drop(labels=[INDEX], axis="columns")
        df_agg[VALUE] = df_agg[VALUE].astype(int)
        df_agg = df_agg.pivot(index=RESOURCE, columns=SUBSET, values=VALUE)
        df_agg = df_agg.reset_index(drop=False)

        # unavailable
        df_agg[UNAVAILABLE] = df_agg[TOTAL] - df_agg[POOL]

        # reorder
        df_agg = df_agg[[RESOURCE, ALLOCATED, IDLE, POOL, UNAVAILABLE, TOTAL]]

        return df_agg


class Load:
    def __init__(self, nodessummary: NodesSummary):
        """
        Pretty print some info about the state of hardware usage on the cluster.

        *_%_available = *_count_available / *_count_hw
        *_%_usage = *_count_allocated / *_count_available
        *_%_allocated = *_count_allocated / *_count_hw

        split on `_count_`
        spread list contents to columns
        unstack on key=left, val=right
        cleanup

        compute percentages
        """

        df = nodessummary.to_df()

        d = df[POOL]
        df.loc[:, "/".join([ALLOCATED, POOL])] = self._to_pct_string(
            n=df[ALLOCATED], d=d
        )
        df.loc[:, "/".join([IDLE, POOL])] = self._to_pct_string(n=df[IDLE], d=d)

        d = df[TOTAL]
        df.loc[:, "/".join([ALLOCATED, TOTAL])] = self._to_pct_string(
            n=df[ALLOCATED], d=d
        )
        df.loc[:, "/".join([IDLE, TOTAL])] = self._to_pct_string(n=df[IDLE], d=d)
        df.loc[:, "/".join([POOL, TOTAL])] = self._to_pct_string(n=df[POOL], d=d)
        df.loc[:, "/".join([UNAVAILABLE, TOTAL])] = self._to_pct_string(
            n=df[UNAVAILABLE], d=d
        )
        df = df.replace(to_replace="nan%", value="")

        df = df.drop(labels=[ALLOCATED, IDLE, POOL, UNAVAILABLE, TOTAL], axis="columns")
        if PARTITIONS in df.columns:
            index_keys = [PARTITIONS, RESOURCE]
        else:
            index_keys = [RESOURCE]
        df = df.set_index(keys=index_keys)
        df = df.stack()
        df = df.unstack(level=1)
        df = df.reset_index(drop=False)
        df = df.rename(mapper={SUBSET: ""}, axis="columns")

        self._df = df

    def to_df(self) -> pd.DataFrame:
        return self._df

    @staticmethod
    def _to_pct_string(n: pd.Series, d: pd.Series) -> pd.Series:
        f = n / d
        out = f.apply(func=lambda x: "{:.1%}".format(x))
        return out


# TODO add formatted partition table for OOD and Shell MOTD banner

import argparse
import multiprocessing as mp
from pathlib import Path, PurePath
from typing import Union

import pandas as pd

import commands
import parse
import styles

PathLike = Union[Path, PurePath, str]


def interface() -> None:
    # TODO allow multiple commands to reuse the same snapshot?
    # may need to allow output_file_name we suffix
    # have to be a little opinionated about it
    args = _get_args()
    test = args.test
    generate_test_case = args.generate_test_case
    command = args.command[0]
    summary = args.summary
    if isinstance(summary, list):
        summary = summary[0]
    style = args.style[0]

    snapshot = parse.snapshot_interface(generate_test=generate_test_case, run_test=test)
    # TODO loop over many commands?
    df = _build(command=command, summary=summary, snapshot=snapshot)
    out = styles.apply_style(
        style=style,
        df=df,
        user_alignments={
            commands.PARTITION: styles.LEFT_ALIGN,
            commands.NAME: styles.LEFT_ALIGN,
        },
        default_alignment=styles.RIGHT_ALIGN,
    )
    print(out, end="")


def _build(command: str, summary: str, snapshot: parse.Snapshot) -> pd.DataFrame:
    # TODO how to cut this spaghetti?
    command = command.casefold()
    if command in ("nodes", "load"):
        nodes = commands.Nodes(snapshot=snapshot)
        if summary is not None:
            nodessummary = commands.NodesSummary(nodes=nodes, grouping=summary)
            if command == "load":
                out = commands.Load(nodessummary=nodessummary).to_df()
            else:
                out = nodessummary.to_df()
        else:
            out = nodes.to_df()
    elif command == "partitions":
        partitions = commands.Partitions(snapshot=snapshot)
        out = partitions.to_df()
        out = out.drop(labels=commands.QOS, axis="columns")
    elif command == "qos":
        qos = commands.QualityOfService(snapshot=snapshot)
        partitions = commands.Partitions(snapshot=snapshot)
        qos.merge_partitions(partitions=partitions)
        out = qos.to_df(empty_value="n/a")
    else:
        assert False

    return out


def _get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tool for quickly checking node state on a SLURM cluster. Reports CPUs, Memory, GPUs (if under gres) and availability. Requires no arguments to run."
    )
    parser.add_argument(
        "-c",
        "--command",
        nargs=1,
        type=str,
        choices=("nodes", "load", "partitions", "qos"),
        default=("load",),
        help="""Command to run.""",
    )
    parser.add_argument(
        "-s",
        "--summary",
        nargs=1,
        type=str,
        choices=commands.NodesSummary.GROUPINGS,
        default=None,
        help="""Summarizes output of --command. Only available for `-c nodes`, ignored otherwise.""",
    )
    parser.add_argument(
        "-f",
        "--style",
        nargs=1,
        type=str,
        choices=styles.STYLES,
        default=(styles.CSV,),
        help="""Formatting of output. `motd` is used for Open OnDemand Message of the Day.""",
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
    return args


if __name__ == "__main__":
    mp.freeze_support()
    interface()

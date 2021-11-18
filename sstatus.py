import argparse
import multiprocessing as mp
from pathlib import Path, PurePath
from typing import Union

import commands
import parse

PathLike = Union[Path, PurePath, str]


def interface() -> None:
    parser = argparse.ArgumentParser(
        description="Tool for quickly checking node state on a SLURM cluster. Reports CPUs, Memory, GPUs (if under gres) and availability. Requires no arguments to run."
    )
    parser.add_argument(
        "-c",
        "--command",
        nargs=1,
        type=str,
        help="""One of ("nodes", "partitions", "load").""",
    )
    parser.add_argument(
        "-s",
        "--summary",
        nargs=1,
        type=str,
        default=None,
        help="""Summarizes output of --command. Only available for ("nodes"). One of ("all", "partitions").""",
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

    # TODO allow multiple commands to reuse the same snapshot
    command = args.command[0]
    summary = args.summary
    if isinstance(summary, list):
        summary = summary[0]
    test = args.test
    generate_test_case = args.generate_test_case

    snapshot = parse.snapshot_interface(generate_test=generate_test_case, run_test=test)

    # TODO loop over many commands
    command = command.casefold()
    if command in ("nodes", "load"):
        nodes = commands.Nodes(snapshot=snapshot)
        if command == "load":
            nodessummary = commands.NodesSummary(nodes=nodes, style="all")
            out = commands.Load(nodessummary=nodessummary)
        elif summary is not None:
            out = commands.NodesSummary(nodes=nodes, style=summary)
        else:
            out = nodes
    elif command == "partitions":
        out = commands.Partitions(snapshot=snapshot)
    else:
        assert False

    # TODO parameterize output format:
    # 1) csv
    # 2) mediawiki
    # 3) fixed_width motd
    # 4) ood banner
    print(out.to_df().to_csv(index=False))


if __name__ == "__main__":
    mp.freeze_support()
    interface()

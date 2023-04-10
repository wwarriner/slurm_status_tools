import datetime as dt
from typing import Dict, List, Optional, Union

from typing_extensions import Literal

import command
import parse

DATA = Dict[str, str]


# TODO sum up memory across allocations


class Sacct:
    """
    https://slurm.schedmd.com/sacct.html
    """

    DELIMITER = "|"

    def get_job_by_jobid(self, jobid: int) -> DATA:
        assert 0 < jobid

        args = [
            "sacct",
            "--allocations",
            "--parsable2",
            "--format",
            "ALL",
            "--jobs",
            f"{jobid}",
        ]
        out = self._get_data(args)
        data = out[0]
        return data

    def get_jobs_by_user(self, user: str, start_date: dt.datetime) -> List[DATA]:
        args = [
            "sacct",
            "--allocations",
            "--parsable2",
            "--format",
            "ALL",
            "--user",
            f"{user}",
            "--starttime",
            f"{start_date:%Y-%m-%dT%H%:%M:%S}",
        ]
        out = self._get_data(args)
        return out

    def _get_data(self, args: List[str]) -> List[DATA]:
        result = command.run(args)
        text = result.stdout
        out = parse.parse_delimited(text, self.DELIMITER)
        return out


class Scontrol:
    """
    https://slurm.schedmd.com/scontrol.html
    """

    DELIMITER = " "
    SEPARATOR = "="

    def get_jobs(self, jobid: Optional[int] = None) -> List[DATA]:
        args = self._build_args("jobs")
        if jobid is not None:
            assert 0 < jobid
            args.append(f"{jobid}")
        out = self._get_data(args)
        return out

    def get_nodes(self, node: Optional[str] = None) -> List[DATA]:
        args = self._build_args("nodes")
        if node is not None:
            assert node != ""
            args.append(f"{node}")
        out = self._get_data(args)
        return out

    def get_partitions(self, partition: Optional[str] = None) -> List[DATA]:
        args = self._build_args("partitions")
        if partition is not None:
            assert partition != ""
            args.append(f"{partition}")
        out = self._get_data(args)
        return out

    def _build_args(
        self, entity: Union[Literal["jobs"], Literal["nodes"], Literal["partitions"]]
    ) -> List[str]:
        return ["scontrol", "show", f"{entity}", "--all", "--details", "--oneliner"]

    def _get_data(self, args: List[str]) -> List[DATA]:
        result = command.run(args)
        text = result.stdout
        out = parse.parse_scontrol(text)
        return out


class Squeue:
    """
    https://slurm.schedmd.com/squeue.html
    """

    def is_job_running(self, jobid: int) -> bool:
        assert 0 < jobid
        args = ["squeue", "--noheader", "--jobs", f"{jobid}"]
        result = command.run(args)
        is_running = result.stdout != ""
        return is_running


class Sacctmgr:
    """
    https://slurm.schedmd.com/sacctmgr.html
    """

    DELIMITER = "|"

    def get_qos(self) -> List[DATA]:
        args = ["sacctmgr", "show", "qos", "--parsable2"]
        result = command.run(args)
        text = result.stdout
        out = parse.parse_delimited(text, self.DELIMITER)
        return out


# TODO REFACTOR
def get_running_jobs(self) -> List[DATA]:
    data = self._get_scontrol_jobs()
    data = [d for d in data if d["state"] == "RUNNING"]
    return data


def _was_running(self, _d: DATA) -> bool:
    return _d["state"] == "RUNNING"

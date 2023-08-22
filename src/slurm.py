import datetime as dt
from typing import Callable, Dict, List, Optional, Union

from typing_extensions import Literal

import src.gather.command as command
import src.gather.parse as parse

ENTRY = Dict[str, str]


# TODO factor out the source so we can also pull from existing files


class Sacct:
    """
    https://slurm.schedmd.com/sacct.html
    """

    DELIMITER = "|"

    def get_job_by_jobid(self, jobid: int) -> List[ENTRY]:
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
        return [self._pull(args)[0]]  # for older Slurm, only get sbatch main allocation

    def get_jobs_by_user(self, user: str, start_date: dt.datetime) -> List[ENTRY]:
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
        return self._pull(args)

    def _pull(self, _args: List[str]) -> List[ENTRY]:
        return _get_data(lambda s: parse.delimited_format(s, self.DELIMITER), _args)


class Scontrol:
    """
    https://slurm.schedmd.com/scontrol.html
    """

    def get_jobs(self, jobid: Optional[int] = None) -> List[ENTRY]:
        _id = ""
        if jobid is not None:
            assert 0 < jobid
            _id = str(jobid)
        return self._get("jobs", _id)

    def get_nodes(self, node: Optional[str] = None) -> List[ENTRY]:
        _id = ""
        if node is not None:
            assert node != ""
            _id = node
        return self._get("nodes", _id)

    def get_partitions(self, partition: Optional[str] = None) -> List[ENTRY]:
        _id = ""
        if partition is not None:
            assert partition != ""
            _id = partition
        return self._get("partitions", _id)

    def _get(
        self,
        entity: Union[Literal["jobs"], Literal["nodes"], Literal["partitions"]],
        entity_id: str,
    ) -> List[ENTRY]:
        args = [
            "scontrol",
            "show",
            f"{entity}",
            "--all",
            "--details",
            "--oneliner",
            entity_id,
        ]
        return self._pull(args)

    def _pull(self, _args: List[str]) -> List[ENTRY]:
        return _get_data(parse.scontrol_format, _args)


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

    def get_qoses(self) -> List[ENTRY]:
        args = ["sacctmgr", "show", "qos", "--parsable2"]
        return _get_data(lambda s: parse.delimited_format(s, self.DELIMITER), args)


def _get_data(parse_fn: Callable[[str], List[ENTRY]], args: List[str]) -> List[ENTRY]:
    result = command.run(args)
    text = result.stdout
    out = parse_fn(text)
    return out


# TODO REFACTOR
def get_running_jobs(self) -> List[ENTRY]:
    data = self._get_scontrol_jobs()
    data = [d for d in data if d["state"] == "RUNNING"]
    return data

from typing import List, Union
from typing_extensions import Literal
import subprocess

RAISE = "raise"
IGNORE = "ignore"


class Result:
    def __init__(self, stdout: str, stderr: str, returncode: int) -> None:
        self._stdout: str = stdout
        self._stderr: str = stderr
        self._returncode: int = returncode

    @property
    def stdout(self) -> str:
        return self._stdout

    @property
    def stderr(self) -> str:
        return self._stderr

    @property
    def returncode(self) -> int:
        return self._returncode


def run(
    args: List[str], error_handling: Union[Literal["raise"], Literal["ignore"]] = RAISE
) -> Result:
    # TODO raises FileNotFoundError
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    returncode = result.returncode
    stderr = result.stderr.decode("utf-8", "ignore")
    stdout = result.stdout.decode("utf-8", "ignore")

    if error_handling == RAISE and 0 < returncode:
        raise RuntimeError(stderr)
    elif error_handling == IGNORE:
        pass
    else:
        assert False

    return Result(stdout, stderr, returncode)

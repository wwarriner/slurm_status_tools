import datetime as dt
import itertools
import re
from typing import Dict, List, Tuple

DATA = Dict[str, str]
GPU_SCONTROL_NODE_REGEX = re.compile(r"gpu:.*?:([0-9]+)?")


def parse_delimited(_s: str, delimiter: str) -> List[DATA]:
    """ """

    parts = _s.split("\n", 1)
    header, body = parts[0], parts[1]

    all_keys = [header.split("|")]
    all_values = [p.split("|") for p in body.split("\n")]

    data = [
        _to_data(keys, values)
        for keys, values in zip(itertools.cycle(all_keys), all_values)
    ]

    return data


def parse_scontrol(_s: str, join_duplicates_with: str = ",") -> List[DATA]:
    lines = _s.split("\n")
    out = [parse_scontrol_line(line, join_duplicates_with) for line in lines]
    return out


def parse_scontrol_line(_s: str, join_duplicates_with: str = ",") -> DATA:
    """
    Transforms an scontrol line into a dataset.

    Duplicate keys can appear for "Nodes" and "GRES_IDX" in `scontrol -o show
    jobs`. There may be others. We join values of duplicated keys as follows:

    `Nodes=c0001 ... Nodes=c0003` -> `{Nodes:[c0001,c0003]}`
    """

    tokens = tokenize_scontrol_line(_s)
    keys = [token[0] for token in tokens]
    data: Dict[str, List[str]] = {key: [] for key in keys}
    [data[k].append(v) for k, v in tokens]
    out = {k: join_duplicates_with.join(v) for k, v in data}
    return out


def tokenize_scontrol_line(_s: str) -> List[Tuple[str, str]]:
    """
    Tokenizes one line of output from scontrol -o *. Lines have quasi-flag-style
    args that look like the following:

    `a=foo b=bar c=hello world d=something=actual value`

    Each token is composed of a field and a value as <field>=<value>. Values are
    allowed to contain any characters but do not have a leading space. If values
    were allowed to have leading spaces, tokenization would be impossible.

    Tokenization starts with splitting on spaces. Parts are examined in reverse
    order, accumulating parts that do not contain "=". When "=" is found in a
    part, split on it. The first part is the key. The second part is a piece of
    the value. All tokens encountered since the last "=", and the second piece
    of the split, are joined by " " to form the value.
    """

    DELIMITER = " "
    SEPARATOR = "="
    parts = _s.split(DELIMITER)
    value_accumulator: List[str] = []
    result: List[Tuple[str, str]] = []
    for part in reversed(parts):
        if SEPARATOR not in part:
            value_accumulator.append(part)
            continue

        key_rest = part.split(SEPARATOR, 1)
        key, rest = key_rest[0], key_rest[1]

        value_accumulator.append(rest)
        value = DELIMITER.join(reversed(value_accumulator))

        result.append((key, value))

        value_accumulator = []
    return result


def parse_gpu_scontrol_show_node(_gres_s: str) -> Dict[str, int]:
    """
    Parses count of gpus from the `Gres` fields.

    Preconditions:
        - input is a comma separated list of colon separated lists
        - each colon separated list is of the form "x:y:z"
            - x is "gpu"
            - y is a non-empty string
            - z is a positive integer
    """
    parts = _gres_s.split(",")
    gpu_counts: Dict[str, int] = {}
    for part in parts:
        pieces = part.split(":", 2)
        if len(pieces) != 3:
            continue

        gpu_name = pieces[1]
        try:
            count = int(pieces[2])
        except:
            continue

        not_gpu = pieces[0] != "gpu"
        unnamed = len(gpu_name) == 0
        bad_count = count <= 0
        if not_gpu or unnamed or bad_count:
            continue

        if gpu_name not in gpu_counts:
            gpu_counts[gpu_name] = count
        else:
            gpu_counts[gpu_name] += count

    return gpu_counts


def parse_gpu_scontrol_show_job(_tres_s: str) -> int:
    """
    Parses count of gpus from the `TRES` field from `scontrol show
    job`.

    Preconditions:
        - input is a comma-separated-list of "k=v" pairs, one of which has the
          form "gres/gpu=x" where x is a positive integer.

    Returns:
        - integer
    """
    KEY = "gres/gpu="
    KEY_LENGTH = len(KEY)

    parts = _tres_s.split(",")
    total = 0
    for part in parts:
        if not part.startswith(KEY):
            continue

        try:
            count = int(part[KEY_LENGTH:])
        except:
            continue

        total += count
    return total


def hyphenated_csl_to_list(_s: str) -> List[int]:
    """
    Utility to parse comma-separated lists (CSL) of integers that can contain
    hyphenated ranges. Returns an explicit list of integers.

    Ex: `1,3-6,7,8` -> [1,3,4,5,6,7,8]

    Preconditions:
        - input is a comma-separated list whose elements are one of
            1. "x" where x is a non-negative integer
            2. "x-y" where x and y are non-decreasing, non-negative integers

    NOTE: We could save memory by turning this into a generator. It would work
    by returning list values. When the list is empty we pop the next token.
    Tokens would be single values or a range, with its trailing comma (if there
    is one). We almost certainly won't need to do this.
    """
    if _s == "":
        return []

    out: List[int] = []
    parts = _s.split(",")
    for part in parts:
        if "-" in part:
            values = hyphenated_range_to_list(part)
        else:
            try:
                values = [int(part)]
            except:
                values = []
        out.extend(values)
    return out


def hyphenated_range_to_list(_s: str) -> List[int]:
    """
    Preconditions:
        - input is of the form "x-y" where x and y are non-decreasing,
          non-negative integers.
    """
    parts = _s.split("-")

    try:
        values = (int(parts[0]), int(parts[1]))
    except:
        values = None

    if values is None:
        out: List[int] = []
    else:
        out = list(range(values[0], values[1] + 1))

    return out


DURATION_REGEX_STRING = (
    r"((?P<days>\d+)-)?((?P<hours>\d{2})):((?P<minutes>\d{2})):((?P<seconds>\d{2}))"
)
DURATION_REGEX = re.compile(DURATION_REGEX_STRING)


def parse_slurm_duration_as_hours(_d: str) -> int:
    """
    Transforms date formats like "d-hh:mm:ss" to "h hours"

    Preconditions:
        - Input is of the form "[d-]hh:mm:ss" where [] indicates optional and
            - d is a positive integer
            - hh is a non-negative integer < 24 with two digits and leading
              zeroes
            - mm is a non-negative integer < 60 with two digits and leading
              zeroes
            - ss is a non-negative integer < 60 with two digits and leading
              zeroes

    Raises:
        - InputError if input does not meet preconditions.
    """
    CONVERSION_TO_SECONDS = [86400, 3600, 60, 1]

    d = DURATION_REGEX.match(_d)
    if d is None:
        out = -1
    else:
        units = ("days", "hours", "minutes", "seconds")
        values: List[float] = []
        for unit in units:
            try:
                v = float(d.group(unit))
            except:
                return -1
            values.append(v)
        seconds = sum([v * c for v, c in zip(values, CONVERSION_TO_SECONDS)])
        hours, _ = divmod(seconds, 3600)
        out = int(hours)
    return out


def parse_memory_value_to_bytes(_s: str) -> int:
    CONVERSION_TO_BYTES = {
        "t": 1099511627776,
        "g": 1073741824,
        "m": 1048576,
        "k": 1024,
    }
    try:
        amount_to_convert = int(_s[:-1])
        unit = _s[-1].casefold()
        amount = CONVERSION_TO_BYTES[unit] * amount_to_convert
    except:
        amount = -1

    return amount


def parse_nodelist(_s: str, minimum_digit_count: int = 0) -> List[str]:
    end_prefix = _s.find("[")
    prefix = _s[:end_prefix]
    if len(prefix) == 0:
        return []
    hyphenated_csl = _s[end_prefix + 1 : -1]
    test = hyphenated_csl.split(",")[0].split("-")[0]
    digit_count = max(len(test), minimum_digit_count)
    node_numbers = hyphenated_csl_to_list(hyphenated_csl)
    format_spec = f"{prefix}{{n:0{digit_count}d}}"
    nodes = [format_spec.format(n=n) for n in node_numbers]
    # except:
    #     nodes = []
    return nodes


def _to_data(_keys: List[str], _values: List[str]) -> DATA:
    _keys = [k.casefold() for k in _keys]
    return {k: v for k, v in zip(_keys, _values)}

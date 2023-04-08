import itertools
from typing import Dict, List, Tuple

DATA = Dict[str, str]


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


def _to_data(_keys: List[str], _values: List[str]) -> DATA:
    _keys = [k.casefold() for k in _keys]
    return {k: v for k, v in zip(_keys, _values)}

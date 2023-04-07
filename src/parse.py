import itertools
from typing import Dict, List

DATA = Dict[str, str]


def is_legacy_scontrol_show_job_output(_s: str) -> bool:
    return 1 < _s.count("\n")


def parse_key_value_lines(_s: str, delimiter: str, separator: str) -> List[DATA]:
    lines = _s.split("\n")
    data = [parse_key_value_line(line, delimiter, separator) for line in lines]
    return data


def parse_key_value_line(_s: str, delimiter: str, separator: str) -> DATA:
    terms = _s.split(delimiter)

    tokens = [term.split(separator, 1) for term in terms]
    tokens = [(t[0], t[1]) for t in tokens]

    keys, values = zip(*tokens)
    keys = list(keys)
    values = list(values)

    data = _to_data(keys, values)

    # TODO too opinionated?
    data.pop("")
    [data.pop(k) for k, v in data.items() if v == ""]

    # TODO interpret values

    return data


def parse_delimited_table(_s: str, delimiter: str) -> List[DATA]:
    """ """
    parts = _s.split("\n", 1)

    all_keys = itertools.cycle([parts[0].split("|")])
    all_values = [p.split("|") for p in parts[1].split("\n")]

    data = [_to_data(keys, values) for keys, values in zip(all_keys, all_values)]

    # TODO interpret values

    return data


def parse_scontrol(_s: str, delimiter: str) -> List[DATA]:
    """
    Parses output of scontrol -o *. The command returns one record per line
    (node or job). Each line has quasi-flag-style args that look like the
    following:
    ```
    a=foo b=bar c=hello world d=something=actual value
    ```

    The model is that each arg is composed of a field and a value as
    <field>=<value>. Values have spaces, but the field-value pairs are space
    separated and values can contain equals symbols, so we have to take care in
    parsing.

    The approach to parsing this involves parsing in reverse order. We loop over
    the contents of a line, popping them from the back end and putting them in
    both the field and value. When we hit an equals character, we reset the
    field and change state (equals_hit=True). When we hit a space while
    equals_hit=True, we assume we've just finished collecting the actual field,
    so we turn the field and value into strings, remove the field and leading
    `=` from the value string, and strip leading and trailing whitespace. Then
    we add the results to a dict with field as key and value as value. We reset
    state and continue parsing the line.

    This method will not work for a value that has a space followed by an equals
    sign. But then the problem becomes ill-posed because we can no longer
    distinguish when a field-value pair ends, so we hope dearly that this never
    happens. If it does we'll have to stop using the `-o` flag.

    The input sep is used to create a delimited string when a field appears
    multiple times. Notably this occurs for "Nodes" and "GRES_IDX" in `scontrol
    -o show jobs`.
    """
    lines = _s.split("\n")

    all_data: List[Dict[str, str]] = []
    for line_s in lines:
        line_data: Dict[str, List[str]] = {}
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
            if (c == " " or not line) and equals_hit:
                field_s: str = "".join(field[::-1])
                field_s = field_s.strip()

                value_s: str = "".join(value[::-1])
                value_s = value_s.replace(field_s, "", 1)
                value_s = value_s.replace("=", "", 1)
                value_s = value_s.strip()

                if field_s in line_data:
                    line_data[field_s].append(value_s)
                else:
                    line_data[field_s] = [value_s]

                field = []
                value = []
                equals_hit = False

        line_data.pop("", None)
        line_df_data: Dict[str, str] = {k: sep.join(v) for k, v in line_data.items()}
        all_data.append(line_df_data)
        # TODO deal with the case where multiple nodes are requested. Will get multiple of some columns!

    df = pd.DataFrame(all_data)
    df = _fillna_extended(df=df)
    return df


def parse_scontrol_line(_s: str, delimiter: str, separator: str) -> DATA:
    # - keys do not have spaces
    # - values may have spaces
    # - proceed through the line one character at a time in reverse order
    # - accumulate popped characters in a token
    # - if we hit an equals sign, the token must have been a value
    # - if we hit a space (or ran out of characters) AND equals was hit
    #   recently, the token must have been a key AND we now have a key-value
    #   pair

    line_s = _s

    line_data: Dict[str, List[str]] = {}
    characters: List[str] = list(line_s)
    token_accumulator: List[str] = []
    key: List[str] = []
    value: List[str] = []
    equals_hit = False
    while characters:
        c: str = characters.pop()
        token_accumulator.append(c)

        if c == "=":
            value = token_accumulator
            token_accumulator = []
            equals_hit = True

        if (c == " " or len(characters) == 0) and equals_hit:
            key = token_accumulator
            token_accumulator = []

            key_s: str = "".join(reversed(key))
            key_s = key_s.strip()

            value_s: str = "".join(reversed(value))
            value_s = value_s.strip()

            if key_s in line_data:
                line_data[key_s].append(value_s)
            else:
                line_data[key_s] = [value_s]

            key = []
            value = []
            equals_hit = False

    line_data.pop("", None)
    out: Dict[str, str] = {k: delimiter.join(v) for k, v in line_data.items()}
    return out


def _to_data(_keys: List[str], _values: List[str]) -> DATA:
    _keys = [k.casefold() for k in _keys]
    return {k: v for k, v in zip(_keys, _values)}

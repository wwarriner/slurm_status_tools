from typing import List

import pandas as pd

CSV = "csv"
MOTD = "motd"
ASCII = "ascii"
MEDIAWIKI = "mediawiki"
STYLES = (CSV, MOTD, ASCII, MEDIAWIKI)


def apply_style(style: str, df: pd.DataFrame) -> str:
    if style == CSV:
        out = as_csv(df=df)
    elif style == MEDIAWIKI:
        out = as_mediawiki(df=df)
    elif style == MOTD:
        out = as_motd(df=df)
    elif style == ASCII:
        out = as_ascii_table(df=df)
    else:
        assert False
    return out


def as_csv(df: pd.DataFrame) -> str:
    return df.to_csv(index=False)


def as_mediawiki(df: pd.DataFrame) -> str:
    doc_l = []

    doc_l.append('{| class="wikitable"\n')

    header_l = list(df.columns)
    header_f = "!" + "!!\t".join(header_l) + "\n"
    doc_l.append(header_f)

    for row in df.itertuples(index=False):
        row = list(row)
        row = [str(x) for x in row]
        row_f = "|" + '||align="right"|\t'.join(row) + "\n"
        doc_l.append(row_f)

    doc_l.append("|}\n")
    doc_f = "|-\n".join(doc_l)

    return doc_f


def as_motd(df: pd.DataFrame) -> str:
    return "<<MOTD GOES HERE>>"


def as_ascii_table(df: pd.DataFrame) -> str:
    # convert all to string
    for column in df.columns:
        d = df[column].dtype
        if d == float:
            df[column] = df[column].apply(lambda x: "{:.2f}".format(x))
        else:
            df[column] = df[column].apply(str)

    # get column-wise max string length
    column_widths = []
    for column in df.columns:
        element_lengths = list(df[column].str.len())
        element_lengths.append(len(column))
        column_width = max(element_lengths)
        column_widths.append(column_width)

    lines = []
    lines.append(_horizontal_line(ns=column_widths))
    lines.append(_data_line(ns=column_widths, data=list(df.columns)))
    lines.append(_horizontal_line(ns=column_widths))
    for row in df.itertuples():
        line = _data_line(ns=column_widths, data=list(row)[1:])  # 1: skip index
        lines.append(line)
    lines.append(_horizontal_line(ns=column_widths))

    out = "\n".join(lines)
    return out


def _horizontal_line(ns: List[int], pad_count: int = 0) -> str:
    struts = [_horizontal_strut(n) for n in ns]
    out = _fuse_line(c=_joint_element(), values=struts)
    return out


def _data_line(ns: List[int], data: List[str], pad_count: int = 0) -> str:
    formats = ["{: >" + "{:d}".format(n) + "s}" for n in ns]
    formatted_data = [f.format(d) for f, d in zip(formats, data)]
    out = _fuse_line(c=_vertical_element(), values=formatted_data)
    return out


def _fuse_line(
    c: str, values: List[str], pad_count: int = 0, pad_char: str = " "
) -> str:
    padding = pad_char * pad_count
    joint = padding + c + padding
    out = joint.join(values)
    out = c + out + c
    return out


def _horizontal_strut(n: int) -> str:
    return _horizontal_element() * n


def _horizontal_element() -> str:
    return "-"


def _vertical_element() -> str:
    return "|"


def _joint_element() -> str:
    return "+"

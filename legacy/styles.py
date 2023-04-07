import itertools
from typing import Dict, Iterable, List, NamedTuple, Optional

import pandas as pd

CSV = "csv"
MOTD = "motd"
ASCII = "ascii"
MEDIAWIKI = "mediawiki"
STYLES = (CSV, MOTD, ASCII, MEDIAWIKI)

LEFT_ALIGN = "l"
CENTER_ALIGN = "c"
RIGHT_ALIGN = "r"
ALIGNS = (LEFT_ALIGN, CENTER_ALIGN, RIGHT_ALIGN)


class AsciiTable:
    _ALIGNMENT_MAP = {
        LEFT_ALIGN: "<",
        CENTER_ALIGN: "^",
        RIGHT_ALIGN: ">",
    }

    def __init__(
        self,
        joint_element: str = "+",
        horizontal_element: str = "-",
        vertical_element: str = "|",
        pad_element: str = " ",
        pad_amount: int = 0,
        precision: int = 1,
        have_top_border: bool = True,
        have_header_separator: bool = True,
        have_bottom_border: bool = True,
        have_left_border: bool = True,
        have_right_border: bool = True,
    ):
        assert len(joint_element) == 1
        assert len(horizontal_element) == 1
        assert len(vertical_element) == 1
        assert len(pad_element) == 1
        assert 0 <= pad_amount
        assert 0 <= precision

        self._j_el = joint_element
        self._h_el = horizontal_element
        self._v_el = vertical_element
        self._p_el = pad_element
        self._p_amt = pad_amount
        self._precision = precision
        self._have_top_border = have_top_border
        self._have_header_separator = have_header_separator
        self._have_bottom_border = have_bottom_border
        self._have_left_border = have_left_border
        self._have_right_border = have_right_border

    @staticmethod
    def get_column_widths(df: pd.DataFrame, precision: int) -> List[int]:
        """
        skips index
        """
        df = _format_df_contents_as_str(df=df, precision=precision)

        # get column-wise max string length
        widths = []
        for column in df.columns:
            element_lengths = list(df[column].str.len())
            element_lengths.append(len(column))  # include column headers
            width = max(element_lengths)
            widths.append(width)
        return widths

    def render(
        self,
        df: pd.DataFrame,
        user_alignments: Optional[Dict[str, str]] = None,
        default_alignment: str = LEFT_ALIGN,
    ) -> str:
        df = _format_df_contents_as_str(df=df, precision=self._precision)
        widths = self.get_column_widths(df, precision=self._precision)
        alignments = _build_alignment_list(
            user_alignments=user_alignments,
            default_alignment=default_alignment,
            columns=df.columns,
            format_map=self._ALIGNMENT_MAP,
        )

        lines = []
        if self._have_top_border:
            lines.append(self._render_h_line(widths=widths))
        lines.append(
            self._render_data_line(
                alignments=alignments, widths=widths, data=list(df.columns)
            )
        )
        if self._have_header_separator:
            lines.append(self._render_h_line(widths=widths))
        for row in df.itertuples(index=False):
            line = self._render_data_line(
                alignments=alignments, widths=widths, data=list(row)
            )
            lines.append(line)
        if self._have_bottom_border:
            lines.append(self._render_h_line(widths=widths))

        out = "\n".join(lines)
        return out

    def _render_data_line(
        self, alignments: List[str], widths: List[int], data: List[str]
    ) -> str:
        # e.g. [{: >4s}, ...]
        formats = []
        for alignment, width in zip(alignments, widths):
            width_s = "{:d}".format(width)
            f = "{: " + alignment + width_s + "s}"
            formats.append(f)
        formatted_data = [f.format(d) for f, d in zip(formats, data)]
        padded_data = [self._pad_cell(cell=d) for d in formatted_data]
        line = self._fuse_cells_into_borderless_line(
            element=self._v_el, cells=padded_data
        )
        line = self._add_lr_borders_to_line(element=self._v_el, line=line)
        return line

    def _render_h_line(self, widths: List[int]) -> str:
        struts = [self._render_h_strut(w) for w in widths]
        line = self._fuse_cells_into_borderless_line(element=self._j_el, cells=struts)
        line = self._add_lr_borders_to_line(element=self._j_el, line=line)
        return line

    def _render_h_strut(self, width: int) -> str:
        pad_w = 2 * self._p_amt
        full_w = width + pad_w
        return self._h_el * full_w

    def _fuse_cells_into_borderless_line(self, element: str, cells: List[str]) -> str:
        return element.join(cells)

    def _add_lr_borders_to_line(self, element: str, line: str) -> str:
        if self._have_left_border:
            line = element + line
        if self._have_right_border:
            line = line + element
        return line

    def _pad_cell(self, cell: str) -> str:
        return self._generate_padding() + cell + self._generate_padding()

    def _generate_padding(self) -> str:
        return self._p_amt * self._p_el


class MediaWikiTable:
    _ALIGNMENT_MAP = {
        LEFT_ALIGN: "left",
        CENTER_ALIGN: "center",
        RIGHT_ALIGN: "right",
    }
    _PRE = "{|"
    _POST = "|}"
    _H_LINE = "|-"
    _HEADER_SEP = "!"
    _VALUE_SEP = "|"
    _CLASS = "wikitable"

    def __init__(
        self, precision: int = 1,
    ):
        self._precision = precision

    def render(
        self,
        df: pd.DataFrame,
        user_alignments: Optional[Dict[str, str]] = None,
        default_alignment: str = LEFT_ALIGN,
    ) -> str:
        df = _format_df_contents_as_str(df=df, precision=self._precision)
        alignments = _build_alignment_list(
            user_alignments=user_alignments,
            default_alignment=default_alignment,
            columns=df.columns,
            format_map=self._ALIGNMENT_MAP,
        )

        lines = []
        for row in df.itertuples(index=False):
            lines.append(self._render_data_row(row=row, alignments=alignments))
            lines.append(self._render_row_separator())
        lines.pop()
        lines.insert(0, self._render_table_metadata())
        lines.insert(1, self._render_header_row(columns=df.columns))
        lines.insert(2, self._render_row_separator())
        lines.append(self._render_table_end())
        out = "\n".join(lines)
        return out

    def _render_table_metadata(self) -> str:
        return self._PRE + 'class="{:s}"'.format(self._CLASS)

    def _render_table_end(self) -> str:
        return self._POST

    def _render_header_row(self, columns: Iterable[str]) -> str:
        alignments = itertools.repeat(None)
        row = self._render_row(sep=self._HEADER_SEP, row=columns, alignments=alignments)
        return row

    def _render_data_row(self, row: Iterable[str], alignments: Iterable[str]) -> str:
        row = self._render_row(sep=self._VALUE_SEP, row=row, alignments=alignments)
        return row

    def _render_row(
        self, sep: str, row: Iterable[str], alignments: Iterable[Optional[str]]
    ) -> str:
        row = [
            self._make_cell(sep=sep, cell=c, alignment=a)
            for c, a in zip(row, alignments)
        ]
        row = "".join(row)
        row = row.rstrip(sep)  # trailing sep not used!
        return row

    def _render_row_separator(self) -> str:
        return self._H_LINE

    def _make_cell(self, sep: str, cell: str, alignment: Optional[str] = None) -> str:
        cell = sep + cell + sep
        if alignment is not None:
            alignment_rep = 'align="{:s}"'.format(alignment)
            cell = sep + alignment_rep + cell
        return cell


def apply_style(
    style: str,
    df: pd.DataFrame,
    user_alignments: Optional[Dict[str, str]] = None,
    default_alignment: str = LEFT_ALIGN,
    precision: int = 1,
) -> str:
    if style == CSV:
        out = as_csv(df=df)
    elif style == MEDIAWIKI:
        out = as_mediawiki(
            df=df,
            user_alignments=user_alignments,
            default_alignment=default_alignment,
            precision=precision,
        )
    elif style == MOTD:
        out = as_motd(
            df=df,
            user_alignments=user_alignments,
            default_alignment=default_alignment,
            precision=precision,
        )
    elif style == ASCII:
        out = as_ascii_table(
            df=df,
            user_alignments=user_alignments,
            default_alignment=default_alignment,
            precision=precision,
        )
    else:
        assert False
    return out


def as_csv(df: pd.DataFrame) -> str:
    return df.to_csv(index=False)


def as_mediawiki(
    df: pd.DataFrame,
    user_alignments: Optional[Dict[str, str]] = None,
    default_alignment: str = LEFT_ALIGN,
    precision: int = 1,
) -> str:
    mediawiki_table = MediaWikiTable(precision=precision)
    out = mediawiki_table.render(
        df=df, user_alignments=user_alignments, default_alignment=default_alignment
    )
    out = out + "\n"
    return out


def as_motd(
    df: pd.DataFrame,
    user_alignments: Optional[Dict[str, str]] = None,
    default_alignment: str = LEFT_ALIGN,
    precision: int = 1,
) -> str:
    """
    Builds an ASCII table suitable for display in Open OnDemand MOTD banner.

    Example:
    |               Name | Nodes | Nodes Per User |       Time Limit | Priority Tier |
    | -----------------: | ----: | -------------: | ---------------: | ------------: |
    |        interactive |   162 |              1 | 0 days,  2 hours |            20 |
    |            express |   162 |      UNLIMITED | 0 days,  2 hours |            20 |
    |              short |   162 |             44 | 0 days, 12 hours |            16 |
    |        pascalnodes |    18 |      UNLIMITED | 0 days, 12 hours |            16 |
    | pascalnodes-medium |     7 |      UNLIMITED | 2 days,  0 hours |            15 |
    |             medium |   162 |             44 | 2 days,  2 hours |            12 |
    |               long |   162 |              5 | 6 days,  6 hours |             8 |
    |         amd-hdr100 |    33 |              5 | 6 days,  6 hours |             8 |
    |           largemem |    14 |             10 | 2 days,  2 hours |             6 |

    OOD Message of the Day (MOTD) aligns columns using the following pattern
    between header row and value rows. So we have to hack these in as a data row
    before using the AsciiTable class.

    Left align:   `:-----`
    Center align: `:----:`
    Right align:  `-----:`
    """
    # inject alignment struts
    widths = AsciiTable.get_column_widths(df=df, precision=precision)
    alignments = _build_alignment_list(
        user_alignments=user_alignments,
        default_alignment=default_alignment,
        columns=df.columns,
    )
    alignment_struts = [
        _render_motd_alignment_strut(width=w, alignment=a)
        for w, a in zip(widths, alignments)
    ]
    # create an extra row for the struts and move it into place
    df = df.append({c: "" for c in df.columns}, ignore_index=True)
    df = df.shift()
    df.iloc[0, :] = alignment_struts

    # build table
    ascii_table = AsciiTable(
        joint_element=" ",
        horizontal_element=" ",
        vertical_element="|",
        pad_element=" ",
        pad_amount=1,
        precision=precision,
        have_top_border=False,
        have_header_separator=False,  # we have the alignment struts instead
        have_bottom_border=False,
    )
    out = ascii_table.render(
        df=df, user_alignments=user_alignments, default_alignment=default_alignment
    )
    out = out + "\n"
    return out


def as_ascii_table(
    df: pd.DataFrame,
    user_alignments: Optional[Dict[str, str]] = None,
    default_alignment: str = LEFT_ALIGN,
    precision: int = 1,
) -> str:
    """
    Builds an ASCII table suitable for verbatim display, especially in a shell
    MOTD banner.

    Example:
    +--------------------+-------+----------------+------------------+---------------+
    |               Name | Nodes | Nodes Per User |       Time Limit | Priority Tier |
    +--------------------+-------+----------------+------------------+---------------+
    |        interactive |   162 |              1 | 0 days,  2 hours |            20 |
    |            express |   162 |      UNLIMITED | 0 days,  2 hours |            20 |
    |              short |   162 |             44 | 0 days, 12 hours |            16 |
    |        pascalnodes |    18 |      UNLIMITED | 0 days, 12 hours |            16 |
    | pascalnodes-medium |     7 |      UNLIMITED | 2 days,  0 hours |            15 |
    |             medium |   162 |             44 | 2 days,  2 hours |            12 |
    |               long |   162 |              5 | 6 days,  6 hours |             8 |
    |         amd-hdr100 |    33 |              5 | 6 days,  6 hours |             8 |
    |           largemem |    14 |             10 | 2 days,  2 hours |             6 |
    |      largemem-long |     5 |             10 | 6 days,  6 hours |             6 |
    +--------------------+-------+----------------+------------------+---------------+
    """
    ascii_table = AsciiTable(
        joint_element="+",
        horizontal_element="-",
        vertical_element="|",
        pad_element=" ",
        pad_amount=1,
        precision=precision,
    )
    out = ascii_table.render(
        df=df, user_alignments=user_alignments, default_alignment=default_alignment
    )
    out = out + "\n"
    return out


def _render_motd_alignment_strut(width: int, alignment: str) -> str:
    """
    Builds OOD MOTD alignment struts like:

    Left align: `:-----`
    Right align: `-----:`
    """
    assert 2 <= width

    if alignment == LEFT_ALIGN:
        dash_length = width - 1
        dashes = "-" * dash_length
        strut = ":" + dashes
    elif alignment == CENTER_ALIGN:
        dash_length = width - 2
        dashes = "-" * dash_length
        strut = ":" + dashes + ":"
    elif alignment == RIGHT_ALIGN:
        dash_length = width - 1
        dashes = "-" * dash_length
        strut = dashes + ":"
    else:
        assert False
    return strut


def _format_df_contents_as_str(df: pd.DataFrame, precision: int) -> pd.DataFrame:
    """
    Copies dataframe. Does not format index.
    """
    df = df.copy()
    precision_fmt = "{:." + "{:d}".format(precision) + "f}"
    for column in df.columns:
        d = df[column].dtype
        if d == float:
            df[column] = df[column].apply(lambda x: precision_fmt.format(x))
        else:
            df[column] = df[column].apply(str)
    return df


def _build_alignment_list(
    user_alignments: Optional[Dict[str, str]],
    default_alignment: str,
    columns: Iterable[str],
    format_map: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Builds formatted alignment strings from user selected alignments, one per
    column. If a user selected alignment exists, that is used. Otherwise the
    default is used. A format map may be supplied to transform raw user
    alignments into a format appropriate for the style.
    """
    if format_map is not None:
        assert set(format_map.keys()) == set(ALIGNS)

    if user_alignments is None:
        user_alignments = {}

    alignments = []
    for c in columns:
        if c in user_alignments:
            alignments.append(user_alignments[c])
        else:
            alignments.append(default_alignment)
    if format_map is not None:
        alignments = [format_map[a] for a in alignments]
    return alignments

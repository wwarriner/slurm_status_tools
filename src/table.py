import abc
import enum
import itertools
from types import DynamicClassAttribute
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from typing_extensions import Literal

VALUES_TYPE = List[List[Any]]
VALUES_STR_TYPE = List[List[str]]


class Alignment(enum.Enum, metaclass=enum.EnumMeta):
    LEFT = "l"
    CENTER = "c"
    RIGHT = "r"
    DEFAULT = "l"

    @DynamicClassAttribute
    def value(self) -> str:
        """The value of the Enum member."""
        return self._value_


DEFAULT_PRECISION = 16


class Table:
    """
    tables are indexed by row, then column
    """

    def __init__(
        self, _columns: List[str], _values: VALUES_TYPE, na_value: Any = ""
    ) -> None:
        column_count = len(_columns)
        if column_count == 0:
            row_count = 0
        else:
            row_count = len(_values)

        self._na: Any = na_value
        self._columns: List[str] = _columns
        self._values: VALUES_TYPE = _values
        self._row_count: int = row_count
        self._column_count: int = len(_columns)

    @property
    def shape(self) -> Tuple[int, int]:
        return (self._row_count, self._column_count)

    @property
    def height(self) -> int:
        return self._row_count

    @property
    def width(self) -> int:
        return self._column_count

    @property
    def size(self) -> int:
        return self._row_count * self._column_count

    @property
    def columns(self) -> List[str]:
        return self._columns.copy()

    @property
    def values(self) -> VALUES_TYPE:
        return self._copy_values()

    def iterate_cells(self) -> Iterable[Any]:
        for row in self.values:
            for cell in row:
                yield cell

    def iterate_rows(self) -> Iterable[List[Any]]:
        for row in self.values:
            yield row

    def subset_columns(
        self, _c: Union[int, Iterable[int], str, Iterable[str]]
    ) -> "Table":
        if isinstance(_c, int):
            column_indices = [_c]
        elif isinstance(_c, str):
            assert _c in self._columns
            column_indices = [self._columns.index(_c)]
        else:
            column_indices = [
                c if isinstance(c, int) else self._columns.index(c) for c in _c
            ]

        for index in column_indices:
            assert 0 <= index < self._column_count

        if len(column_indices) == 0:
            return self.empty()

        column_indices = sorted(column_indices)
        table = self._new_empty_values(self._row_count)
        for row_index in range(self._row_count):
            for column_index in column_indices:
                v = self._values[row_index][column_index]
                table[row_index].append(v)

        columns = [self._columns[i] for i in column_indices]

        return Table(columns, table, self._na)

    def values_as_str(
        self,
        precision: Optional[Union[int, Dict[str, int]]],
        default_precision: int = DEFAULT_PRECISION,
    ) -> VALUES_STR_TYPE:
        if precision is None:
            precisions = [default_precision] * self._column_count
        elif isinstance(precision, int):
            precisions = [precision] * self._column_count
        elif isinstance(precision, dict):
            column_indices = [self._columns.index(c) for c in precision.keys()]
            precision_map = list(precision.values())
            precisions = [default_precision] * self._column_count
            for index, p in zip(column_indices, precision_map):
                precisions[index] = p
        else:
            assert False

        precision_formats = [f"{{:.{p}f}}" for p in precisions]
        values = self._copy_values()
        values = [
            [
                p_format.format(cell) if isinstance(cell, float) else str(cell)
                for cell, p_format in zip(row, precision_formats)
            ]
            for row in values
        ]
        return values

    def _copy_values(self) -> VALUES_TYPE:
        return [row.copy() for row in self._values]

    @classmethod
    def empty(cls) -> "Table":
        return cls([], cls._new_empty_values(0))

    @classmethod
    def from_rows(cls, _data: List[Dict[str, Any]], na_value: Any = "") -> "Table":
        seen: Set[str] = set()
        keys: List[str] = [
            key
            for row in _data
            for key in row.keys()
            if not (key in seen or seen.add(key))
        ]

        row_count = len(_data)
        table = cls._new_empty_values(row_count)
        for row_index in range(row_count):
            for key in keys:
                v = _data[row_index].get(key, na_value)
                table[row_index].append(v)

        return cls(keys, table, na_value)

    @staticmethod
    def _new_empty_values(_row_count: int) -> VALUES_TYPE:
        assert 0 <= _row_count
        return [[] for _ in range(_row_count)]


class Style(abc.ABC):
    @abc.abstractmethod
    def render(
        self,
        table: Table,
        column_alignments: Optional[Dict[str, Alignment]] = None,
        default_alignment: Alignment = Alignment.DEFAULT,
        column_precisions: Optional[Dict[str, int]] = None,
        default_precision: int = DEFAULT_PRECISION,
    ):
        ...

    @staticmethod
    def _build_alignment_list(
        column_alignments: Optional[Dict[str, Alignment]],
        default_alignment: Alignment,
        columns: List[str],
        alignment_map: Optional[Dict[Alignment, str]] = None,
    ) -> List[str]:
        """
        Builds formatted alignment strings from user selected alignments, one per
        column. If a user selected alignment exists, that is used. Otherwise the
        default is used. A format map may be supplied to transform raw user
        alignments into a format appropriate for the style.
        """

        if column_alignments is None:
            column_alignments = {}

        if alignment_map is None:
            alignment_map = {a: a.value for a in Alignment}

        for column in column_alignments.keys():
            assert column in columns

        alignments: List[Alignment] = []
        for c in columns:
            if c in column_alignments:
                alignments.append(column_alignments[c])
            else:
                alignments.append(default_alignment)
        out = [alignment_map[a] for a in alignments]
        return out


class AsciiStyle(Style):
    _ALIGNMENT_MAP: Dict[Alignment, str] = {
        Alignment.LEFT: "<",
        Alignment.CENTER: "^",
        Alignment.RIGHT: ">",
    }

    def __init__(
        self,
        joint_element: str = "+",
        horizontal_element: str = "-",
        vertical_element: str = "|",
        pad_element: str = " ",
        pad_amount: int = 0,
        render_top_border: bool = True,
        render_header_separator: bool = True,
        render_bottom_border: bool = True,
        render_left_border: bool = True,
        render_right_border: bool = True,
    ):
        assert len(joint_element) == 1
        assert len(horizontal_element) == 1
        assert len(vertical_element) == 1
        assert len(pad_element) == 1
        assert 0 <= pad_amount

        self._j_el: str = joint_element
        self._h_el: str = horizontal_element
        self._v_el: str = vertical_element
        self._p_el: str = pad_element
        self._p_amt: int = pad_amount
        self._top_border: bool = render_top_border
        self._header_separator = render_header_separator
        self._bottom_border = render_bottom_border
        self._left_border = render_left_border
        self._right_border = render_right_border

    def render(
        self,
        table: Table,
        column_alignments: Optional[Dict[str, Alignment]] = None,
        default_alignment: Alignment = Alignment.LEFT,
        column_precisions: Optional[Dict[str, int]] = None,
        default_precision: int = DEFAULT_PRECISION,
    ) -> str:
        headers = table.columns
        values = table.values_as_str(column_precisions, default_precision)

        alignments = self._build_alignment_list(
            column_alignments=column_alignments,
            default_alignment=default_alignment,
            columns=headers,
            alignment_map=self._ALIGNMENT_MAP,
        )

        widths = self._get_column_widths(headers, values)
        lines = []
        if self._top_border:
            lines.append(self._render_h_line(widths))
        lines.append(self._render_row_line(alignments, widths, headers))
        if self._header_separator:
            lines.append(self._render_separator_line(widths, alignments))
        for row in values:
            lines.append(self._render_row_line(alignments, widths, row))
        if self._bottom_border:
            lines.append(self._render_h_line(widths))

        out = "\n".join(lines)
        out += "\n"
        return out

    def _render_row_line(
        self, alignments: List[str], widths: List[int], row: List[str]
    ) -> str:
        # e.g. [{: >4s}, ...]
        # formats each value aligned and padded with spaces (s)
        formats = [
            f"{{: {alignment}{width}s}}" for alignment, width in zip(alignments, widths)
        ]
        formatted_data = [f.format(d) for f, d in zip(formats, row)]
        padded_data = [self._pad_cell(cell=d) for d in formatted_data]
        line = self._v_el.join(padded_data)
        line = self._add_lr_borders_to_line(self._v_el, line)
        return line

    def _render_separator_line(
        self, widths: List[int], alignments: Optional[List[str]] = None
    ) -> str:
        return self._render_h_line(widths)

    def _render_h_line(self, widths: List[int]) -> str:
        struts = [self._render_h_strut(w) for w in widths]
        line = self._j_el.join(struts)
        line = self._add_lr_borders_to_line(self._j_el, line)
        return line

    def _render_h_strut(self, width: int) -> str:
        pad_w = 2 * self._p_amt
        full_w = width + pad_w
        return self._h_el * full_w

    def _add_lr_borders_to_line(self, element: str, line: str) -> str:
        if self._left_border:
            line = element + line
        if self._right_border:
            line = line + element
        return line

    def _pad_cell(self, cell: str) -> str:
        return self._generate_padding() + cell + self._generate_padding()

    def _generate_padding(self) -> str:
        return self._p_amt * self._p_el

    @staticmethod
    def _get_column_widths(columns: List[str], values: VALUES_STR_TYPE) -> List[int]:
        widths = [0] * len(columns)
        for row in values:
            cell_lengths = [len(cell) for cell in row]
            widths = [
                max(width, cell_length)
                for width, cell_length in zip(widths, cell_lengths)
            ]
        return widths


class CsvStyle(Style):
    def __init__(
        self,
        delimiter: str = ",",
        render_headers: bool = True,
        quoting: Union[Literal["minimal"], Literal["all"]] = "minimal",
        quote_char: str = '"',
    ):
        assert len(delimiter) == 1
        assert len(quote_char) == 1

        self._delim: str = delimiter
        self._render_headers: bool = render_headers
        self._quoting: str = quoting
        self._quote_char: str = quote_char

    def render(
        self,
        table: Table,
        column_alignments: Optional[Dict[str, Alignment]] = None,
        default_alignment: Alignment = Alignment.LEFT,
        column_precisions: Optional[Dict[str, int]] = None,
        default_precision: int = DEFAULT_PRECISION,
    ) -> str:
        values = table.values_as_str(column_precisions, default_precision)
        values = self._quote(values)
        rows = [self._delim.join(row) for row in values]
        cells = "\n".join(rows)

        columns = self._delim.join(table.columns)
        out = "\n".join([columns, cells])
        out += "\n"
        return out

    def _quote(self, _values: VALUES_STR_TYPE) -> VALUES_STR_TYPE:
        if self._quoting == "minimal":
            values = [
                [
                    self._quote_char + cell + self._quote_char
                    if self._delim in cell
                    else cell
                    for cell in row
                ]
                for row in _values
            ]
        elif self._quoting == "all":
            values = [
                [self._quote_char + cell + self._quote_char for cell in row]
                for row in _values
            ]
        else:
            assert False

        return values

    @classmethod
    def render_with_defaults(
        cls,
        table: Table,
        column_alignments: Optional[Dict[str, Alignment]] = None,
        default_alignment: Alignment = Alignment.LEFT,
        column_precisions: Optional[Dict[str, int]] = None,
        default_precision: int = DEFAULT_PRECISION,
    ) -> str:
        style = cls()
        out = style.render(
            table,
            column_alignments,
            default_alignment,
            column_precisions,
            default_precision,
        )
        return out


class MarkdownStyle(AsciiStyle):
    def __init__(self):
        super().__init__(
            joint_element="|",
            horizontal_element="-",
            vertical_element="|",
            pad_element=" ",
            pad_amount=1,
            render_top_border=False,
            render_header_separator=True,
            render_bottom_border=False,
            render_left_border=True,
            render_right_border=True,
        )

    def render(
        self,
        table: Table,
        column_alignments: Optional[Dict[str, Alignment]] = None,
        default_alignment: Alignment = Alignment.LEFT,
        column_precisions: Optional[Dict[str, int]] = None,
        default_precision: int = DEFAULT_PRECISION,
    ) -> str:
        headers = table.columns
        values = table.values_as_str(column_precisions, default_precision)
        widths = self._get_column_widths(headers, values)
        assert all([2 <= width for width in widths])
        out = super().render(
            table,
            column_alignments,
            default_alignment,
            column_precisions,
            default_precision,
        )
        return out

    def _render_separator_line(self, widths: List[int], alignments: List[str]) -> str:
        struts: List[str] = [
            self._render_alignment_strut(width, alignment)
            for width, alignment in zip(widths, alignments)
        ]
        out = self._j_el.join(struts)
        out = self._j_el + out + self._j_el
        return out

    def _render_alignment_strut(self, width: int, alignment: str) -> str:
        assert 2 <= width

        if alignment == self._ALIGNMENT_MAP[Alignment.LEFT]:
            ends = (":", "")
        elif alignment == self._ALIGNMENT_MAP[Alignment.CENTER]:
            ends = (":", ":")
        elif alignment == self._ALIGNMENT_MAP[Alignment.RIGHT]:
            ends = ("", ":")
        else:
            assert False

        h_el_count = width - sum([len(e) for e in ends])
        mid = h_el_count * self._h_el

        strut = ends[0] + mid + ends[1]
        strut = self._pad_cell(strut)

        return strut


class MediaWikiStyle(Style):
    _ALIGNMENT_MAP: Dict[Alignment, str] = {
        Alignment.LEFT: "left",
        Alignment.CENTER: "center",
        Alignment.RIGHT: "right",
    }
    _PRE = "{|"
    _POST = "|}"
    _H_LINE = "|-"
    _HEADER_SEP = "!"
    _VALUE_SEP = "|"
    _CLASS = "wikitable"

    def __init__(
        self,
        precision: int = 1,
    ):
        self._precision = precision

    def render(
        self,
        table: Table,
        column_alignments: Optional[Dict[str, Alignment]] = None,
        default_alignment: Alignment = Alignment.RIGHT,
        column_precisions: Optional[Dict[str, int]] = None,
        default_precision: int = DEFAULT_PRECISION,
    ) -> str:
        headers = table.columns
        values = table.values_as_str(column_precisions, default_precision)

        alignments = self._build_alignment_list(
            column_alignments=column_alignments,
            default_alignment=default_alignment,
            columns=headers,
            alignment_map=self._ALIGNMENT_MAP,
        )

        lines = []
        lines.append(self._render_metadata_line())
        lines.append(self._render_header_row(headers))
        lines.append(self._render_separator_line())
        for row in values:
            lines.append(self._render_row_line(row, alignments))
            lines.append(self._render_separator_line())
        lines.pop()
        lines.append(self._render_end_line())

        out = "\n".join(lines)
        out += "\n"
        return out

    def _render_metadata_line(self) -> str:
        return self._PRE + 'class="{:s}"'.format(self._CLASS)

    def _render_end_line(self) -> str:
        return self._POST

    def _render_header_row(self, columns: Iterable[str]) -> str:
        alignments = itertools.repeat(None)
        row = self._render_row(sep=self._HEADER_SEP, row=columns, alignments=alignments)
        return row

    def _render_row_line(self, row: Iterable[str], alignments: Iterable[str]) -> str:
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

    def _render_separator_line(self) -> str:
        return self._H_LINE

    def _make_cell(self, sep: str, cell: str, alignment: Optional[str] = None) -> str:
        cell = sep + cell + sep
        if alignment is not None:
            alignment_rep = 'align="{:s}"'.format(alignment)
            cell = sep + alignment_rep + cell
        return cell

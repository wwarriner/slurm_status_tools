import textwrap
import unittest

from src.table import *


class TestTable(unittest.TestCase):
    def test_empty(self):
        t = Table.empty()
        self.assertEqual(t.shape, (0, 0))
        self.assertEqual(t.height, 0)
        self.assertEqual(t.width, 0)
        self.assertEqual(t.size, 0)
        self.assertEqual(t.columns, [])
        self.assertEqual(t.values, [])

    def test_from_rows(self):
        NA = "."
        rows: List[Dict[str, Any]] = [
            {"a": 1, "d": 4.0},
            {"a": 2, "b": "b"},
            {"b": 3, "c": "c"},
        ]
        t = Table.from_rows(rows, na_value=NA)
        self.assertEqual(t.shape, (3, 4))
        self.assertEqual(t.height, 3)
        self.assertEqual(t.width, 4)
        self.assertEqual(t.size, 12)
        self.assertEqual(t.columns, ["a", "b", "c", "d"])
        self.assertEqual(
            t.values, [[1, NA, NA, 4.0], [2, "b", NA, NA], [NA, 3, "c", NA]]
        )

    def test_subset_columns(self):
        NA = "."
        rows: List[Dict[str, Any]] = [{"a": 1, "b": 2, "c": 3}, {"a": 4}]
        t = Table.from_rows(rows, na_value=NA)

        sub = t.subset_columns("a")
        self.assertEqual(sub.shape, (2, 1))
        self.assertEqual(sub.height, 2)
        self.assertEqual(sub.width, 1)
        self.assertEqual(sub.size, 2)
        self.assertEqual(sub.columns, ["a"])
        self.assertEqual(sub.values, [[1], [4]])

        sub = t.subset_columns(0)
        self.assertEqual(sub.shape, (2, 1))
        self.assertEqual(sub.height, 2)
        self.assertEqual(sub.width, 1)
        self.assertEqual(sub.size, 2)
        self.assertEqual(sub.columns, ["a"])
        self.assertEqual(sub.values, [[1], [4]])

        sub = t.subset_columns(["b", "c"])
        self.assertEqual(sub.shape, (2, 2))
        self.assertEqual(sub.height, 2)
        self.assertEqual(sub.width, 2)
        self.assertEqual(sub.size, 4)
        self.assertEqual(sub.columns, ["b", "c"])
        self.assertEqual(sub.values, [[2, 3], [NA, NA]])

        sub = t.subset_columns([1, 2])
        self.assertEqual(sub.shape, (2, 2))
        self.assertEqual(sub.height, 2)
        self.assertEqual(sub.width, 2)
        self.assertEqual(sub.size, 4)
        self.assertEqual(sub.columns, ["b", "c"])
        self.assertEqual(sub.values, [[2, 3], [NA, NA]])


class TestAsciiStyle(unittest.TestCase):
    def test_render(self):
        NA = "."
        t = Table.from_rows(
            [
                {"l": "left", "c": "center", "r": "right", "d": "left"},
                {"l": 1, "c": 1.2, "r": 2.3, "d": 3.4},
            ],
            NA,
        )
        align = {"l": Alignment.LEFT, "c": Alignment.CENTER, "r": Alignment.RIGHT}
        precisions = {"l": 1, "c": 3, "r": 0}
        style = AsciiStyle()
        actual = style.render(
            t,
            column_alignments=align,
            default_alignment=Alignment.LEFT,
            column_precisions=precisions,
            default_precision=1,
        )
        expected = textwrap.dedent(
            """
            +----+------+-----+----+
            |l   |  c   |    r|d   |
            +----+------+-----+----+
            |left|center|right|left|
            |1   |1.200 |    2|3.4 |
            +----+------+-----+----+
            """
        )[1:]
        self.assertEqual(actual, expected)


class TestCsvStyle(unittest.TestCase):
    def test_render(self):
        NA = "."
        t = Table.from_rows(
            [
                {"l": "left", "c": "center", "r": "right", "d": "left"},
                {"l": 1, "c": 1.2, "r": 2.3, "d": 3.4},
            ],
            NA,
        )
        align = {"l": Alignment.LEFT, "c": Alignment.CENTER, "r": Alignment.RIGHT}
        precisions = {"l": 1, "c": 3, "r": 0}
        style = CsvStyle()
        actual = style.render(
            t,
            column_alignments=align,
            default_alignment=Alignment.LEFT,
            column_precisions=precisions,
            default_precision=1,
        )
        expected = textwrap.dedent(
            """
            l,c,r,d
            left,center,right,left
            1,1.200,2,3.4
            """
        )[1:]
        self.assertEqual(actual, expected)


class TestMarkdownStyle(unittest.TestCase):
    def test_render(self):
        NA = "."
        t = Table.from_rows(
            [
                {"l": "left", "c": "center", "r": "right", "d": "left"},
                {"l": 1, "c": 1.2, "r": 2.3, "d": 3.4},
            ],
            NA,
        )
        align = {"l": Alignment.LEFT, "c": Alignment.CENTER, "r": Alignment.RIGHT}
        precisions = {"l": 1, "c": 3, "r": 0}
        style = MarkdownStyle()
        actual = style.render(
            t,
            column_alignments=align,
            default_alignment=Alignment.LEFT,
            column_precisions=precisions,
            default_precision=1,
        )
        expected = textwrap.dedent(
            """
            | l    |   c    |     r | d    |
            | :--- | :----: | ----: | :--- |
            | left | center | right | left |
            | 1    | 1.200  |     2 | 3.4  |
            """
        )[1:]
        self.assertEqual(actual, expected)


class TestMediaWiki(unittest.TestCase):
    def test_render(self):
        NA = "."
        t = Table.from_rows(
            [
                {"l": "left", "c": "center", "r": "right", "d": "left"},
                {"l": 1, "c": 1.2, "r": 2.3, "d": 3.4},
            ],
            NA,
        )
        align = {"l": Alignment.LEFT, "c": Alignment.CENTER, "r": Alignment.RIGHT}
        precisions = {"l": 1, "c": 3, "r": 0}
        style = MediaWikiStyle()
        actual = style.render(
            t,
            column_alignments=align,
            default_alignment=Alignment.LEFT,
            column_precisions=precisions,
            default_precision=1,
        )
        expected = textwrap.dedent(
            """
            {|class="wikitable"
            !l!!c!!r!!d
            |-
            |align="left"|left||align="center"|center||align="right"|right||align="left"|left
            |-
            |align="left"|1||align="center"|1.200||align="right"|2||align="left"|3.4
            |}
            """
        )[1:]
        self.assertEqual(actual, expected)

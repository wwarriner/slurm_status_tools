import unittest

from src.interpret import _common
from src.interpret.decode import (
    dont_care_int,
    na_str,
    ns_int,
    unlimited_int,
    yes_no_bool,
)


class DecodeTest(unittest.TestCase):
    def test_na_str(self):
        self.assertEqual(na_str(_common.NA), None)
        self.assertEqual(na_str(""), "")
        self.assertEqual(na_str("hello"), "hello")

    def test_ns_int(self):
        self.assertEqual(ns_int(""), None)
        self.assertEqual(ns_int(_common.NS), None)
        self.assertEqual(ns_int("0"), 0)

    def test_dont_care_int(self):
        self.assertEqual(dont_care_int(""), None)
        self.assertEqual(dont_care_int(_common.DONT_CARE), None)
        self.assertEqual(dont_care_int("-1"), None)
        self.assertEqual(dont_care_int("0"), 0)

    def test_unlimited_int(self):
        self.assertEqual(unlimited_int(""), None)
        self.assertEqual(unlimited_int(_common.UNLIMITED), None)
        self.assertEqual(unlimited_int("-1"), None)
        self.assertEqual(unlimited_int("0"), 0)

    def test_yes_no_bool(self):
        self.assertEqual(yes_no_bool(""), None)
        self.assertEqual(yes_no_bool("other"), None)
        self.assertEqual(yes_no_bool(_common.YES), True)
        self.assertEqual(yes_no_bool(_common.NO), False)

    def test_int_bool(self):
        raise NotImplementedError()

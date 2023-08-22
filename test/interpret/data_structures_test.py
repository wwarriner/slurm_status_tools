import unittest
from test.interpret._strategies import dont_care_integers, u8_integers

from hypothesis import given
from hypothesis.strategies import composite

from src.interpret.data_structures import ExitCode, NTasksPerNBSC, ReqBSCT


@composite
def exit_code_strings(draw):
    code = draw(u8_integers())
    signal = draw(u8_integers())
    return f"{code}:{signal}"


class ExitCodeTest(unittest.TestCase):
    @given(exit_code_strings())
    def test_roundtrip(self, s):
        ec = ExitCode.from_string(s)
        self.assertEqual(s, str(ec))


@composite
def bsct_style_strings(draw):
    b = draw(dont_care_integers())
    s = draw(dont_care_integers())
    c = draw(dont_care_integers())
    t = draw(dont_care_integers())
    return f"{b}:{s}:{c}:{t}"


class NTasksPerNBSCTest(unittest.TestCase):
    @given(bsct_style_strings())
    def test_roundtrip(self, s):
        rbsct = NTasksPerNBSC.from_string(s)
        self.assertEqual(s, str(rbsct))


class ReqBSCTTest(unittest.TestCase):
    @given(bsct_style_strings())
    def test_roundtrip(self, s):
        rbsct = ReqBSCT.from_string(s)
        self.assertEqual(s, str(rbsct))

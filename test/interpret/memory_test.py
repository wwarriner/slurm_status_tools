import unittest
from test.interpret._strategies import positive_u16_integers

from hypothesis import given
from hypothesis.strategies import composite, sampled_from

from src.interpret.memory import BYTE_CONVERSIONS, MULTIPLIER_COUNT_CODES, MemorySpec


@composite
def memory_spec_strings(draw):
    si_count = draw(positive_u16_integers())
    si_prefix = draw(sampled_from(list(BYTE_CONVERSIONS.keys())))
    multiplier_code = draw(sampled_from(MULTIPLIER_COUNT_CODES))
    return f"{si_count}{si_prefix}{multiplier_code}"


class MemorySpecTest(unittest.TestCase):
    @given(memory_spec_strings())
    def test_roundtrip(self, s):
        ms = MemorySpec.from_string(s)
        self.assertEqual(s, str(ms))

    def test_call(self):
        msn = MemorySpec.from_string("12gn")
        assert msn is not None
        self.assertEqual(msn(2, 3), 3 * 12 * 1024**3)

        msc = MemorySpec.from_string("8mc")
        assert msc is not None
        self.assertEqual(msc(3, 4), 3 * 8 * 1024**2)

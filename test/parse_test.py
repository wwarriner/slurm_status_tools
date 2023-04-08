import unittest

from hypothesis import given
from hypothesis.strategies import characters, composite, data, lists, text, tuples

from src.parse import *


@composite
def scontrol_line(draw):
    keys = text(characters(blacklist_characters=(" =")), min_size=1)
    values = text(characters()).filter(lambda x: not x.startswith(" "))
    pairs = draw(lists(tuples(keys, values)))
    return " ".join(["=".join(pair) for pair in pairs])


class TestParse(unittest.TestCase):
    @given(scontrol_line())
    def test_tokenize_scontrol_line_concatenates_identically(self, s):
        t = tokenize_scontrol_line(s)
        c = " ".join(["=".join(pairs) for pairs in reversed(t)])
        assert s == c


if __name__ == "__main__":
    unittest.main()

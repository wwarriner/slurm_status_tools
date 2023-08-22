import unittest
from test.interpret._strategies import u16_integers

from hypothesis import assume, given
from hypothesis.strategies import characters, composite, lists, one_of, text

from src.interpret.node import NodeList


@composite
def prefix_strings(draw):
    return draw(
        text(characters(whitelist_categories=["Lu", "Ll"]), min_size=1, max_size=8)
    )


@composite
def node_spec_single_strings(draw):
    prefix = draw(prefix_strings())
    value = draw(u16_integers())
    return f"{prefix}{value:05d}"


@composite
def node_spec_range_strings(draw):
    prefix = draw(prefix_strings())
    lo: int = draw(u16_integers())
    hi: int = draw(u16_integers())
    lo, hi = (min(lo, hi), max(lo, hi))
    assume(lo < hi)
    return f"{prefix}[{lo:05d}-{hi:05d}]"


@composite
def node_spec_strings(draw):
    return draw(one_of(node_spec_single_strings(), node_spec_range_strings()))


@composite
def node_list_strings(draw):
    node_list = draw(lists(node_spec_strings(), min_size=1, max_size=8))
    return ",".join(node_list)


class NodeListTest(unittest.TestCase):
    @given(node_list_strings())
    def test_roundtrip(self, s):
        nl = NodeList.from_string(s)
        self.assertEqual(s, str(nl))

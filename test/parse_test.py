import unittest

from hypothesis import given
from hypothesis.strategies import (
    characters,
    composite,
    integers,
    lists,
    none,
    one_of,
    text,
    tuples,
)

from src.parse import *


@composite
def scontrol_line(draw):
    keys = text(characters(blacklist_characters=(" =")), min_size=1)
    values = text(characters()).filter(lambda x: not x.startswith(" "))
    pairs = draw(lists(tuples(keys, values)))
    return " ".join(["=".join(pair) for pair in pairs])


@composite
def hyphenated_range(draw):
    MAX_VALUE = 1024
    value_min = draw(integers(min_value=0, max_value=MAX_VALUE))
    value_max = draw(integers(min_value=value_min, max_value=MAX_VALUE))
    pair = (str(value_min), str(value_max))
    return "-".join(pair)


class TestParse(unittest.TestCase):
    @given(scontrol_line())
    def test_tokenize_scontrol_line_recomposes(self, s):
        t = tokenize_scontrol_line(s)
        c = " ".join(["=".join(pairs) for pairs in reversed(t)])
        self.assertEqual(c, s)

    @given(hyphenated_range())
    def test_hyphenated_range_to_list_recomposes(self, s):
        t = hyphenated_range_to_list(s)
        c = "-".join((str(t[0]), str(t[-1])))
        self.assertEqual(c, s)

    @given(hyphenated_range())
    def test_hyphenated_range_to_list_contains_all(self, s):
        t = hyphenated_range_to_list(s)
        diff = [second - first for first, second in zip(t[:-1], t[1:])]
        check = all([d == 1 for d in diff]) and len(t) == (t[-1] - t[0] + 1)
        self.assertTrue(check)

    def test_hyphenated_range_to_list_boundaries(self):
        self.assertEqual(hyphenated_range_to_list(""), [])
        self.assertEqual(hyphenated_range_to_list("a"), [])
        self.assertEqual(hyphenated_range_to_list("-"), [])
        self.assertEqual(hyphenated_range_to_list("0"), [])
        self.assertEqual(hyphenated_range_to_list("0-"), [])
        self.assertEqual(hyphenated_range_to_list("-0"), [])
        self.assertEqual(hyphenated_range_to_list("0--0"), [])
        self.assertEqual(hyphenated_range_to_list("0-0-"), [0])
        self.assertEqual(hyphenated_range_to_list("0-0-1"), [0])

    def test_hyphenated_csl_to_list_boundaries(self):
        self.assertEqual(hyphenated_csl_to_list(""), [])
        self.assertEqual(hyphenated_csl_to_list("a"), [])
        self.assertEqual(hyphenated_csl_to_list("-"), [])
        self.assertEqual(hyphenated_csl_to_list(","), [])
        self.assertEqual(hyphenated_csl_to_list("0-"), [])
        self.assertEqual(hyphenated_csl_to_list("-0"), [])
        self.assertEqual(hyphenated_csl_to_list(",-"), [])
        self.assertEqual(hyphenated_csl_to_list("-,"), [])
        self.assertEqual(hyphenated_csl_to_list("0,"), [0])
        self.assertEqual(hyphenated_csl_to_list(",0"), [0])
        self.assertEqual(hyphenated_csl_to_list("-0,"), [])
        self.assertEqual(hyphenated_csl_to_list("0-,"), [])
        self.assertEqual(hyphenated_csl_to_list("0--0,"), [])
        self.assertEqual(hyphenated_csl_to_list("0-0-,"), [0])
        self.assertEqual(hyphenated_csl_to_list("0-0-1,"), [0])
        self.assertEqual(hyphenated_csl_to_list(",-0"), [])
        self.assertEqual(hyphenated_csl_to_list(",0-"), [])
        self.assertEqual(hyphenated_csl_to_list(",0--0"), [])
        self.assertEqual(hyphenated_csl_to_list(",0-0-"), [0])
        self.assertEqual(hyphenated_csl_to_list(",0-0-1"), [0])

    def test_interpret_gpu_scontrol_show_node(self):
        fn = parse_gpu_scontrol_show_node
        self.assertEqual(fn(""), {})
        self.assertEqual(fn(":"), {})
        self.assertEqual(fn(":"), {})
        self.assertEqual(fn(":a"), {})
        self.assertEqual(fn(":0"), {})
        self.assertEqual(fn(":1"), {})
        self.assertEqual(fn("::"), {})
        self.assertEqual(fn("::0"), {})
        self.assertEqual(fn("::1"), {})
        self.assertEqual(fn(":a:"), {})
        self.assertEqual(fn(":a:0"), {})
        self.assertEqual(fn(":a:1"), {})
        self.assertEqual(fn("a"), {})
        self.assertEqual(fn("a:"), {})
        self.assertEqual(fn("a:a"), {})
        self.assertEqual(fn("a:0"), {})
        self.assertEqual(fn("a:1"), {})
        self.assertEqual(fn("a::"), {})
        self.assertEqual(fn("a::0"), {})
        self.assertEqual(fn("a::1"), {})
        self.assertEqual(fn("a:a:"), {})
        self.assertEqual(fn("a:a:0"), {})
        self.assertEqual(fn("a:a:1"), {})
        self.assertEqual(fn("gpu"), {})
        self.assertEqual(fn("gpu:"), {})
        self.assertEqual(fn("gpu:a"), {})
        self.assertEqual(fn("gpu:0"), {})
        self.assertEqual(fn("gpu:1"), {})
        self.assertEqual(fn("gpu::"), {})
        self.assertEqual(fn("gpu::0"), {})
        self.assertEqual(fn("gpu::1"), {})
        self.assertEqual(fn("gpu:a:"), {})
        self.assertEqual(fn("gpu:a:0"), {})
        self.assertEqual(fn("gpu:a:1"), {"a": 1})
        self.assertEqual(fn("gpu:a:1,gpu:a:1"), {"a": 2})
        self.assertEqual(fn("gpu:a:1,gpu:b:1"), {"a": 1, "b": 1})

    def test_interpret_gpu_scontrol_show_job(self):
        fn = parse_gpu_scontrol_show_job
        self.assertEqual(fn(""), 0)
        self.assertEqual(fn("0"), 0)
        self.assertEqual(fn("="), 0)
        self.assertEqual(fn("=0"), 0)
        self.assertEqual(fn("a"), 0)
        self.assertEqual(fn("a="), 0)
        self.assertEqual(fn("a=0"), 0)
        self.assertEqual(fn(","), 0)
        self.assertEqual(fn("a,"), 0)
        self.assertEqual(fn("a=,"), 0)
        self.assertEqual(fn("a=0,"), 0)
        self.assertEqual(fn(",a"), 0)
        self.assertEqual(fn(",a="), 0)
        self.assertEqual(fn(",a=0"), 0)
        self.assertEqual(fn("gres/gpu"), 0)
        self.assertEqual(fn("gres/gpu="), 0)
        self.assertEqual(fn("gres/gpu=1"), 1)
        self.assertEqual(fn("gres/gpu=1,gres/gpu=1"), 2)

    def test_parse_slurm_duration_as_hours(self):
        fn = parse_slurm_duration_as_hours
        self.assertEqual(fn(""), -1)
        self.assertEqual(fn("0"), -1)
        self.assertEqual(fn("1"), -1)
        self.assertEqual(fn("-"), -1)
        self.assertEqual(fn(":"), -1)
        self.assertEqual(fn("1-01:"), -1)
        self.assertEqual(fn("1-01:02:"), -1)
        self.assertEqual(fn("1-1:02:03"), -1)
        self.assertEqual(fn("1-01:2:03"), -1)
        self.assertEqual(fn("1-01:02:3"), -1)
        self.assertEqual(fn("1-01:02:03"), 25)

    def test_parse_memory_value_to_bytes(self):
        fn = parse_memory_value_to_bytes
        self.assertEqual(fn(""), -1)
        self.assertEqual(fn("k"), -1)
        self.assertEqual(fn("m"), -1)
        self.assertEqual(fn("g"), -1)
        self.assertEqual(fn("t"), -1)
        self.assertEqual(fn("1"), -1)
        self.assertEqual(fn("10"), -1)
        self.assertEqual(fn("1k"), 1024)
        self.assertEqual(fn("1K"), 1024)
        self.assertEqual(fn("1m"), 1048576)
        self.assertEqual(fn("1g"), 1073741824)
        self.assertEqual(fn("1t"), 1099511627776)

    def test_parse_nodelist(self):
        fn = parse_nodelist
        self.assertEqual(fn(""), [])
        self.assertEqual(fn("c"), [])
        self.assertEqual(fn("0"), [])
        self.assertEqual(fn("1"), [])
        self.assertEqual(fn("["), [])
        self.assertEqual(fn("]"), [])
        self.assertEqual(fn(","), [])
        self.assertEqual(fn("[]"), [])
        self.assertEqual(fn("[,]"), [])
        self.assertEqual(fn("[1]"), [])
        self.assertEqual(fn("c[]"), [])
        self.assertEqual(fn("c[a]"), [])
        self.assertEqual(fn("c[,]"), [])
        self.assertEqual(fn("c[1]"), ["c1"])
        self.assertEqual(fn("c[1,a]"), ["c1"])
        self.assertEqual(fn("c[1,2]"), ["c1", "c2"])
        self.assertEqual(fn("c[1-3]"), ["c1", "c2", "c3"])
        self.assertEqual(fn("c[1-3,a]"), ["c1", "c2", "c3"])
        self.assertEqual(fn("c[a-3,1]"), ["c1"])
        self.assertEqual(fn("c[1]", 4), ["c0001"])


if __name__ == "__main__":
    unittest.main()

import unittest

from src.interpret.tres import TresSpec


class TresSpecTest(unittest.TestCase):
    def test_roundtrip(self):
        s = 'cpu=1,mem=2,gres/gpu=3,license="ansys"'
        tres_spec = TresSpec.from_string(s)
        self.assertEqual(s, str(tres_spec))

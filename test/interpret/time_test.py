import datetime as dt
import unittest

from src.interpret.time import (
    duration_timedelta,
    minutes_timedelta,
    seconds_timedelta,
    timepoint_datetime,
)


class TimeTest(unittest.TestCase):
    def test_duration_timedelta(self):
        Z = dt.timedelta()
        S = dt.timedelta(seconds=1)
        M = dt.timedelta(minutes=1)
        H = dt.timedelta(hours=1)
        D = dt.timedelta(days=1)

        self.assertEqual(duration_timedelta(""), None)
        self.assertEqual(duration_timedelta("0"), None)

        self.assertEqual(duration_timedelta("00"), Z)
        self.assertEqual(duration_timedelta("01"), M)

        self.assertEqual(duration_timedelta("00:00"), Z)
        self.assertEqual(duration_timedelta("00:01"), S)
        self.assertEqual(duration_timedelta("01:00"), M)

        self.assertEqual(duration_timedelta("00:00:00"), Z)
        self.assertEqual(duration_timedelta("00:00:01"), S)
        self.assertEqual(duration_timedelta("00:01:00"), M)
        self.assertEqual(duration_timedelta("01:00:00"), H)

        self.assertEqual(duration_timedelta("0-00"), Z)
        self.assertEqual(duration_timedelta("0-01"), H)
        self.assertEqual(duration_timedelta("1-00"), D)

        self.assertEqual(duration_timedelta("0-00:00"), Z)
        self.assertEqual(duration_timedelta("0-00:01"), M)
        self.assertEqual(duration_timedelta("0-01:00"), H)
        self.assertEqual(duration_timedelta("1-00:00"), D)

        self.assertEqual(duration_timedelta("0-00:00:00"), Z)
        self.assertEqual(duration_timedelta("0-00:00:01"), S)
        self.assertEqual(duration_timedelta("0-00:01:00"), M)
        self.assertEqual(duration_timedelta("0-01:00:00"), H)
        self.assertEqual(duration_timedelta("1-00:00:00"), D)

    def test_seconds_timedelta(self):
        S = lambda s: dt.timedelta(seconds=s)

        self.assertEqual(seconds_timedelta(""), None)
        self.assertEqual(seconds_timedelta("0"), S(0))
        self.assertEqual(seconds_timedelta("1"), S(1))

    def test_minutes_timedelta(self):
        M = lambda m: dt.timedelta(minutes=m)

        self.assertEqual(minutes_timedelta(""), None)
        self.assertEqual(minutes_timedelta("0"), M(0))
        self.assertEqual(minutes_timedelta("1"), M(1))

    def test_timepoint_datetime(self):
        self.assertEqual(timepoint_datetime(""), None)
        self.assertEqual(timepoint_datetime("2022"), None)
        self.assertEqual(timepoint_datetime("2022-01-01"), None)
        self.assertEqual(
            timepoint_datetime("2022-01-01T12:34:56"),
            dt.datetime(year=2022, month=1, day=1, hour=12, minute=34, second=56),
        )

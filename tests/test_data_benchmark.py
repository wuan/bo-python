import pytest

from blitzortung.data import Timestamp, Event
from blitzortung.geom import Envelope
from blitzortung.types import Point


@pytest.fixture
def timestamp() -> Timestamp:
    return Timestamp()


@pytest.fixture
def point() -> Point:
    x = 5.0
    y = 3.0
    return Point(x, y)


def test_bench_event(timestamp, benchmark):
    benchmark.pedantic(Event, args=(timestamp, point), rounds=1000, iterations=100)


def test_bench_point(timestamp, benchmark):
    benchmark.pedantic(Point, args=(2, 3), rounds=1000, iterations=100)


def test_bench_envelope(timestamp, benchmark):
    benchmark.pedantic(Envelope, args=(-3, 3, 5, 10), rounds=1000, iterations=100)

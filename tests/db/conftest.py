import datetime
import os
from typing import Generator

import blitzortung.db
import psycopg2
import pyproj
import pytest


@pytest.fixture
def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


@pytest.fixture
def time_interval(now) -> blitzortung.db.query.TimeInterval:
    timedelta = datetime.timedelta(seconds=1)
    return blitzortung.db.query.TimeInterval(now - timedelta, now + timedelta)


@pytest.fixture
def utm_eu() -> pyproj.CRS:
    return pyproj.CRS('epsg:32633')

@pytest.fixture
def grid_factory(utm_eu):
    return blitzortung.geom.GridFactory(10, 15, 40, 50, utm_eu, 15, 45)

@pytest.fixture
def global_grid_factory(utm_eu):
    return blitzortung.geom.GridFactory(-180, 180, -90, 90, utm_eu, 11, 48)

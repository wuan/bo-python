import datetime
import os
from typing import Generator, Callable

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
def utm_north():
    return pyproj.CRS('epsg:32631') # UTM 31 N / WGS84

@pytest.fixture
def utm_south():
    return pyproj.CRS('epsg:32731')  # UTM 31 S / WGS84

@pytest.fixture
def local_grid_factory(utm_north, utm_south) -> Callable[[int, int, int], blitzortung.geom.GridFactory]:
    def _factory(x : int, y: int, data_area=5):
        data_area_size_factor = 3
        size = data_area * data_area_size_factor
        reference_longitude = (x - 1) * data_area
        reference_latitude = (y - 1) * data_area
        center_latitude = reference_latitude + size / 2.0
        longitude_extension = abs(center_latitude) / 15.0
        utm_longitude = 3
        local_grid = blitzortung.geom.GridFactory(
            reference_longitude - longitude_extension,
            reference_longitude + size + longitude_extension,
            reference_latitude,
            reference_latitude + size,
            utm_north if reference_latitude >= 0 else utm_south,
            utm_longitude,
            reference_latitude + size / 2.0
        )
        return local_grid
    return _factory

@pytest.fixture
def global_grid_factory(utm_eu):
    return blitzortung.geom.GridFactory(-180, 180, -90, 90, utm_eu, 11, 48)

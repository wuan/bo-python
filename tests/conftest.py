import datetime
import os
from pathlib import Path
from typing import Callable

# On PyPy psycopg2cffi provides the psycopg2 API, but only once it has been
# registered.  Register it before importing psycopg2 so the tests work under
# both CPython and PyPy.
try:
    from psycopg2cffi import compat as _psycopg2cffi_compat
except ImportError:  # pragma: no cover - CPython without psycopg2cffi
    pass
else:
    _psycopg2cffi_compat.register()

import psycopg2
import pyproj
import pytest
from testcontainers.community.postgres import PostgresContainer

import blitzortung.db


# The canonical schema that is also deployed in production.  The fixture below
# applies it verbatim so the tests (and benchmarks) run against the same table
# and index set as production instead of a reduced hand-written approximation.
SCHEMA_PATH = Path(blitzortung.__file__).parent.parent / "docs" / "schema" / "strikes.sql"


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
    return pyproj.CRS('epsg:32631')  # UTM 31 N / WGS84


@pytest.fixture
def utm_south():
    return pyproj.CRS('epsg:32731')  # UTM 31 S / WGS84


@pytest.fixture
def local_grid_factory(utm_north, utm_south) -> Callable[[int, int, int], blitzortung.geom.GridFactory]:
    def _factory(x: int, y: int, data_area=5):
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


@pytest.fixture(scope="session")
def postgres_container(request) -> PostgresContainer:
    return create_postgres_container(request)

def create_postgres_container(request) -> PostgresContainer:
    # The official postgis/postgis image only publishes amd64 manifests, which
    # breaks the suite on Apple Silicon.  Default to the multi-arch community
    # build and allow overriding it for other environments.
    image = os.environ.get("BLITZORTUNG_TEST_POSTGIS_IMAGE", "imresamu/postgis:16-3.5")
    postgres = PostgresContainer(image)

    def remove_container():
        postgres.stop()

    if request is not None:
        request.addfinalizer(remove_container)

    postgres.start()

    return postgres


@pytest.fixture(scope="session")
def connection_string(postgres_container: PostgresContainer):
    yield f"host={postgres_container.get_container_host_ip()} dbname={postgres_container.dbname} user={postgres_container.username} password={postgres_container.password} port={postgres_container.get_exposed_port(5432)}"


@pytest.fixture
def connection_pool(connection_string):
    connection_pool = psycopg2.pool.ThreadedConnectionPool(4, 50, connection_string)
    yield connection_pool
    connection_pool.closeall()


@pytest.fixture
def db_strikes(connection_pool):
    conn = connection_pool.getconn()

    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()

    query_builder = blitzortung.db.query_builder.Strike()
    strike_builder = blitzortung.builder.strike.Strike()
    mapper = blitzortung.db.mapper.Strike(strike_builder)

    strike = blitzortung.db.table.Strike(connection_pool, query_builder, mapper)
    yield strike
    strike.close()

    with conn.cursor() as cur:
        cur.execute("""DROP TABLE strikes""")
    conn.commit()

    connection_pool.putconn(conn)


@pytest.fixture
def seed_strikes(db_strikes):
    """Insert synthetic strikes for performance tests.

    Returns a callable ``seed(count=..., region=...) -> TimeInterval`` that
    inserts ``count`` evenly spaced strikes and returns the interval covering
    them.  Using ``insert_many`` keeps seeding itself off the measured path.
    """

    def _seed(count: int = 1000, region: int = 1) -> blitzortung.db.query.TimeInterval:
        end_time = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        start_time = end_time - datetime.timedelta(minutes=30)
        step = (end_time - start_time) / max(count, 1)

        strikes = []
        for index in range(count):
            builder = blitzortung.builder.strike.Strike()
            builder.set_timestamp(start_time + step * index)
            builder.set_x(10.0 + (index % 100) * 0.01)
            builder.set_y(45.0 + (index % 100) * 0.01)
            builder.set_lateral_error(index % 10)
            builder.set_station_count(index % 50)
            strikes.append(builder.build())

        db_strikes.insert_many(strikes, region=region)
        db_strikes.commit()

        return blitzortung.db.query.TimeInterval(start_time, end_time)

    return _seed

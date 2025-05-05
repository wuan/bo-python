import datetime
from typing import Callable

import pytest
from assertpy import assert_that
from mock import Mock, call

from blitzortung import geom, builder
from blitzortung.data import Timestamp
from blitzortung.service.strike import StrikeQuery, StrikeState
from blitzortung.service.strike_grid import StrikeGridQuery, GridParameters, StrikeGridState
from tests.conftest import time_interval


@pytest.fixture
def query_builder():
    return Mock(name='query_builder')


@pytest.fixture
def connection():
    return Mock(name='connection')


@pytest.fixture
def statsd_client():
    return Mock(name='statsd_client')


@pytest.fixture
def strike_mapper():
    return Mock(name='strike_mapper')


class TestStrikeGridQuery:

    @pytest.fixture
    def uut(self, query_builder, strike_mapper):
        return StrikeQuery(query_builder, strike_mapper)

    @pytest.fixture
    def grid_parameters_factory(self, grid_factory) -> Callable[[int, int], GridParameters]:
        def _factory(raster_baselength, region=1):
            return GridParameters(
                grid_factory.get_for(raster_baselength),
                raster_baselength,
                region,
            )

        return _factory

    @pytest.fixture
    def state(self, statsd_client, time_interval, query_builder, connection, strike_mapper ):
        return StrikeState(statsd_client, Timestamp(time_interval.end))

    def test_create(self, uut, time_interval, query_builder, connection, statsd_client, state):
        result, state = uut.create(-600, time_interval, connection, statsd_client)

        query_builder.select_query.assert_called_once_with("strikes", geom.Geometry.default_srid,
                                                           time_interval=time_interval,
                                                           order=uut.id_order, id_interval=None)

        query = query_builder.select_query.return_value
        assert connection.runQuery.call_args == call(str(query), query.get_parameters.return_value)
        assert result == connection.runQuery.return_value

        assert result.addCallback.call_args.args == (uut.build_result,)
        assert result.addCallback.call_args.kwargs["state"] == state

    def test_build_result(self, uut, state, time_interval, strike_mapper):
        strike_mapper.create_object.return_value = \
            builder.Strike() \
                .set_id(1234) \
                .set_timestamp(time_interval.end - datetime.timedelta(seconds=60)) \
                .set_x(123.4) \
                .set_y(45.6) \
                .set_amplitude(1.2) \
                .set_lateral_error(3.4) \
                .set_altitude(234.1) \
                .set_station_count(12) \
                .build()

        query_result = [[567]]

        result = uut.build_result(query_result, state)

        assert result == {'next': 568, 's': ((60, 123.4, 45.6, 234.1, 3.4, 1.2, 12),)}


    def test_build_strikes_response(self, uut, state, time_interval):

        grid_result = {'next': 123, 's': ((60, 123.4, 45.6, 234.1, 3.4, 1.2, 12),)}
        histogram_result = [0, 0, 0, 0, 0, 1]
        result = (grid_result, histogram_result)

        response = uut.build_strikes_response(result, state=state)

        assert response == {
            'h': [0, 0, 0, 0, 0, 1],
            'next': 123,
            's': ((60, 123.4, 45.6, 234.1, 3.4, 1.2, 12),),
            't': time_interval.end.strftime("%Y%m%dT%H:%M:%S"),
        }

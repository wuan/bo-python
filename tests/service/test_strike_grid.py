import datetime
from typing import Callable

import pytest
import pytest_twisted
from assertpy import assert_that
from mock import Mock, call
from twisted.internet import defer

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


class TestStrikeGridQuery:

    @pytest.fixture
    def uut(self, query_builder):
        return StrikeGridQuery(query_builder)

    @pytest.fixture
    def now(self):
        return datetime.datetime.now(tz=datetime.timezone.utc)

    @pytest.fixture
    def grid_parameters_factory(self, grid_factory) -> Callable[[int, int], GridParameters]:
        def _factory(raster_baselength, region=1):
            return GridParameters(
                grid_factory.get_for(raster_baselength),
                raster_baselength,
                region,
            )

        return _factory

    @pytest_twisted.inlineCallbacks
    def test_create(self, uut, now, grid_parameters_factory, time_interval, query_builder, connection, statsd_client):
        grid_parameters = grid_parameters_factory(10000)
        connection.runQuery.return_value = defer.succeed([{
            "rx": 7,
            "ry": 9,
            "strike_count": 3,
            "timestamp": now - datetime.timedelta(seconds=65)
        }])

        deferred_result, state = uut.create(grid_parameters, time_interval, connection, statsd_client)
        result = yield deferred_result

        assert result == ((7, 102, 3, -66),)

        query_builder.grid_query.assert_called_once_with("strikes", grid_parameters.grid, time_interval=time_interval,
                                                         count_threshold=grid_parameters.count_threshold)

        query = query_builder.grid_query.return_value
        assert connection.runQuery.call_args == call(str(query), query.get_parameters.return_value)
        assert state.grid_parameters == grid_parameters

    def test_build_result(self, uut, statsd_client, grid_parameters_factory, time_interval, ):
        grid_parameters = grid_parameters_factory(10000)
        state = StrikeGridState(statsd_client, grid_parameters, time_interval)

        seconds_offset = 65
        timestamp = time_interval.end - datetime.timedelta(seconds=seconds_offset)
        rx = 7
        ry = 9
        query_result = [
            {"rx": rx,
             "ry": ry,
             "strike_count": 3,
             "timestamp": timestamp}
        ]

        result = uut.build_result(query_result, state=state)

        assert result == ((rx, grid_parameters.grid.y_bin_count - ry, 3, -seconds_offset),)

    def test_build_grid_response(self, uut, statsd_client, grid_parameters_factory, time_interval, ):
        grid_parameters = grid_parameters_factory(10000)
        state = StrikeGridState(statsd_client, grid_parameters, time_interval)

        rx = 7
        ry = 102
        seconds_offset = 65

        grid_result = ((rx, ry, 3, -seconds_offset),)
        histogram_result = [0, 0, 0, 0, 0, 1]
        result = (grid_result, histogram_result)

        response = uut.build_grid_response(result, state=state)

        assert response == {
            'dt': 2,
            'h': [0, 0, 0, 0, 0, 1],
            'r': ((7, 102, 3, -65),),
            't': time_interval.end.strftime("%Y%m%dT%H:%M:%S"),
            'x0': 10,
            'xc': 39,
            'xd': 0.127078,
            'y1': 50.0742,
            'yc': 111,
            'yd': 0.089948
        }

        assert_that(response["x0"] + response["xd"] * response["xc"]).is_close_to(grid_parameters.grid.x_max, 0.1)
        assert_that(response["y1"] - response["yd"] * response["yc"]).is_close_to(grid_parameters.grid.y_min, 0.1)


class TestGlobalStrikeGridQuery:

    @pytest.fixture
    def uut(self, query_builder):
        return StrikeGridQuery(query_builder)

    @pytest.fixture
    def grid_parameters_factory(self, global_grid_factory) -> Callable[[int], GridParameters]:
        def _factory(raster_baselength):
            return GridParameters(
                global_grid_factory.get_for(raster_baselength),
                raster_baselength,
            )

        return _factory

    @pytest_twisted.inlineCallbacks
    def test_create(self, uut, now, grid_parameters_factory, time_interval, query_builder, connection, statsd_client):
        grid_parameters = grid_parameters_factory(10000)
        connection.runQuery.return_value = defer.succeed([{
            "rx": 7,
            "ry": 9,
            "strike_count": 3,
            "timestamp": now - datetime.timedelta(seconds=65)
        }])

        deferred_result, state = uut.create(grid_parameters, time_interval, connection, statsd_client)
        result = yield deferred_result

        assert result == ((7, 1898, 3, -66),)

        query_builder.grid_query.assert_called_once_with("strikes", grid_parameters.grid, time_interval=time_interval,
                                                         count_threshold=grid_parameters.count_threshold)

        query = query_builder.grid_query.return_value
        assert connection.runQuery.call_args == call(str(query), query.get_parameters.return_value)

    def test_build_result(self, uut, statsd_client, grid_parameters_factory, time_interval, ):
        grid_parameters = grid_parameters_factory(10000)
        state = StrikeGridState(statsd_client, grid_parameters, time_interval)

        seconds_offset = 65
        timestamp = time_interval.end - datetime.timedelta(seconds=seconds_offset)
        rx = 7
        ry = 9
        query_result = [
            {"rx": rx,
             "ry": ry,
             "strike_count": 3,
             "timestamp": timestamp}
        ]

        result = uut.build_result(query_result, state=state)

        assert result == ((rx, grid_parameters.grid.y_bin_count - ry, 3, -seconds_offset),)

    def test_build_grid_response(self, uut, statsd_client, grid_parameters_factory, time_interval, ):
        grid_parameters = grid_parameters_factory(10000)
        state = StrikeGridState(statsd_client, grid_parameters, time_interval)

        rx = 7
        ry = 102
        seconds_offset = 65

        grid_result = ((rx, ry, 3, -seconds_offset),)
        histogram_result = [0, 0, 0, 0, 0, 1]
        result = (grid_result, histogram_result)

        response = uut.build_grid_response(result, state=state)

        assert response == {
            'dt': 2,
            'h': [0, 0, 0, 0, 0, 1],
            'r': ((7, 102, 3, -65),),
            't': time_interval.end.strftime("%Y%m%dT%H:%M:%S"),
            'x0': -180.0,
            'xc': 2834,
            'xd': 0.127011,
            'y1': 90.0238,
            'yc': 1907,
            'yd': 0.094352
        }

        assert_that(response["x0"] + response["xd"] * response["xc"]).is_close_to(grid_parameters.grid.x_max, 0.1)
        assert_that(response["y1"] - response["yd"] * response["yc"]).is_close_to(grid_parameters.grid.y_min, 0.1)

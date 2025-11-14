import time

import pytest
import pytest_twisted
from mock import Mock, call
from twisted.internet import defer

from blitzortung.service.general import create_time_interval
from blitzortung.service.histogram import HistogramQuery


@pytest.fixture
def query_builder():
    return Mock(name='query_builder')


@pytest.fixture
def connection():
    return Mock(name='connection')


class TestHistogramQuery:

    @pytest.fixture
    def uut(self, query_builder) -> HistogramQuery:
        return HistogramQuery(query_builder)

    @pytest_twisted.inlineCallbacks
    def test_create(self, uut, query_builder, connection):
        connection.runQuery.return_value = defer.succeed([[-4, 5], [-3, 3], [-2, 1], [-1, 2], [0, 4]])

        query_time_interval = create_time_interval(30, 0)

        result = yield uut.create(query_time_interval, connection)

        assert result == [0,5,3,1,2,4]
        query_builder.histogram_query.assert_called_once_with("strikes", query_time_interval, 5, None, None)
        query = query_builder.histogram_query.return_value

        assert connection.runQuery.call_args == call(str(query), query.get_parameters.return_value)

    def test_build_result(self, uut):
        reference_time = time.time() - 0.5
        query_result = [[-2, 3], [-1, 2], [0, 1]]

        result = uut.build_result(query_result, 30, 5, reference_time)

        assert result == [0, 0, 0, 3, 2, 1]

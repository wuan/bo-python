import time

import pytest
from mock import Mock, call

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
    def uut(self, query_builder):
        return HistogramQuery(query_builder)

    def test_create(self, uut, query_builder, connection):
        query_time_interval = create_time_interval(30, 0)
        result = uut.create(query_time_interval, connection)

        query_builder.histogram_query.assert_called_once_with("strikes", query_time_interval, 5, None, None)

        query = query_builder.histogram_query.return_value
        assert connection.runQuery.call_args == call(str(query), query.get_parameters.return_value)
        assert result == connection.runQuery.return_value

        assert result.addCallback.call_args.args == (uut.build_result,)
        assert result.addCallback.call_args.kwargs["minutes"] == 30
        assert result.addCallback.call_args.kwargs["bin_size"] == 5
        now = time.time()
        assert result.addCallback.call_args.kwargs["reference_time"] > now - 0.005
        assert result.addCallback.call_args.kwargs["reference_time"] < now + 0.005

    def test_build_result(self, uut):
        reference_time = time.time() - 0.5
        query_result = [[-2, 3], [-1, 2], [0, 1]]

        result = uut.build_result(query_result, 30, 5, reference_time)

        assert result == [0, 0, 0, 3, 2, 1]

import time

import pytest
from mock import Mock, call

from blitzortung.service.histogram import HistogramQuery


@pytest.fixture
def query_builder():
    return Mock(name='query_builder')


@pytest.fixture
def connection():
    return Mock(name='connection')


@pytest.fixture
def uut(query_builder):
    return HistogramQuery(query_builder)


def test_create(uut, query_builder, connection):

    result = uut.create(connection, 30, 0)

    query_builder.histogram_query.assert_called_once_with("strikes", 30, 0, 5, None, None)

    query = query_builder.histogram_query.return_value
    assert connection.runQuery.call_args == call(str(query), query.get_parameters.return_value)
    assert result == connection.runQuery.return_value

    assert result.addCallback.call_args.args == (uut.build_result,)
    assert result.addCallback.call_args.kwargs["minutes"] == 30
    assert result.addCallback.call_args.kwargs["bin_size"] == 5
    now = time.time()
    assert result.addCallback.call_args.kwargs["reference_time"] > now - 0.001
    assert result.addCallback.call_args.kwargs["reference_time"] < now + 0.001

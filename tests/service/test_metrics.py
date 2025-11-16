import pytest
from unittest.mock import Mock

from blitzortung.service.metrics import StatsDMetrics


@pytest.fixture
def mock_statsd():
    """Create a mock StatsClient for testing."""
    statsd = Mock()
    statsd.incr = Mock()
    statsd.gauge = Mock()
    return statsd


class TestStatsDMetrics:
    """Tests for StatsDMetrics class."""

    @pytest.fixture
    def metrics(self, mock_statsd):
        return StatsDMetrics(mock_statsd)


    def test_init_with_custom_statsd(self, metrics, mock_statsd):
        """Test initialization with custom StatsClient."""
        assert metrics.statsd is mock_statsd

    def test_init_with_default_statsd(self):
        """Test initialization with default StatsClient."""
        metrics = StatsDMetrics()
        assert metrics.statsd is not None
        # Default should create StatsClient with specific config
        assert hasattr(metrics.statsd, 'incr')
        assert hasattr(metrics.statsd, 'gauge')

    @pytest.mark.parametrize("minute_length,is_bg,cache_ratio", [
        (10, True, 0.75),
        (60, False, 0.85),
    ])
    def test_for_global_strikes(self, metrics, mock_statsd, minute_length, is_bg, cache_ratio):
        """Test for_global_strikes with 10 minute length."""

        metrics.for_global_strikes(minute_length=minute_length, cache_ratio=cache_ratio)

        # Verify all expected calls
        assert mock_statsd.incr.call_count == 2 + 2 * is_bg

        mock_statsd.incr.assert_any_call('strikes_grid.total_count')
        mock_statsd.incr.assert_any_call('global_strikes_grid.total_count')
        if is_bg:
            mock_statsd.incr.assert_any_call('strikes_grid.bg_count')
            mock_statsd.incr.assert_any_call('global_strikes_grid.bg_count')

        mock_statsd.gauge.assert_called_once_with('global_strikes_grid.cache_hits', cache_ratio)

    @pytest.mark.parametrize("minute_length,is_bg,cache_ratio,data_area", [
        (10, True, 0.75, 5),
        (60, False, 0.85, 10),
    ])
    def test_for_local_strikes_with_10_minute_length(self, metrics, mock_statsd, minute_length, is_bg, cache_ratio, data_area):
        """Test for_local_strikes."""

        metrics.for_local_strikes(minute_length=minute_length, data_area=data_area, cache_ratio=cache_ratio)

        # Verify all expected calls
        assert mock_statsd.incr.call_count == 3 + 2 * is_bg
        mock_statsd.incr.assert_any_call('strikes_grid.total_count')
        mock_statsd.incr.assert_any_call('local_strikes_grid.total_count')
        mock_statsd.incr.assert_any_call(f'local_strikes_grid.data_area.{data_area}')
        if is_bg:
            mock_statsd.incr.assert_any_call('strikes_grid.bg_count')
            mock_statsd.incr.assert_any_call('local_strikes_grid.bg_count')

        mock_statsd.gauge.assert_called_once_with('local_strikes_grid.cache_hits', cache_ratio)


    @pytest.mark.parametrize("minute_length,is_bg,cache_ratio,region", [
        (10, True, 0.75, 1),
        (60, False, 0.85, 2),
    ])
    def test_for_strikes_with_10_minute_length(self, metrics, mock_statsd, minute_length, is_bg, cache_ratio, region):
        """Test for_strikes on regions."""

        metrics.for_strikes(minute_length=minute_length, region=region, cache_ratio=cache_ratio)

        # Verify all expected calls
        assert mock_statsd.incr.call_count == 2 + 2 * is_bg
        mock_statsd.incr.assert_any_call('strikes_grid.total_count')
        mock_statsd.incr.assert_any_call(f'strikes_grid.total_count.{region}')
        if is_bg:
            mock_statsd.incr.assert_any_call('strikes_grid.bg_count')
            mock_statsd.incr.assert_any_call(f'strikes_grid.bg_count.{region}')

        mock_statsd.gauge.assert_called_once_with('strikes_grid.cache_hits', cache_ratio)

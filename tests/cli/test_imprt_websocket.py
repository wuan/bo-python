"""Tests for blitzortung.cli.imprt_websocket module."""

import datetime
import json
import time
from unittest.mock import MagicMock, Mock, patch

import pytest

import blitzortung
import blitzortung.cli.imprt_websocket as imprt_websocket
from blitzortung.lock import FailedToAcquireException


def make_message(timestamp=None, region=1, delay=1.0):
    """Create a valid strike message as JSON string."""
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)
    return json.dumps({
        "time": int(timestamp.timestamp() * 1000),
        "lat": 32.5,
        "lon": -89.5,
        "alt": 0,
        "region": region,
        "delay": delay,
    })


@pytest.fixture
def strike():
    """A strike returned by the (mocked) builder."""
    strike = MagicMock()
    strike.timestamp.datetime = datetime.datetime.now(datetime.timezone.utc)
    return strike


@pytest.fixture
def strike_builder(strike):
    """A strike builder whose from_json().build() returns a strike."""
    builder = MagicMock()
    builder.from_json.return_value.build.return_value = strike
    return builder


@pytest.fixture(autouse=True)
def module_state(monkeypatch, strike_builder):
    """Isolate the module-level mutable state and collaborators."""
    monkeypatch.setattr(imprt_websocket, "strike_builder", strike_builder)
    monkeypatch.setattr(imprt_websocket, "statsd_client", MagicMock())
    monkeypatch.setattr(imprt_websocket, "strike_db", None)
    monkeypatch.setattr(imprt_websocket, "strike_count", 0)
    monkeypatch.setattr(imprt_websocket, "last_commit_time", time.time())


class TestOnMessage:
    """Tests for the on_message callback."""

    def test_processes_message_without_database(self, strike, strike_builder):
        """Test that a message is parsed, tracked and counted."""
        message = make_message(region=3)

        imprt_websocket.on_message(Mock(), message)

        data = json.loads(message)
        strike_builder.from_json.assert_called_once_with(data)
        imprt_websocket.statsd_client.incr.assert_called_once_with("strikes")
        imprt_websocket.statsd_client.gauge.assert_called_once()
        assert imprt_websocket.strike_count == 1

    def test_inserts_into_database_when_available(self, monkeypatch, strike):
        """Test that strikes are inserted into the database."""
        db = MagicMock()
        monkeypatch.setattr(imprt_websocket, "strike_db", db)

        imprt_websocket.on_message(Mock(), make_message(region=5))

        db.insert.assert_called_once_with(strike, 5)

    def test_accepts_utf8_encoded_message(self):
        """Test that a bytes message is decoded before processing."""
        imprt_websocket.on_message(Mock(), make_message().encode("utf-8"))

        assert imprt_websocket.strike_count == 1

    def test_commits_after_count_threshold(self, monkeypatch):
        """Test that a commit happens once more than 100 strikes arrived."""
        db = MagicMock()
        monkeypatch.setattr(imprt_websocket, "strike_db", db)
        monkeypatch.setattr(imprt_websocket, "strike_count", 100)

        imprt_websocket.on_message(Mock(), make_message())

        db.commit.assert_called_once_with()
        assert imprt_websocket.strike_count == 0

    def test_commits_after_time_threshold(self, monkeypatch):
        """Test that a commit happens when the last one is older than 5s."""
        db = MagicMock()
        monkeypatch.setattr(imprt_websocket, "strike_db", db)
        monkeypatch.setattr(imprt_websocket, "strike_count", 1)
        monkeypatch.setattr(imprt_websocket, "last_commit_time", time.time() - 10)

        imprt_websocket.on_message(Mock(), make_message())

        db.commit.assert_called_once_with()
        assert imprt_websocket.strike_count == 0

    def test_commits_after_count_threshold_without_database(self, monkeypatch):
        """Test the commit path when no database is configured."""
        monkeypatch.setattr(imprt_websocket, "strike_count", 100)
        monkeypatch.setattr(imprt_websocket, "strike_db", None)

        imprt_websocket.on_message(Mock(), make_message())

        assert imprt_websocket.strike_count == 0

    def test_reraises_builder_errors(self, strike_builder):
        """Test that builder errors are re-raised after being logged."""
        strike_builder.from_json.return_value.build.side_effect = ValueError("invalid strike")
        message = make_message()
        websocket = Mock()

        with pytest.raises(ValueError, match="invalid strike"):
            imprt_websocket.on_message(websocket, message)


class TestCallbacks:
    """Tests for the remaining websocket callbacks."""

    def test_on_error_logs_warning(self):
        """Test that errors are logged as warnings."""
        with patch.object(imprt_websocket.logger, "warning") as warning:
            imprt_websocket.on_error(Mock(), ValueError("connection lost"))

        warning.assert_called_once()

    @pytest.mark.parametrize(
        "close_status_code, close_msg",
        [(1000, "Normal closure"), (None, None)],
    )
    def test_on_close_logs_info(self, close_status_code, close_msg):
        """Test that both populated and empty close reasons are logged."""
        with patch.object(imprt_websocket.logger, "info") as info:
            imprt_websocket.on_close(Mock(), close_status_code, close_msg)

        info.assert_called_once()

    def test_on_open_sends_initialization_and_starts_refresher(self):
        """Test the on_open messages and the background refresher thread."""
        ws = MagicMock()
        with patch.object(imprt_websocket.threading, "Thread") as thread:
            imprt_websocket.on_open(ws)

        ws.send.assert_called_once_with('{"a":111}')
        thread.assert_called_once()

    def test_on_open_refresher_sends_and_exits_on_close(self):
        """Test that the refresher sends a message and stops on close."""
        ws = MagicMock()
        ws.send.side_effect = [None, None, imprt_websocket.WebSocketConnectionClosedException()]
        captured = {}

        def capture(target, *args, **kwargs):
            captured["target"] = target
            return MagicMock()

        with patch.object(imprt_websocket.threading, "Thread", side_effect=capture):
            with patch.object(imprt_websocket.time, "sleep"):
                imprt_websocket.on_open(ws)
                # Execute the captured thread target: sends "{}" once, then exits.
                captured["target"]()

        assert ws.send.call_count == 3  # initialization, refresh, failed refresh


class TestMain:
    """Tests for the main() entry point."""

    class StopMain(Exception):
        """Sentinel used to break out of the endless connect loop."""

    def _patch_runtime(self, monkeypatch, options, run_forever_side_effect):
        parser = Mock()
        parser.parse_args.return_value = (options, [])
        monkeypatch.setattr(imprt_websocket, "OptionParser", Mock(return_value=parser))

        lock = MagicMock()
        monkeypatch.setattr(imprt_websocket, "LockWithTimeout", Mock(return_value=lock))

        monkeypatch.setattr(imprt_websocket, "random", Mock(choices=Mock(return_value=[1])))

        ws = Mock()
        ws.run_forever.side_effect = run_forever_side_effect
        monkeypatch.setattr(imprt_websocket.websocket, "WebSocketApp", Mock(return_value=ws))

        return lock, ws

    def test_main_connects_and_imports(self, monkeypatch):
        """Test that main() acquires the lock, opens a DB and connects."""
        options = Mock(debug=False, verbose=False, test=False)
        _, ws = self._patch_runtime(monkeypatch, options, self.StopMain())
        strike_db = MagicMock()
        monkeypatch.setattr(blitzortung.db, "strike", Mock(return_value=strike_db))

        with pytest.raises(self.StopMain):
            imprt_websocket.main()

        imprt_websocket.websocket.WebSocketApp.assert_called_once()
        ws.run_forever.assert_called_once_with(origin='https://www.blitzortung.org', skip_utf8_validation=True)

    def test_main_test_mode_skips_database(self, monkeypatch):
        """Test that the test flag avoids connecting to the database."""
        options = Mock(debug=False, verbose=False, test=True)
        self._patch_runtime(monkeypatch, options, self.StopMain())
        strike_factory = Mock()
        monkeypatch.setattr(blitzortung.db, "strike", strike_factory)

        with pytest.raises(self.StopMain):
            imprt_websocket.main()

        strike_factory.assert_not_called()

    def test_main_debug_enables_trace(self, monkeypatch):
        """Test that the debug flag enables websocket tracing and log level."""
        options = Mock(debug=True, verbose=False, test=False)
        self._patch_runtime(monkeypatch, options, self.StopMain())
        monkeypatch.setattr(blitzortung.db, "strike", Mock(return_value=MagicMock()))

        with patch.object(blitzortung, "set_log_level") as set_log_level:
            with patch.object(imprt_websocket.websocket, "enableTrace") as enable_trace:
                with pytest.raises(self.StopMain):
                    imprt_websocket.main()

        set_log_level.assert_called_once()
        enable_trace.assert_called_once_with(True)

    def test_main_verbose_sets_log_level(self, monkeypatch):
        """Test that the verbose flag only changes the log level."""
        options = Mock(debug=False, verbose=True, test=False)
        self._patch_runtime(monkeypatch, options, self.StopMain())
        monkeypatch.setattr(blitzortung.db, "strike", Mock(return_value=MagicMock()))

        with patch.object(blitzortung, "set_log_level") as set_log_level:
            with patch.object(imprt_websocket.websocket, "enableTrace") as enable_trace:
                with pytest.raises(self.StopMain):
                    imprt_websocket.main()

        set_log_level.assert_called_once()
        enable_trace.assert_not_called()

    def test_main_logs_finished_after_disconnect(self, monkeypatch):
        """Test that a normal disconnect is logged before reconnecting."""
        options = Mock(debug=False, verbose=False, test=True)
        self._patch_runtime(monkeypatch, options, [None, self.StopMain()])

        with patch.object(imprt_websocket.logger, "info") as info:
            with pytest.raises(self.StopMain):
                imprt_websocket.main()

        info.assert_any_call("finished")

    def test_main_logs_failed_lock(self, monkeypatch):
        """Test that a lock timeout is caught and logged."""
        options = Mock(debug=False, verbose=False, test=False)
        lock, _ = self._patch_runtime(monkeypatch, options, self.StopMain())
        lock.locked.side_effect = FailedToAcquireException()

        with patch.object(imprt_websocket.logger, "warning") as warning:
            imprt_websocket.main()

        warning.assert_called_once()

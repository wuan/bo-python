"""Tests for blitzortung.cli.start_webservice module."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def start_webservice(monkeypatch):
    """Import start_webservice with the heavy webservice module mocked out.

    Importing ``blitzortung.cli.webservice`` installs a Twisted reactor and
    creates a database connection pool at import time, which must not happen
    while running the unit tests.
    """
    monkeypatch.setitem(sys.modules, "blitzortung.cli.webservice", MagicMock())
    monkeypatch.delitem(sys.modules, "blitzortung.cli.start_webservice", raising=False)
    monkeypatch.setattr(sys, "argv", list(sys.argv))

    from blitzortung.cli import start_webservice as module

    yield module

    sys.modules.pop("blitzortung.cli.start_webservice", None)


class TestMain:
    """Tests for the main function."""

    def test_returns_run_exit_code(self, start_webservice):
        """Test that main exits with the value returned by run."""
        with patch.object(start_webservice, "run", return_value=0) as mock_run:
            with pytest.raises(SystemExit) as exc_info:
                start_webservice.main()

        assert exc_info.value.code == 0
        mock_run.assert_called_once_with()

    def test_propagates_non_zero_exit_code(self, start_webservice):
        """Test that a failing run result becomes the process exit code."""
        with patch.object(start_webservice, "run", return_value=1):
            with pytest.raises(SystemExit) as exc_info:
                start_webservice.main()

        assert exc_info.value.code == 1

    def test_invokes_twistd_with_webservice_script(self, start_webservice):
        """Test that twistd is pointed at the webservice.py file."""
        with patch.object(start_webservice, "run", return_value=0):
            with pytest.raises(SystemExit):
                start_webservice.main()

        assert sys.argv[0] == "twistd"
        script_index = sys.argv.index("-oy")
        assert sys.argv[script_index + 1] == os.path.join(
            os.path.dirname(os.path.abspath(start_webservice.__file__)), "webservice.py"
        )

    def test_sets_pidfile_by_default(self, start_webservice, monkeypatch):
        """Test that a pidfile is configured when not running in test mode."""
        monkeypatch.delenv("BLITZORTUNG_TEST", raising=False)

        with patch.object(start_webservice, "run", return_value=0):
            with pytest.raises(SystemExit):
                start_webservice.main()

        assert "--pidfile" in sys.argv
        pidfile_index = sys.argv.index("--pidfile")
        assert sys.argv[pidfile_index + 1] == "/var/run/bo-webservice.pid"

    def test_omits_pidfile_in_test_mode(self, start_webservice, monkeypatch):
        """Test that no pidfile is configured when BLITZORTUNG_TEST is set."""
        monkeypatch.setenv("BLITZORTUNG_TEST", "1")

        with patch.object(start_webservice, "run", return_value=0):
            with pytest.raises(SystemExit):
                start_webservice.main()

        assert "--pidfile" not in sys.argv

    def test_run_is_called_after_argv_is_prepared(self, start_webservice):
        """Test that sys.argv is fully prepared before run is invoked."""
        captured = {}

        def fake_run():
            captured["argv"] = list(sys.argv)
            return 0

        with patch.object(start_webservice, "run", side_effect=fake_run):
            with pytest.raises(SystemExit):
                start_webservice.main()

        assert captured["argv"][0] == "twistd"
        assert "-oy" in captured["argv"]
        assert captured["argv"][-1].endswith("webservice.py")

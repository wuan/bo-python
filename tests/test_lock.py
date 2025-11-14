# -*- coding: utf8 -*-

"""

Copyright 2014-2016 Andreas Würl

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import tempfile
from mock import patch

from assertpy import assert_that  # pylint: disable=import-error
import pytest  # pylint: disable=import-error

import blitzortung.lock


class TestFailedToAcquireException:
    """Test suite for FailedToAcquireException."""

    def test_is_exception_subclass(self):
        """Test that FailedToAcquireException is an Exception."""
        assert_that(
            issubclass(blitzortung.lock.FailedToAcquireException, Exception)
        ).is_true()

    def test_can_be_raised(self):
        """Test that FailedToAcquireException can be raised and caught."""
        with pytest.raises(blitzortung.lock.FailedToAcquireException):
            raise blitzortung.lock.FailedToAcquireException()

    def test_can_be_raised_with_message(self):
        """Test that FailedToAcquireException can be raised with a message."""
        with pytest.raises(blitzortung.lock.FailedToAcquireException) as exc_info:
            raise blitzortung.lock.FailedToAcquireException(
                "Lock acquisition timed out"
            )
        assert_that(str(exc_info.value)).contains("timed out")


class TestLockWithTimeout:
    """Test suite for LockWithTimeout class."""

    @pytest.fixture
    def temp_lock_file(self):
        """Create a temporary file for lock testing."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            return tmp.name

    @pytest.fixture
    def lock(self, temp_lock_file):
        """Create a LockWithTimeout instance."""
        return blitzortung.lock.LockWithTimeout(temp_lock_file)

    def test_lock_inherits_from_interprocess_lock(self):
        """Test that LockWithTimeout inherits from InterProcessLock."""
        from fasteners import InterProcessLock  # pylint: disable=import-outside-toplevel,import-error

        assert_that(
            issubclass(blitzortung.lock.LockWithTimeout, InterProcessLock)
        ).is_true()

    def test_lock_has_locked_method(self, lock):
        """Test that LockWithTimeout has locked context manager method."""
        assert_that(hasattr(lock, "locked")).is_true()
        assert_that(callable(lock.locked)).is_true()

    def test_locked_is_context_manager(self, lock):
        """Test that locked method returns a context manager."""
        result = lock.locked(timeout=1.0)
        assert_that(hasattr(result, "__enter__")).is_true()
        assert_that(hasattr(result, "__exit__")).is_true()

    def test_locked_acquires_lock_with_timeout(self):
        """Test that locked method acquires lock with timeout."""
        # This test verifies the context manager can be used
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            lock_file = tmp.name

        lock = blitzortung.lock.LockWithTimeout(lock_file)
        # Just verify it doesn't crash when used
        with patch.object(lock, "acquire", return_value=True):
            with patch.object(lock, "release"):
                with lock.locked(timeout=1.0):
                    pass

    def test_locked_raises_exception_on_failed_acquire(self, lock):
        """Test that locked raises FailedToAcquireException when acquire fails."""
        with patch.object(lock, "acquire", return_value=False):
            with pytest.raises(blitzortung.lock.FailedToAcquireException):
                with lock.locked(timeout=0.1):
                    pass

    def test_locked_calls_acquire_with_timeout(self):
        """Test that locked calls acquire with the specified timeout."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            lock_file = tmp.name

        lock = blitzortung.lock.LockWithTimeout(lock_file)
        with patch.object(lock, "acquire", return_value=True) as mock_acquire:
            with patch.object(lock, "release"):
                with lock.locked(timeout=5.0):
                    pass
                mock_acquire.assert_called_once_with(timeout=5.0)

    def test_locked_releases_lock_after_context(self):
        """Test that locked releases the lock after exiting context."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            lock_file = tmp.name

        lock = blitzortung.lock.LockWithTimeout(lock_file)

        with patch.object(lock, "acquire", return_value=True) as mock_acquire:
            with patch.object(lock, "release") as mock_release:
                with lock.locked(timeout=1.0):
                    mock_acquire.assert_called_once_with(timeout=1.0)
                mock_release.assert_called_once()

    def test_locked_releases_lock_on_exception(self):
        """Test that locked releases lock even when exception occurs in context."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            lock_file = tmp.name

        lock = blitzortung.lock.LockWithTimeout(lock_file)

        with patch.object(lock, "acquire", return_value=True):
            with patch.object(lock, "release") as mock_release:
                try:
                    with lock.locked(timeout=1.0):
                        raise ValueError("Test exception")
                except ValueError:
                    pass
                mock_release.assert_called_once()

    def test_locked_timeout_parameter_is_passed(self):
        """Test that timeout parameter is correctly passed to acquire."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            lock_file = tmp.name

        lock = blitzortung.lock.LockWithTimeout(lock_file)

        timeouts = [0.1, 1.0, 5.0, 10.0]
        for timeout in timeouts:
            with patch.object(lock, "acquire", return_value=True) as mock_acquire:
                with patch.object(lock, "release"):
                    with lock.locked(timeout=timeout):
                        pass
                    mock_acquire.assert_called_once_with(timeout=timeout)

    def test_locked_context_manager_yield(self):
        """Test that locked context manager yields control."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            lock_file = tmp.name

        lock = blitzortung.lock.LockWithTimeout(lock_file)

        executed = False
        with patch.object(lock, "acquire", return_value=True):
            with patch.object(lock, "release"):
                with lock.locked(timeout=1.0):
                    executed = True

        assert_that(executed).is_true()

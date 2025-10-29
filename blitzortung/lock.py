from contextlib import contextmanager

from fasteners import InterProcessLock


# Taken from https://github.com/harlowja/fasteners/issues/51#issuecomment-755723163
class FailedToAcquireException(Exception):
    pass


class LockWithTimeout(InterProcessLock):

    @contextmanager
    def locked(self, timeout):
        ok = self.acquire(timeout=timeout)
        if not ok:
            raise FailedToAcquireException()
        try:
            yield
        finally:
            self.release()

from .base import (
    FileTransport,
    HttpFileTransport,
    BlitzortungDataPath,
    BlitzortungDataPathGenerator,
)
from .raw_signal import RawSignalsBlitzortungDataProvider
from .station import StationsBlitzortungDataProvider
from .strike import StrikesBlitzortungDataProvider

__all__ = [
    FileTransport,
    HttpFileTransport,
    BlitzortungDataPath,
    BlitzortungDataPathGenerator,
    RawSignalsBlitzortungDataProvider,
    StrikesBlitzortungDataProvider,
    StationsBlitzortungDataProvider,
]


def strikes():
    from .. import INJECTOR

    return INJECTOR.get(StrikesBlitzortungDataProvider)


def stations():
    from .. import INJECTOR

    return INJECTOR.get(StationsBlitzortungDataProvider)


def raw():
    from .. import INJECTOR

    return INJECTOR.get(RawSignalsBlitzortungDataProvider)

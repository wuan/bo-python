from .base import (
    FileTransport,
    HttpFileTransport,
    BlitzortungDataPath,
    BlitzortungDataPathGenerator,
)
from .strike import StrikesBlitzortungDataProvider

__all__ = [
    FileTransport,
    HttpFileTransport,
    BlitzortungDataPath,
    BlitzortungDataPathGenerator,
    StrikesBlitzortungDataProvider,
]


def strikes():
    from .. import INJECTOR

    return INJECTOR.get(StrikesBlitzortungDataProvider)

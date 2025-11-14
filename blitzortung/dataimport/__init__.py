from .base import FileTransport, HttpFileTransport, BlitzortungDataPath, BlitzortungDataPathGenerator
from .strike import StrikesBlitzortungDataProvider


def strikes():
    from .. import INJECTOR

    return INJECTOR.get(StrikesBlitzortungDataProvider)

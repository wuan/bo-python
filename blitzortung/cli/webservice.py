"""Blitzortung webservice entry point for twistd."""

import os

from twisted.application import internet, service
from twisted.internet import reactor
from twisted.internet.error import ReactorAlreadyInstalledError
from twisted.python import log
from twisted.python.log import ILogObserver
from twisted.python.logfile import DailyLogFile
from twisted.web import server

# Install epoll/kqueue reactor for better performance
try:
    reactor.install()
except ReactorAlreadyInstalledError:
    pass

from blitzortung.service.base import Blitzortung, LogObserver
import blitzortung.config

application = service.Application("Blitzortung.org JSON-RPC Server")

log_directory = "/var/log/blitzortung"
try:
    if log_directory and os.path.exists(log_directory):
        logfile = DailyLogFile("webservice.log", log_directory)
        application.setComponent(ILogObserver, LogObserver(logfile).emit)
    else:
        log_directory = None
except OSError as exc:
    log.err(exc, "Failed to initialize webservice file logging; disabling file logging")
    log_directory = None


def start_server(connection_pool):
    """Start the JSON-RPC server with the given connection pool."""
    print("Connection pool is ready")
    config = blitzortung.config.config()
    port = config.get_webservice_port()
    root = Blitzortung(connection_pool, log_directory)
    site = server.Site(root)
    site.displayTracebacks = False
    jsonrpc_server = internet.TCPServer(port, site, interface='127.0.0.1')
    jsonrpc_server.setServiceParent(application)
    return jsonrpc_server


def on_error(failure):
    """Error handler for connection pool failures."""
    log.err(failure, "Failed to create connection pool")
    raise failure.value


from blitzortung.service.db import create_connection_pool
deferred_connection_pool = create_connection_pool()
deferred_connection_pool.addCallback(start_server).addErrback(on_error)

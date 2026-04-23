"""Base service infrastructure for the Blitzortung webservice."""

import os

from twisted.application import internet, service
from twisted.python import log
from twisted.python.log import ILogObserver
from twisted.python.logfile import DailyLogFile
from twisted.web import server

from blitzortung.cli.webservice import Blitzortung, LogObserver
import blitzortung.config


application = service.Application("Blitzortung.org JSON-RPC Server")

log_directory = "/var/log/blitzortung"
if log_directory and os.path.exists(log_directory):
    logfile = DailyLogFile("webservice.log", log_directory)
    application.setComponent(ILogObserver, LogObserver(logfile).emit)
else:
    log_directory = None


def start_server(connection_pool):
    """Start the JSON-RPC server with the given connection pool."""
    print("Connection pool is ready")
    root = Blitzortung(connection_pool, log_directory)
    config = blitzortung.config.config()
    site = server.Site(root)
    site.displayTracebacks = False
    jsonrpc_server = internet.TCPServer(config.get_webservice_port(), site, interface='127.0.0.1')
    jsonrpc_server.setServiceParent(application)
    return jsonrpc_server


def on_error(failure):
    """Error handler for connection pool failures."""
    log.err(failure, "Failed to create connection pool")
    raise failure.value


# Set up connection pool when this module is loaded (skip in test mode)
if not os.environ.get('BLITZORTUNG_TEST'):
    from blitzortung.service.db import create_connection_pool
    deferred_connection_pool = create_connection_pool()
    deferred_connection_pool.addCallback(start_server).addErrback(on_error)

"""Blitzortung webservice entry point for twistd."""

import os

from twisted.application import service
from twisted.python.log import ILogObserver
from twisted.python.logfile import DailyLogFile

# Import classes - these are used by the application
from blitzortung.service.base import Blitzortung, LogObserver  # noqa: F401

application = service.Application("Blitzortung.org JSON-RPC Server")

log_directory = "/var/log/blitzortung"
try:
    if log_directory and os.path.exists(log_directory):
        logfile = DailyLogFile("webservice.log", log_directory)
        application.setComponent(ILogObserver, LogObserver(logfile).emit)
    else:
        log_directory = None
except Exception:
    log_directory = None


# Set up connection pool when running via twistd
if not os.environ.get('BLITZORTUNG_TEST'):
    from twisted.application import internet
    from twisted.python import log
    from twisted.web import server
    import blitzortung.config

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
        jsonrpc_server.startService()
        return jsonrpc_server

    def on_error(failure):
        """Error handler for connection pool failures."""
        log.err(failure, "Failed to create connection pool")
        raise failure.value

    from blitzortung.service.db import create_connection_pool
    deferred_connection_pool = create_connection_pool()
    deferred_connection_pool.addCallback(start_server).addErrback(on_error)

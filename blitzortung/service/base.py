"""Base service infrastructure for the Blitzortung webservice."""

import os

from twisted.application import internet
from twisted.python import log
from twisted.web import server

from blitzortung.cli.webservice import Blitzortung, application, log_directory
import blitzortung.config


def start_server(connection_pool):
    """Start the JSON-RPC server with the given connection pool."""
    print("Connection pool is ready")
    config = blitzortung.config.config()
    port = config.get_webservice_port()
    print(f"Starting server on port {port}")
    root = Blitzortung(connection_pool, log_directory)
    site = server.Site(root)
    site.displayTracebacks = False
    jsonrpc_server = internet.TCPServer(port, site, interface='127.0.0.1')
    print(f"Setting service parent, jsonrpc_server={jsonrpc_server}")
    jsonrpc_server.setServiceParent(application)
    print("Service parent set, returning")
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

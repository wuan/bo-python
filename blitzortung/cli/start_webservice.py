"""Blitzortung webservice entry point."""

import os.path
import sys

from twisted.scripts.twistd import run

# Import webservice to get application variable (twistd expects it in this module)
import blitzortung.cli.webservice  # noqa: F401

# Import service.base to set up connection pool
from blitzortung.service import base  # noqa: F401


def main():
    """Run the twistd server."""
    target_dir = os.path.dirname(os.path.abspath(__file__))

    args = ["twistd"]
    if not os.environ.get("BLITZORTUNG_TEST"):
        args += ["--pidfile", "/var/run/bo-webservice.pid"]

    sys.argv = args + ["-oy", os.path.join(target_dir, "webservice.py")]
    sys.exit(run())


if __name__ == "__main__":
    main()

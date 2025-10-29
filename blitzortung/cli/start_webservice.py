import os.path
import sys

from twisted.scripts.twistd import run


def main():
    target_dir = os.path.dirname(os.path.abspath(__file__))

    pid_file = "/var/run/bo-webservice.pid"

    sys.argv = ["twistd", "--pidfile", pid_file, "-oy", os.path.join(target_dir, "webservice.py")]
    sys.exit(run())


if __name__ == "__main__":
    main()

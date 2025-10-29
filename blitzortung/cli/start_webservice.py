import subprocess
import os.path
from pathlib import Path


def main():
    subprocess.call(["which", "twistd"])
    target_dir = os.path.dirname(os.path.abspath(__file__))

    pid_file = "/var/run/bo-webservice.pid"

    pid_file = pid_file if os.path.exists(pid_file) and os.path.isfile(pid_file) and os.access(pid_file, os.W_OK) else str(Path('~').expanduser() / ".bo-webservice.pid")

    args = ["twistd", "--pidfile", pid_file, "-oy", os.path.join(target_dir, "webservice.py")]
    print(" ".join(args))
    subprocess.call(args)


if __name__ == "__main__":
    main()

import os.path
import subprocess


def main():
    target_dir = os.path.dirname(os.path.abspath(__file__))

    pid_file = "/var/run/bo-webservice.pid"

    args = [os.path.join(target_dir, "twistd"), "--pidfile", pid_file, "-oy", os.path.join(target_dir, "webservice.py")]
    print(" ".join(args))
    subprocess.call(args)


if __name__ == "__main__":
    main()

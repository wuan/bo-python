import subprocess
import os.path


def main():
    target_dir = os.path.dirname(os.path.abspath(__file__))

    args = ["twistd", "--pidfile", "/var/run/bo-webservice.pid", "-oy", os.path.join(target_dir, "webservice.py")]
    print(" ".join(args))
    subprocess.call(args)


if __name__ == "__main__":
    main()

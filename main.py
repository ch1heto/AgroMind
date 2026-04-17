from __future__ import annotations

import subprocess
import sys
import time


def main() -> None:
    worker = None
    dashboard = None

    try:
        worker = subprocess.Popen([sys.executable, "worker.py"])
        time.sleep(2)
        dashboard = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "app.py"])

        worker.wait()
        dashboard.wait()
    except KeyboardInterrupt:
        if worker is not None:
            worker.terminate()
        if dashboard is not None:
            dashboard.terminate()


if __name__ == "__main__":
    main()

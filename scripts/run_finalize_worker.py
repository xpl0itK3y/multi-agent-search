import argparse
import os
import signal
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_shutdown = threading.Event()


def _handle_signal(signum, frame):  # noqa: ARG001
    print(f"job-worker: received signal {signum}, shutting down gracefully…")
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


def run_once() -> int:
    from src.bootstrap import create_research_service
    from src.workers import JobWorker

    worker = JobWorker(
        create_research_service(),
        worker_name=os.environ.get("WORKER_NAME", "job-worker"),
    )
    processed = worker.run_once()
    print(f"job-worker: processed={processed}")
    return processed


def main() -> int:
    parser = argparse.ArgumentParser(description="Run research finalize worker")
    parser.add_argument("--once", action="store_true", help="Process pending jobs once and exit")
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("FINALIZE_WORKER_INTERVAL", "2.0")),
        help="Polling interval in seconds for loop mode",
    )
    args = parser.parse_args()

    if args.once:
        run_once()
        return 0

    while not _shutdown.is_set():
        try:
            run_once()
        except Exception as exc:
            # A transient failure (DB hiccup, Redis blip) must not crash the replica into a
            # Docker restart loop — log, back off, and keep polling.
            print(f"job-worker: run_once failed, backing off then retrying: {exc}", flush=True)
            _shutdown.wait(timeout=min(30.0, max(args.interval * 5, 1.0)))
            continue
        _shutdown.wait(timeout=args.interval)

    print("job-worker: shutdown complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

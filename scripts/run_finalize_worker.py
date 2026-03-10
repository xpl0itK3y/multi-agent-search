import argparse
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_once() -> int:
    from src.bootstrap import create_research_service
    from src.workers import FinalizeWorker, SearchWorker

    research_service = create_research_service()
    search_processed = SearchWorker(research_service).run_once()
    finalize_processed = FinalizeWorker(research_service).run_once()
    processed = search_processed + finalize_processed
    print(
        "job-worker: "
        f"search_processed={search_processed} "
        f"finalize_processed={finalize_processed} "
        f"processed={processed}"
    )
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

    while True:
        run_once()
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())

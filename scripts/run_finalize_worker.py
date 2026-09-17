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


def _create_worker():
    from src.bootstrap import create_research_service
    from src.workers import JobWorker

    return JobWorker(
        create_research_service(),
        worker_name=os.environ.get("WORKER_NAME", "job-worker"),
    )


def _start_metrics_server(port: int):
    """Serve Prometheus /metrics for this worker process on a daemon thread.

    Renders the same prometheus_client registry the worker updates via
    observe_worker_job(), so scrape output is identical in shape to the API's
    /metrics endpoint. The thread is a daemon: it never blocks shutdown.
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from src.observability.metrics import render_metrics

    class _MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.split("?", 1)[0] != "/metrics":
                self.send_error(404)
                return
            payload, content_type = render_metrics()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):  # noqa: A002 - stdlib signature
            return  # keep worker logs free of per-scrape access lines

    server = ThreadingHTTPServer(("0.0.0.0", port), _MetricsHandler)
    threading.Thread(
        target=server.serve_forever,
        name=f"worker-metrics-{port}",
        daemon=True,
    ).start()
    return server


def run_once(worker=None) -> int:
    if worker is None:
        worker = _create_worker()
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
    worker = _create_worker()

    if args.once:
        run_once(worker)
        return 0

    metrics_port = int(os.environ.get("WORKER_METRICS_PORT", "9101") or 0)
    metrics_server = None
    if metrics_port > 0:
        try:
            metrics_server = _start_metrics_server(metrics_port)
            print(f"job-worker: serving /metrics on 0.0.0.0:{metrics_port}", flush=True)
        except OSError as exc:
            # Metrics exposure must never take the worker down (e.g. port already
            # in use when several workers share a host network).
            print(f"job-worker: metrics server not started: {exc}", flush=True)

    while not _shutdown.is_set():
        try:
            run_once(worker)
        except Exception as exc:
            # A transient failure (DB hiccup, Redis blip) must not crash the replica into a
            # Docker restart loop — log, back off, and keep polling.
            print(f"job-worker: run_once failed, backing off then retrying: {exc}", flush=True)
            _shutdown.wait(timeout=min(30.0, max(args.interval * 5, 1.0)))
            continue
        _shutdown.wait(timeout=args.interval)

    if metrics_server is not None:
        metrics_server.shutdown()
    print("job-worker: shutdown complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

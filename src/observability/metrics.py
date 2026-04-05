from __future__ import annotations

from typing import Tuple

from src.config import settings

try:  # pragma: no cover - import behavior depends on optional dependency presence
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
except Exception:  # pragma: no cover
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    Counter = Gauge = Histogram = None
    generate_latest = None


if Counter is not None and settings.prometheus_metrics_enabled:
    API_REQUESTS_TOTAL = Counter(
        "mas_api_requests_total",
        "Total API requests.",
        ["method", "path", "status"],
    )
    API_REQUEST_DURATION_SECONDS = Histogram(
        "mas_api_request_duration_seconds",
        "API request duration in seconds.",
        ["method", "path"],
    )
    WORKER_JOBS_TOTAL = Counter(
        "mas_worker_jobs_total",
        "Worker jobs processed.",
        ["worker_name", "job_type", "status"],
    )
    QUEUE_BACKLOG = Gauge(
        "mas_queue_backlog",
        "Current total queue backlog.",
    )
    QUEUE_JOBS = Gauge(
        "mas_queue_jobs",
        "Current queue counts by kind and status.",
        ["job_type", "status"],
    )
else:  # pragma: no cover
    API_REQUESTS_TOTAL = None
    API_REQUEST_DURATION_SECONDS = None
    WORKER_JOBS_TOTAL = None
    QUEUE_BACKLOG = None
    QUEUE_JOBS = None


def observe_api_request(method: str, path: str, status_code: int, elapsed_seconds: float) -> None:
    if API_REQUESTS_TOTAL is None or API_REQUEST_DURATION_SECONDS is None:
        return
    normalized_method = (method or "GET").upper()
    normalized_path = path or "/"
    API_REQUESTS_TOTAL.labels(
        method=normalized_method,
        path=normalized_path,
        status=str(status_code),
    ).inc()
    API_REQUEST_DURATION_SECONDS.labels(
        method=normalized_method,
        path=normalized_path,
    ).observe(max(elapsed_seconds, 0.0))


def observe_worker_job(worker_name: str, job_type: str, status: str, count: int = 1) -> None:
    if WORKER_JOBS_TOTAL is None or count <= 0:
        return
    WORKER_JOBS_TOTAL.labels(
        worker_name=worker_name or "-",
        job_type=job_type or "unknown",
        status=status or "unknown",
    ).inc(count)


def set_queue_metrics(metrics) -> None:
    if QUEUE_BACKLOG is None or QUEUE_JOBS is None:
        return
    backlog = (
        int(metrics.pending_search_jobs or 0)
        + int(metrics.running_search_jobs or 0)
        + int(metrics.dead_letter_search_jobs or 0)
        + int(metrics.pending_finalize_jobs or 0)
        + int(metrics.running_finalize_jobs or 0)
        + int(metrics.dead_letter_finalize_jobs or 0)
    )
    QUEUE_BACKLOG.set(backlog)
    QUEUE_JOBS.labels(job_type="search", status="pending").set(int(metrics.pending_search_jobs or 0))
    QUEUE_JOBS.labels(job_type="search", status="running").set(int(metrics.running_search_jobs or 0))
    QUEUE_JOBS.labels(job_type="search", status="dead_letter").set(int(metrics.dead_letter_search_jobs or 0))
    QUEUE_JOBS.labels(job_type="finalize", status="pending").set(int(metrics.pending_finalize_jobs or 0))
    QUEUE_JOBS.labels(job_type="finalize", status="running").set(int(metrics.running_finalize_jobs or 0))
    QUEUE_JOBS.labels(job_type="finalize", status="dead_letter").set(int(metrics.dead_letter_finalize_jobs or 0))


def render_metrics() -> Tuple[bytes, str]:
    if generate_latest is None or not settings.prometheus_metrics_enabled:
        return b"", CONTENT_TYPE_LATEST
    return generate_latest(), CONTENT_TYPE_LATEST

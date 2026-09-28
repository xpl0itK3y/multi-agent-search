"""Framework-agnostic service errors (ARCH-004).

The service layer raises these instead of fastapi.HTTPException so that src/services and
src/domain carry no web-framework dependency. The API layer registers a single exception
handler that translates them to HTTP responses (status_code + {"detail": ...}), preserving
the exact responses clients saw before.
"""
from __future__ import annotations


class ServiceError(Exception):
    """Base service error carrying an HTTP-translatable status code + detail message."""

    status_code: int = 500

    def __init__(self, detail: str = "Internal error") -> None:
        self.detail = detail
        super().__init__(detail)


class BadRequestError(ServiceError):
    status_code = 400


class UnauthorizedError(ServiceError):
    status_code = 401


class ForbiddenError(ServiceError):
    status_code = 403


class NotFoundError(ServiceError):
    status_code = 404


class ConflictError(ServiceError):
    status_code = 409


class UnprocessableError(ServiceError):
    status_code = 422


class ServiceUnavailableError(ServiceError):
    status_code = 503


class SearchJobLeaseLost(RuntimeError):
    """A search runner's lease on its job was taken away: stale recovery or a requeue
    bumped the job's lease epoch (or ended the job), so this runner's writes are refused.
    Internal to the worker path, not an HTTP error: the runner stops without settling the
    job, since whoever holds it now will."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Search job {job_id} lease was lost")

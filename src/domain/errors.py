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


class NotFoundError(ServiceError):
    status_code = 404


class ConflictError(ServiceError):
    status_code = 409


class UnprocessableError(ServiceError):
    status_code = 422


class ServiceUnavailableError(ServiceError):
    status_code = 503

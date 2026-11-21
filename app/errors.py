from __future__ import annotations

from typing import Any


class DrillError(Exception):
    """Domain error carrying a stable code and an HTTP status."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail or {}

    def payload(self) -> dict[str, Any]:
        body = {"code": self.code, "message": self.message}
        if self.detail:
            body["detail"] = self.detail
        return body


class NotFound(DrillError):
    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__(404, "not_found", message, detail=detail or None)


class Conflict(DrillError):
    def __init__(self, code: str, message: str, **detail: Any) -> None:
        super().__init__(409, code, message, detail=detail or None)


class ValidationFailure(DrillError):
    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__(422, "validation_failed", message, detail=detail or None)


class CleanupFailed(DrillError):
    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__(500, "cleanup_failed", message, detail=detail or None)

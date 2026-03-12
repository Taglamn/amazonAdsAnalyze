from __future__ import annotations

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from .dependencies import parse_auth_context_from_header


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Attach decoded JWT claims to request state when Authorization header exists."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        authorization = request.headers.get("Authorization")
        request.state.auth_context = None

        if authorization:
            try:
                request.state.auth_context = parse_auth_context_from_header(authorization)
            except Exception:  # noqa: BLE001
                request.state.auth_context = None

        return await call_next(request)

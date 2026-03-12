from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
from jose import JWTError, jwt

from .config import get_auth_settings


class AuthError(RuntimeError):
    """Raised when token or credential checks fail."""


def hash_password(password: str) -> str:
    """Hash plain password with bcrypt."""

    settings = get_auth_settings()
    salt = bcrypt.gensalt(rounds=settings.bcrypt_rounds)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Validate plain password against bcrypt hash."""

    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        return False


def create_access_token(*, user_id: int, tenant_id: int, role: str, email: str) -> str:
    """Create JWT access token with user and tenant claims."""

    settings = get_auth_settings()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=settings.jwt_expire_minutes)
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "tenant_id": tenant_id,
        "role": role,
        "email": email,
        "typ": "access",
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def create_refresh_token(*, user_id: int, tenant_id: int, role: str, email: str) -> str:
    """Create JWT refresh token with longer expiry window."""

    settings = get_auth_settings()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=settings.jwt_refresh_expire_days)
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "tenant_id": tenant_id,
        "role": role,
        "email": email,
        "typ": "refresh",
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def _decode_jwt(token: str) -> dict[str, Any]:
    """Decode and validate JWT token signature/time window."""

    settings = get_auth_settings()
    try:
        return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        raise AuthError("Invalid or expired token") from exc


def decode_access_token(token: str) -> dict[str, Any]:
    """Decode and validate JWT access token."""

    payload = _decode_jwt(token)
    token_type = str(payload.get("typ") or "access").strip().lower()
    if token_type != "access":
        raise AuthError("Invalid token type")
    return payload


def decode_refresh_token(token: str) -> dict[str, Any]:
    """Decode and validate JWT refresh token."""

    payload = _decode_jwt(token)
    token_type = str(payload.get("typ") or "").strip().lower()
    if token_type != "refresh":
        raise AuthError("Invalid token type")
    return payload

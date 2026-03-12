from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from .crud import user_has_store_access
from .database import get_db_session
from .models import RoleName, User, UserStatus
from .security import AuthError, decode_access_token


@dataclass(frozen=True)
class AuthContext:
    """Token-derived identity claims used by authorization dependencies."""

    user_id: int
    tenant_id: int
    role: str
    email: str


def _extract_bearer_token(authorization: str | None) -> str:
    """Extract bearer token from Authorization header."""

    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Authorization header")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Authorization header")
    return token.strip()


def parse_auth_context_from_header(authorization: str | None) -> AuthContext:
    """Decode JWT from Authorization header into auth context."""

    token = _extract_bearer_token(authorization)
    try:
        payload = decode_access_token(token)
        return AuthContext(
            user_id=int(payload["sub"]),
            tenant_id=int(payload["tenant_id"]),
            role=str(payload.get("role") or ""),
            email=str(payload.get("email") or ""),
        )
    except (KeyError, ValueError, AuthError) as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc


def get_current_user(
    request: Request,
    db: Session = Depends(get_db_session),
    authorization: str | None = Header(default=None),
) -> User:
    """Resolve current authenticated user from request state or bearer token."""

    state_context = getattr(request.state, "auth_context", None)
    if isinstance(state_context, AuthContext):
        context = state_context
    else:
        context = parse_auth_context_from_header(authorization)

    user = db.get(User, context.user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    if user.tenant_id != context.tenant_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tenant mismatch")
    if user.status != UserStatus.ACTIVE.value:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User is inactive")
    return user


def require_roles(*allowed_roles: RoleName | str) -> Callable[[User], User]:
    """Build dependency that authorizes user role against allowed role names."""

    normalized = {str(role.value if isinstance(role, RoleName) else role) for role in allowed_roles}

    def _dep(current_user: User = Depends(get_current_user)) -> User:
        role_name = current_user.role.name if current_user.role else ""
        if role_name not in normalized:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role permissions")
        return current_user

    return _dep


def enforce_store_access(db: Session, *, current_user: User, external_store_id: str) -> None:
    """Validate current user can access target store id."""

    if not user_has_store_access(db, user=current_user, external_store_id=external_store_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"No access to store {external_store_id}",
        )

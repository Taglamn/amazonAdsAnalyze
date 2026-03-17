from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from .crud import (
    list_user_scoped_stores,
    CRUDValidationError,
    add_store_access,
    assign_role,
    authenticate_user,
    create_user,
    ensure_actor_can_manage_user,
    ensure_user_same_tenant,
    get_user_by_id,
    list_tenant_stores,
    list_accessible_stores,
    list_users,
    remove_store_access,
    reset_password,
    set_user_status,
    sync_user_store_access,
    user_has_store_access,
)
from .config import get_auth_settings
from .database import get_db_session
from .dependencies import get_current_user, require_roles
from .models import RoleName, User, UserStatus
from .schemas import (
    AccessibleStoresResponse,
    AdminCreateUserRequest,
    AssignRoleRequest,
    BulkSetStoreAccessRequest,
    MeResponse,
    PasswordResetRequest,
    PermissionCheckResponse,
    RefreshTokenRequest,
    TenantStoresResponse,
    SetStoreAccessRequest,
    TokenResponse,
    UserListResponse,
    UserLoginRequest,
    UserOut,
    UserRegisterRequest,
    UserStoreAccessListResponse,
    UserStatusRequest,
)
from .security import AuthError, create_access_token, create_refresh_token, decode_refresh_token

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _build_default_email_from_username(username: str) -> str:
    """Generate a deterministic placeholder email for username-only onboarding."""

    normalized = username.strip().lower()
    return f"{normalized}@local.invalid"


def _to_user_out(user: User) -> UserOut:
    """Convert ORM user to response payload with role name."""

    return UserOut(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        tenant_id=user.tenant_id,
        role_id=user.role_id,
        role=user.role.name if user.role else "",
        status=user.status,
        created_at=user.created_at,
        last_login=user.last_login,
    )


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register_user(payload: UserRegisterRequest, db: Session = Depends(get_db_session)) -> UserOut:
    """Register user with unique email and bcrypt-hashed password."""

    try:
        created = create_user(
            db,
            username=(payload.username or str(payload.email).split("@", 1)[0]),
            email=payload.email,
            password=payload.password,
            role_name=payload.role.value,
            tenant_id=payload.tenant_id,
        )
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    created = db.get(User, created.user_id)
    if created is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load created user")
    return _to_user_out(created)


@router.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user_by_admin(
    payload: AdminCreateUserRequest,
    current_user: User = Depends(require_roles(RoleName.ADMIN)),
    db: Session = Depends(get_db_session),
) -> UserOut:
    """Create user account under current admin's tenant."""

    try:
        created = create_user(
            db,
            username=payload.username,
            email=_build_default_email_from_username(payload.username),
            password=payload.password,
            role_name=payload.role.value,
            tenant_id=current_user.tenant_id,
            store_external_ids=payload.store_ids,
        )
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    created = db.get(User, created.user_id)
    if created is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load created user")
    return _to_user_out(created)


@router.post("/login", response_model=TokenResponse)
def login_user(payload: UserLoginRequest, db: Session = Depends(get_db_session)) -> TokenResponse:
    """Authenticate user and issue JWT token."""

    login_account = (payload.account or "").strip() or str(payload.email or "").strip()
    user = authenticate_user(db, account=login_account, password=payload.password)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid account or password")

    access_token = create_access_token(
        user_id=user.user_id,
        tenant_id=user.tenant_id,
        role=user.role.name if user.role else "",
        email=user.email,
    )
    refresh_token = create_refresh_token(
        user_id=user.user_id,
        tenant_id=user.tenant_id,
        role=user.role.name if user.role else "",
        email=user.email,
    )
    settings = get_auth_settings()
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=settings.jwt_expire_minutes * 60,
        refresh_expires_in=settings.jwt_refresh_expire_days * 24 * 3600,
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh_access_token(payload: RefreshTokenRequest, db: Session = Depends(get_db_session)) -> TokenResponse:
    """Refresh access token using a valid refresh token."""

    try:
        claims = decode_refresh_token(payload.refresh_token.strip())
        user_id = int(claims["sub"])
        tenant_id = int(claims["tenant_id"])
    except (KeyError, ValueError, AuthError) as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token") from exc

    user = db.get(User, user_id)
    if user is None or user.tenant_id != tenant_id or user.status != UserStatus.ACTIVE.value:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token is no longer valid")

    access_token = create_access_token(
        user_id=user.user_id,
        tenant_id=user.tenant_id,
        role=user.role.name if user.role else "",
        email=user.email,
    )
    refresh_token = create_refresh_token(
        user_id=user.user_id,
        tenant_id=user.tenant_id,
        role=user.role.name if user.role else "",
        email=user.email,
    )
    settings = get_auth_settings()
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=settings.jwt_expire_minutes * 60,
        refresh_expires_in=settings.jwt_refresh_expire_days * 24 * 3600,
    )


@router.get("/me", response_model=MeResponse)
def get_me(current_user: User = Depends(get_current_user)) -> MeResponse:
    """Return current authenticated user profile."""

    return MeResponse(
        user_id=current_user.user_id,
        username=current_user.username,
        email=current_user.email,
        tenant_id=current_user.tenant_id,
        role=current_user.role.name if current_user.role else "",
        status=current_user.status,
        created_at=current_user.created_at,
        last_login=current_user.last_login,
    )


@router.get("/users", response_model=UserListResponse)
def get_users(
    tenant_id: int | None = Query(default=None),
    role: str | None = Query(default=None),
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> UserListResponse:
    """List users filtered by tenant_id and/or role inside actor's tenant scope."""

    effective_tenant_id = tenant_id
    if current_user.role and current_user.role.name != RoleName.ADMIN.value:
        effective_tenant_id = current_user.tenant_id
    elif effective_tenant_id is None and current_user.username != get_auth_settings().bootstrap_admin_username:
        effective_tenant_id = current_user.tenant_id

    users = list_users(db, tenant_id=effective_tenant_id, role_name=role)
    return UserListResponse(items=[_to_user_out(item) for item in users])


@router.post("/users/{user_id}/role", response_model=UserOut)
def assign_user_role(
    user_id: int,
    payload: AssignRoleRequest,
    current_user: User = Depends(require_roles(RoleName.ADMIN)),
    db: Session = Depends(get_db_session),
) -> UserOut:
    """Assign role to user, restricted to tenant admin."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    try:
        ensure_actor_can_manage_user(current_user, target)
        updated = assign_role(db, user_id=user_id, role_name=payload.role.value)
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    updated = db.get(User, updated.user_id)
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")
    return _to_user_out(updated)


@router.post("/users/{user_id}/stores", response_model=PermissionCheckResponse)
def add_user_store_access(
    user_id: int,
    payload: SetStoreAccessRequest,
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> PermissionCheckResponse:
    """Grant store access to a user."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    try:
        ensure_actor_can_manage_user(current_user, target)
        add_store_access(
            db,
            user_id=user_id,
            external_store_id=payload.external_store_id.strip(),
            store_name=payload.store_name,
        )
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    has_access = user_has_store_access(db, user=target, external_store_id=payload.external_store_id.strip())
    return PermissionCheckResponse(user_id=user_id, external_store_id=payload.external_store_id.strip(), has_access=has_access)


@router.delete("/users/{user_id}/stores/{external_store_id}", response_model=PermissionCheckResponse)
def delete_user_store_access(
    user_id: int,
    external_store_id: str,
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> PermissionCheckResponse:
    """Revoke store access from a user."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    try:
        ensure_actor_can_manage_user(current_user, target)
        remove_store_access(db, user_id=user_id, external_store_id=external_store_id.strip())
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    has_access = user_has_store_access(db, user=target, external_store_id=external_store_id.strip())
    return PermissionCheckResponse(user_id=user_id, external_store_id=external_store_id.strip(), has_access=has_access)


@router.get("/users/{user_id}/stores/{external_store_id}/access", response_model=PermissionCheckResponse)
def check_store_access_permission(
    user_id: int,
    external_store_id: str,
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> PermissionCheckResponse:
    """Check whether target user has access to a store."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    try:
        ensure_actor_can_manage_user(current_user, target)
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    has_access = user_has_store_access(db, user=target, external_store_id=external_store_id.strip())
    return PermissionCheckResponse(user_id=user_id, external_store_id=external_store_id.strip(), has_access=has_access)


@router.get("/users/{user_id}/stores", response_model=UserStoreAccessListResponse)
def get_user_store_access_list(
    user_id: int,
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> UserStoreAccessListResponse:
    """List store permissions for a target user."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")
    try:
        ensure_actor_can_manage_user(current_user, target)
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UserStoreAccessListResponse(
        user_id=user_id,
        stores=list_user_scoped_stores(db, user_id=user_id),
    )


@router.put("/users/{user_id}/stores", response_model=UserStoreAccessListResponse)
def bulk_set_user_store_access(
    user_id: int,
    payload: BulkSetStoreAccessRequest,
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> UserStoreAccessListResponse:
    """Bulk set user store permissions."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")
    try:
        ensure_actor_can_manage_user(current_user, target)
        stores = sync_user_store_access(
            db,
            user_id=user_id,
            external_store_ids=payload.store_ids,
            replace_existing=payload.replace_existing,
        )
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UserStoreAccessListResponse(user_id=user_id, stores=stores)


@router.post("/users/{user_id}/password", response_model=UserOut)
def reset_user_password(
    user_id: int,
    payload: PasswordResetRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> UserOut:
    """Reset password; users can reset themselves and admins can reset tenant users."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    is_self = current_user.user_id == target.user_id
    is_admin = current_user.role and current_user.role.name == RoleName.ADMIN.value
    if not is_self and not is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not allowed to reset this password")

    try:
        ensure_user_same_tenant(current_user, target)
        updated = reset_password(db, user_id=user_id, new_password=payload.new_password)
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    updated = db.get(User, updated.user_id)
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")
    return _to_user_out(updated)


@router.post("/users/{user_id}/status", response_model=UserOut)
def update_user_status(
    user_id: int,
    payload: UserStatusRequest,
    current_user: User = Depends(require_roles(RoleName.ADMIN)),
    db: Session = Depends(get_db_session),
) -> UserOut:
    """Activate/deactivate a user account."""

    target = get_user_by_id(db, user_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    try:
        ensure_user_same_tenant(current_user, target)
        updated = set_user_status(db, user_id=user_id, active=(payload.status.value == "active"))
    except CRUDValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    updated = db.get(User, updated.user_id)
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")
    return _to_user_out(updated)


@router.get("/stores/me", response_model=AccessibleStoresResponse)
def get_my_accessible_stores(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> AccessibleStoresResponse:
    """List stores visible to current user under RBAC and store mapping."""

    stores = list_accessible_stores(db, user=current_user)
    return AccessibleStoresResponse(stores=stores)


@router.get("/stores", response_model=TenantStoresResponse)
def get_tenant_stores_for_permission(
    current_user: User = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER)),
    db: Session = Depends(get_db_session),
) -> TenantStoresResponse:
    """List all tenant stores for user-permission management UI."""

    stores = list_tenant_stores(db, tenant_id=current_user.tenant_id)
    return TenantStoresResponse(stores=stores)

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import Select, and_, select
from sqlalchemy.orm import Session

from .models import Role, RoleName, Store, Tenant, User, UserStatus, UserStoreMapping
from .security import hash_password, verify_password


class CRUDValidationError(RuntimeError):
    """Raised when a business constraint fails."""


def _normalize_email(email: str) -> str:
    """Normalize email for uniqueness checks and login lookup."""

    return email.strip().lower()


def _normalize_username(username: str) -> str:
    """Normalize username for uniqueness checks and login lookup."""

    return username.strip().lower()


def ensure_default_roles(db: Session) -> None:
    """Seed RBAC role catalog if roles are missing."""

    role_descriptions = {
        RoleName.ADMIN.value: "Full tenant administration",
        RoleName.MANAGER.value: "Operational manager",
        RoleName.STAFF.value: "Customer service and operational execution",
        RoleName.VIEWER.value: "Read-only data access",
    }

    existing = {
        row[0]
        for row in db.execute(select(Role.name)).all()
    }
    for role_name, description in role_descriptions.items():
        if role_name in existing:
            continue
        db.add(Role(name=role_name, description=description))
    db.commit()


def ensure_default_tenant(db: Session, tenant_name: str) -> Tenant:
    """Ensure there is at least one tenant available for onboarding users."""

    stmt = select(Tenant).where(Tenant.name == tenant_name)
    tenant = db.execute(stmt).scalar_one_or_none()
    if tenant is not None:
        return tenant

    tenant = Tenant(name=tenant_name, status=UserStatus.ACTIVE.value)
    db.add(tenant)
    db.commit()
    db.refresh(tenant)
    return tenant


def get_role_by_name(db: Session, role_name: str) -> Role:
    """Load a role by role name and validate it exists."""

    stmt = select(Role).where(Role.name == role_name)
    role = db.execute(stmt).scalar_one_or_none()
    if role is None:
        raise CRUDValidationError(f"Role {role_name} does not exist")
    return role


def get_user_by_email(db: Session, email: str) -> User | None:
    """Get user by unique email."""

    stmt = select(User).where(User.email == _normalize_email(email))
    return db.execute(stmt).scalar_one_or_none()


def get_user_by_username(db: Session, username: str) -> User | None:
    """Get user by unique username."""

    stmt = select(User).where(User.username == _normalize_username(username))
    return db.execute(stmt).scalar_one_or_none()


def get_user_by_id(db: Session, user_id: int) -> User | None:
    """Get user by primary key."""

    return db.get(User, user_id)


def create_user(
    db: Session,
    *,
    username: str,
    email: str,
    password: str,
    lingxing_erp_username: str | None = None,
    lingxing_erp_password: str | None = None,
    role_name: str,
    tenant_id: int,
    status: str = UserStatus.ACTIVE.value,
) -> User:
    """Create a new user with bcrypt password hash and role assignment."""

    normalized_username = _normalize_username(username)
    if not normalized_username:
        raise CRUDValidationError("Username cannot be empty")
    if get_user_by_username(db, normalized_username) is not None:
        raise CRUDValidationError("Username already exists")

    normalized_email = _normalize_email(email)
    if get_user_by_email(db, normalized_email) is not None:
        raise CRUDValidationError("Email already exists")
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise CRUDValidationError(f"Tenant {tenant_id} not found")

    role = get_role_by_name(db, role_name)
    user = User(
        tenant_id=tenant_id,
        username=normalized_username,
        email=normalized_email,
        password_hash=hash_password(password),
        lingxing_erp_username=(lingxing_erp_username or "").strip() or None,
        lingxing_erp_password=(lingxing_erp_password or "").strip() or None,
        role_id=role.role_id,
        status=status,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def authenticate_user(db: Session, *, account: str, password: str) -> User | None:
    """Verify login credentials and return user when valid and active."""

    account_normalized = account.strip().lower()
    user = get_user_by_email(db, account_normalized)
    if user is None:
        user = get_user_by_username(db, account_normalized)
    if user is None:
        return None
    if user.status != UserStatus.ACTIVE.value:
        return None
    if not verify_password(password, user.password_hash):
        return None

    user.last_login = datetime.now(timezone.utc)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def list_users(
    db: Session,
    *,
    tenant_id: int | None = None,
    role_name: str | None = None,
) -> list[User]:
    """List users with optional tenant and role filters."""

    stmt: Select[tuple[User]] = select(User)
    if tenant_id is not None:
        stmt = stmt.where(User.tenant_id == tenant_id)
    if role_name:
        stmt = stmt.join(Role, Role.role_id == User.role_id).where(Role.name == role_name)
    stmt = stmt.order_by(User.created_at.desc(), User.user_id.desc())
    return list(db.execute(stmt).scalars().all())


def assign_role(db: Session, *, user_id: int, role_name: str) -> User:
    """Assign a new RBAC role to a user."""

    user = get_user_by_id(db, user_id)
    if user is None:
        raise CRUDValidationError(f"User {user_id} not found")

    role = get_role_by_name(db, role_name)
    user.role_id = role.role_id
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def set_user_status(db: Session, *, user_id: int, active: bool) -> User:
    """Activate or deactivate a user account."""

    user = get_user_by_id(db, user_id)
    if user is None:
        raise CRUDValidationError(f"User {user_id} not found")

    user.status = UserStatus.ACTIVE.value if active else UserStatus.INACTIVE.value
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def reset_password(
    db: Session,
    *,
    user_id: int,
    new_password: str | None = None,
    lingxing_erp_username: str | None = None,
    lingxing_erp_password: str | None = None,
) -> User:
    """Reset password and/or Lingxing ERP credentials for a user."""

    user = get_user_by_id(db, user_id)
    if user is None:
        raise CRUDValidationError(f"User {user_id} not found")

    if not new_password and lingxing_erp_username is None and lingxing_erp_password is None:
        raise CRUDValidationError("At least one field must be provided")

    if new_password:
        user.password_hash = hash_password(new_password)
    if lingxing_erp_username is not None:
        normalized = lingxing_erp_username.strip()
        user.lingxing_erp_username = normalized or None
    if lingxing_erp_password is not None:
        normalized = lingxing_erp_password.strip()
        user.lingxing_erp_password = normalized or None

    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def get_or_create_store(
    db: Session,
    *,
    tenant_id: int,
    external_store_id: str,
    store_name: str,
) -> Store:
    """Create store metadata on demand to support authorization mapping."""

    normalized_store_id = external_store_id.strip()
    if not normalized_store_id:
        raise CRUDValidationError("external_store_id cannot be empty")

    stmt = select(Store).where(
        and_(
            Store.tenant_id == tenant_id,
            Store.external_store_id == normalized_store_id,
        )
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing is not None:
        if store_name and existing.store_name != store_name:
            existing.store_name = store_name
            db.add(existing)
            db.commit()
            db.refresh(existing)
        return existing

    created = Store(
        tenant_id=tenant_id,
        external_store_id=normalized_store_id,
        store_name=(store_name.strip() or normalized_store_id),
        status=UserStatus.ACTIVE.value,
    )
    db.add(created)
    db.commit()
    db.refresh(created)
    return created


def get_store_by_external_id(db: Session, *, tenant_id: int, external_store_id: str) -> Store | None:
    """Look up store metadata by tenant and external store identifier."""

    stmt = select(Store).where(
        and_(
            Store.tenant_id == tenant_id,
            Store.external_store_id == external_store_id,
        )
    )
    return db.execute(stmt).scalar_one_or_none()


def add_store_access(db: Session, *, user_id: int, external_store_id: str, store_name: str = "") -> UserStoreMapping:
    """Grant user access to a store within the same tenant."""

    user = get_user_by_id(db, user_id)
    if user is None:
        raise CRUDValidationError(f"User {user_id} not found")

    store = get_or_create_store(
        db,
        tenant_id=user.tenant_id,
        external_store_id=external_store_id,
        store_name=store_name or external_store_id,
    )

    stmt = select(UserStoreMapping).where(
        and_(
            UserStoreMapping.user_id == user.user_id,
            UserStoreMapping.store_id == store.store_id,
        )
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing is not None:
        return existing

    mapping = UserStoreMapping(
        user_id=user.user_id,
        store_id=store.store_id,
        tenant_id=user.tenant_id,
    )
    db.add(mapping)
    db.commit()
    db.refresh(mapping)
    return mapping


def remove_store_access(db: Session, *, user_id: int, external_store_id: str) -> bool:
    """Revoke user access to a specific store."""

    user = get_user_by_id(db, user_id)
    if user is None:
        raise CRUDValidationError(f"User {user_id} not found")

    store = get_store_by_external_id(db, tenant_id=user.tenant_id, external_store_id=external_store_id)
    if store is None:
        return False

    stmt = select(UserStoreMapping).where(
        and_(
            UserStoreMapping.user_id == user.user_id,
            UserStoreMapping.store_id == store.store_id,
        )
    )
    mapping = db.execute(stmt).scalar_one_or_none()
    if mapping is None:
        return False

    db.delete(mapping)
    db.commit()
    return True


def user_has_store_access(db: Session, *, user: User, external_store_id: str) -> bool:
    """Check whether user can access the given external store id."""

    if user.status != UserStatus.ACTIVE.value:
        return False

    role_name = user.role.name if user.role else ""
    if role_name == RoleName.ADMIN.value:
        return True

    stmt = (
        select(UserStoreMapping.id)
        .join(Store, Store.store_id == UserStoreMapping.store_id)
        .where(
            and_(
                UserStoreMapping.user_id == user.user_id,
                UserStoreMapping.tenant_id == user.tenant_id,
                Store.external_store_id == external_store_id,
            )
        )
    )
    return db.execute(stmt).scalar_one_or_none() is not None


def list_accessible_stores(db: Session, *, user: User) -> list[Store]:
    """Return stores visible to current user under tenant scope."""

    role_name = user.role.name if user.role else ""
    if role_name == RoleName.ADMIN.value:
        stmt = (
            select(Store)
            .where(Store.tenant_id == user.tenant_id)
            .order_by(Store.store_name.asc(), Store.store_id.asc())
        )
        return list(db.execute(stmt).scalars().all())

    stmt = (
        select(Store)
        .join(UserStoreMapping, UserStoreMapping.store_id == Store.store_id)
        .where(
            and_(
                UserStoreMapping.user_id == user.user_id,
                UserStoreMapping.tenant_id == user.tenant_id,
            )
        )
        .order_by(Store.store_name.asc(), Store.store_id.asc())
    )
    return list(db.execute(stmt).scalars().all())


def list_user_scoped_stores(db: Session, *, user_id: int) -> list[Store]:
    """List stores explicitly granted to a target user."""

    stmt = (
        select(Store)
        .join(UserStoreMapping, UserStoreMapping.store_id == Store.store_id)
        .where(UserStoreMapping.user_id == user_id)
        .order_by(Store.store_name.asc(), Store.store_id.asc())
    )
    return list(db.execute(stmt).scalars().all())


def ensure_user_same_tenant(actor: User, target: User) -> None:
    """Enforce that user-management actions stay inside actor's tenant."""

    if actor.tenant_id != target.tenant_id:
        raise CRUDValidationError("Cross-tenant operation is not allowed")


def bulk_sync_stores(
    db: Session,
    *,
    tenant_id: int,
    stores: Iterable[tuple[str, str]],
) -> None:
    """Ensure store catalog rows exist for all known external stores."""

    for external_store_id, store_name in stores:
        get_or_create_store(
            db,
            tenant_id=tenant_id,
            external_store_id=external_store_id,
            store_name=store_name,
        )

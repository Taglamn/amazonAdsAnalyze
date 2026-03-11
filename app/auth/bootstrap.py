from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from .config import get_auth_settings
from .crud import (
    create_user,
    ensure_default_roles,
    ensure_default_tenant,
    get_user_by_email,
    get_user_by_username,
)
from .database import Base, SessionLocal, engine
from .models import Role, RoleName, UserStatus
from .security import hash_password


def _ensure_users_username_column() -> None:
    """Backfill users.username for environments created before username support."""

    inspector = inspect(engine)
    if "users" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("users")}
    add_sql: list[str] = []

    if "username" not in existing_columns:
        add_sql.append("ALTER TABLE users ADD COLUMN username VARCHAR(64)")
        add_sql.append(
            "UPDATE users "
            "SET username = LOWER(COALESCE(NULLIF(SPLIT_PART(email, '@', 1), ''), 'user_' || user_id::text)) "
            "WHERE username IS NULL OR username = ''"
        )
        add_sql.append("ALTER TABLE users ALTER COLUMN username SET NOT NULL")
        add_sql.append("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username ON users (username)")

    if not add_sql:
        return

    with engine.begin() as conn:
        for sql in add_sql:
            conn.execute(text(sql))


def _ensure_users_lingxing_columns() -> None:
    """Backfill users Lingxing credential columns for older environments."""

    inspector = inspect(engine)
    if "users" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("users")}
    add_sql: list[str] = []

    if "lingxing_erp_username" not in existing_columns:
        add_sql.append("ALTER TABLE users ADD COLUMN lingxing_erp_username VARCHAR(255)")
    if "lingxing_erp_password" not in existing_columns:
        add_sql.append("ALTER TABLE users ADD COLUMN lingxing_erp_password VARCHAR(255)")

    if not add_sql:
        return

    with engine.begin() as conn:
        for sql in add_sql:
            conn.execute(text(sql))


def init_auth_schema() -> None:
    """Create auth/RBAC tables and seed default roles and admin account."""

    Base.metadata.create_all(bind=engine)
    _ensure_users_username_column()
    _ensure_users_lingxing_columns()
    settings = get_auth_settings()

    db: Session = SessionLocal()
    try:
        ensure_default_roles(db)
        tenant = ensure_default_tenant(db, settings.bootstrap_tenant_name)

        existing_admin = get_user_by_username(db, settings.bootstrap_admin_username)
        if existing_admin is None:
            existing_admin = get_user_by_email(db, settings.bootstrap_admin_email)

        if existing_admin is None:
            create_user(
                db,
                username=settings.bootstrap_admin_username,
                email=settings.bootstrap_admin_email,
                password=settings.bootstrap_admin_password,
                role_name=RoleName.ADMIN.value,
                tenant_id=tenant.tenant_id,
            )
        else:
            admin_role = db.query(Role).filter(Role.name == RoleName.ADMIN.value).one()
            existing_admin.username = settings.bootstrap_admin_username
            existing_admin.email = settings.bootstrap_admin_email
            existing_admin.password_hash = hash_password(settings.bootstrap_admin_password)
            existing_admin.status = UserStatus.ACTIVE.value
            existing_admin.tenant_id = tenant.tenant_id
            existing_admin.role_id = admin_role.role_id
            db.add(existing_admin)
            db.commit()
    finally:
        db.close()

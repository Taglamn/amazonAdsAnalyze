from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


@dataclass(frozen=True)
class AuthSettings:
    """Configuration for authentication, authorization, and shared database access."""

    database_url: str
    jwt_secret_key: str
    jwt_algorithm: str
    jwt_expire_minutes: int
    bcrypt_rounds: int
    bootstrap_admin_username: str
    bootstrap_admin_email: str
    bootstrap_admin_password: str
    bootstrap_tenant_name: str


@lru_cache(maxsize=1)
def get_auth_settings() -> AuthSettings:
    """Load auth settings from environment variables."""

    if load_dotenv is not None:
        load_dotenv()

    jwt_expire_minutes_raw = os.getenv("JWT_EXPIRE_MINUTES", "120").strip()
    bcrypt_rounds_raw = os.getenv("BCRYPT_ROUNDS", "12").strip()

    try:
        jwt_expire_minutes = max(5, int(jwt_expire_minutes_raw))
    except ValueError:
        jwt_expire_minutes = 120

    try:
        bcrypt_rounds = min(16, max(10, int(bcrypt_rounds_raw)))
    except ValueError:
        bcrypt_rounds = 12

    database_url = (
        os.getenv("DATABASE_URL", "").strip()
        or os.getenv("CUSTOMER_SERVICE_DATABASE_URL", "").strip()
        or "postgresql+psycopg2:///amazon_ads"
    )

    return AuthSettings(
        database_url=database_url,
        jwt_secret_key=os.getenv("JWT_SECRET_KEY", "change-me-in-production").strip(),
        jwt_algorithm=os.getenv("JWT_ALGORITHM", "HS256").strip() or "HS256",
        jwt_expire_minutes=jwt_expire_minutes,
        bcrypt_rounds=bcrypt_rounds,
        bootstrap_admin_username=os.getenv("BOOTSTRAP_ADMIN_USERNAME", "admin").strip().lower() or "admin",
        bootstrap_admin_email=os.getenv("BOOTSTRAP_ADMIN_EMAIL", "admin@example.com").strip().lower(),
        bootstrap_admin_password=os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "ChangeThisPassword123!").strip(),
        bootstrap_tenant_name=os.getenv("BOOTSTRAP_TENANT_NAME", "default").strip() or "default",
    )

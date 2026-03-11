from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import get_auth_settings


class Base(DeclarativeBase):
    """Declarative base shared across auth and customer-service models."""


settings = get_auth_settings()
engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def get_db_session() -> Iterator[Session]:
    """FastAPI dependency that yields a transactional SQLAlchemy session."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

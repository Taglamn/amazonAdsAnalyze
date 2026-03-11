from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy.orm import Session

from app.auth.database import Base, SessionLocal, engine
from app.auth.models import BuyerMessage, MessageStatus


def init_customer_service_schema() -> None:
    """Create customer-service related tables in shared PostgreSQL schema."""

    Base.metadata.create_all(bind=engine)


def get_db_session() -> Iterator[Session]:
    """Dependency that yields SQLAlchemy session for customer-service APIs."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

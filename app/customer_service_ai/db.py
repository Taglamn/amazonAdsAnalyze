from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.auth.database import Base, SessionLocal, engine
from app.auth.models import BuyerMessage, MessageStatus


def init_customer_service_schema() -> None:
    """Create customer-service related tables in shared PostgreSQL schema."""

    Base.metadata.create_all(bind=engine)
    _ensure_buyer_messages_hash_index()


def get_db_session() -> Iterator[Session]:
    """Dependency that yields SQLAlchemy session for customer-service APIs."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _ensure_buyer_messages_hash_index() -> None:
    """Migrate buyer_messages unique key from long text to hash-safe key."""

    inspector = inspect(engine)
    if "buyer_messages" not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns("buyer_messages")}
    with engine.begin() as conn:
        if "buyer_message_hash" not in columns:
            conn.execute(text("ALTER TABLE buyer_messages ADD COLUMN buyer_message_hash VARCHAR(64)"))

        conn.execute(
            text(
                "UPDATE buyer_messages "
                "SET buyer_message_hash = md5(COALESCE(buyer_message, '')) "
                "WHERE buyer_message_hash IS NULL OR buyer_message_hash = ''"
            )
        )
        conn.execute(text("ALTER TABLE buyer_messages ALTER COLUMN buyer_message_hash SET NOT NULL"))

        conn.execute(
            text(
                "ALTER TABLE buyer_messages "
                "DROP CONSTRAINT IF EXISTS uq_buyer_messages_scope_conversation_message"
            )
        )
        conn.execute(text("DROP INDEX IF EXISTS uq_buyer_messages_scope_conversation_message"))

        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_buyer_messages_scope_conversation_message "
                "ON buyer_messages (tenant_id, store_id, conversation_id, buyer_message_hash)"
            )
        )

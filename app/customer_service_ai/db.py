from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .config import get_customer_service_settings


class MessageStatus(str, Enum):
    NEW = "new"
    AI_GENERATED = "ai_generated"
    AUTO_SENT = "auto_sent"
    WAITING_REVIEW = "waiting_review"
    APPROVED = "approved"
    SENT = "sent"


class Base(DeclarativeBase):
    pass


class BuyerMessage(Base):
    __tablename__ = "buyer_messages"
    __table_args__ = (
        UniqueConstraint(
            "conversation_id",
            "buyer_message",
            name="uq_buyer_messages_conversation_message",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    conversation_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    buyer_message: Mapped[str] = mapped_column(Text, nullable=False)

    category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sentiment: Mapped[str | None] = mapped_column(String(32), nullable=True)
    risk_level: Mapped[str | None] = mapped_column(String(32), nullable=True)
    product_issue: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_reply: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_reply: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=MessageStatus.NEW.value)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


settings = get_customer_service_settings()
engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_customer_service_schema() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_buyer_messages_columns()


def _ensure_buyer_messages_columns() -> None:
    inspector = inspect(engine)
    if "buyer_messages" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("buyer_messages")}
    add_sql: list[str] = []

    if "category" not in existing_columns:
        add_sql.append("ALTER TABLE buyer_messages ADD COLUMN category VARCHAR(64)")
    if "sentiment" not in existing_columns:
        add_sql.append("ALTER TABLE buyer_messages ADD COLUMN sentiment VARCHAR(32)")
    if "risk_level" not in existing_columns:
        add_sql.append("ALTER TABLE buyer_messages ADD COLUMN risk_level VARCHAR(32)")
    if "product_issue" not in existing_columns:
        add_sql.append("ALTER TABLE buyer_messages ADD COLUMN product_issue TEXT")
    if "final_reply" not in existing_columns:
        add_sql.append("ALTER TABLE buyer_messages ADD COLUMN final_reply TEXT")

    if not add_sql:
        return

    with engine.begin() as conn:
        for sql in add_sql:
            conn.execute(text(sql))


def get_db_session() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

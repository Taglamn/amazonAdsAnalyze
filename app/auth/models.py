from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


class RoleName(str, Enum):
    ADMIN = "admin"
    MANAGER = "manager"
    STAFF = "staff"
    VIEWER = "viewer"


class UserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class Tenant(Base):
    """Company-level tenant used to isolate users, stores, and messages."""

    __tablename__ = "tenants"

    tenant_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=UserStatus.ACTIVE.value)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class Role(Base):
    """Role catalog for RBAC."""

    __tablename__ = "roles"

    role_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class Store(Base):
    """Store metadata used for authorization and data scoping."""

    __tablename__ = "stores"
    __table_args__ = (
        UniqueConstraint("tenant_id", "external_store_id", name="uq_stores_tenant_external_store"),
    )

    store_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.tenant_id"), nullable=False, index=True)
    external_store_id: Mapped[str] = mapped_column(String(128), nullable=False)
    store_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=UserStatus.ACTIVE.value)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class User(Base):
    """System user authenticated by email/password with a tenant-scoped role."""

    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.tenant_id"), nullable=False, index=True)
    username: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role_id: Mapped[int] = mapped_column(ForeignKey("roles.role_id"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=UserStatus.ACTIVE.value)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    role: Mapped[Role] = relationship(lazy="joined")


class UserStoreMapping(Base):
    """Association table for user-to-store authorization scope."""

    __tablename__ = "user_store_mapping"
    __table_args__ = (
        UniqueConstraint("user_id", "store_id", name="uq_user_store_mapping_user_store"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.user_id"), nullable=False, index=True)
    store_id: Mapped[int] = mapped_column(ForeignKey("stores.store_id"), nullable=False, index=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.tenant_id"), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class MessageStatus(str, Enum):
    NEW = "new"
    AI_GENERATED = "ai_generated"
    AUTO_SENT = "auto_sent"
    WAITING_REVIEW = "waiting_review"
    APPROVED = "approved"
    SENT = "sent"


class BuyerMessage(Base):
    """Customer-service message record scoped by tenant and store."""

    __tablename__ = "buyer_messages"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "store_id",
            "conversation_id",
            "buyer_message_hash",
            name="uq_buyer_messages_scope_conversation_message",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.tenant_id"), nullable=False, index=True)
    store_id: Mapped[int] = mapped_column(ForeignKey("stores.store_id"), nullable=False, index=True)

    conversation_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    buyer_message: Mapped[str] = mapped_column(Text, nullable=False)
    buyer_message_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sentiment: Mapped[str | None] = mapped_column(String(32), nullable=True)
    risk_level: Mapped[str | None] = mapped_column(String(32), nullable=True)
    product_issue: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_reply: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_reply: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=MessageStatus.NEW.value)

    approved_by_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.user_id"), nullable=True, index=True
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

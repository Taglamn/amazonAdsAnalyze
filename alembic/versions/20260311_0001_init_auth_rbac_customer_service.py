"""Init auth, RBAC, store mapping, and customer service tables.

Revision ID: 20260311_0001
Revises: 
Create Date: 2026-03-11 18:10:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260311_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("tenant_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=128), nullable=False, unique=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "roles",
        sa.Column("role_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=32), nullable=False, unique=True),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "stores",
        sa.Column("store_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.tenant_id"), nullable=False),
        sa.Column("external_store_id", sa.String(length=128), nullable=False),
        sa.Column("store_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "external_store_id", name="uq_stores_tenant_external_store"),
    )
    op.create_index("ix_stores_tenant_id", "stores", ["tenant_id"], unique=False)

    op.create_table(
        "users",
        sa.Column("user_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.tenant_id"), nullable=False),
        sa.Column("username", sa.String(length=64), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("role_id", sa.Integer(), sa.ForeignKey("roles.role_id"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_login", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("username", name="uq_users_username"),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    op.create_index("ix_users_role_id", "users", ["role_id"], unique=False)
    op.create_index("ix_users_tenant_id", "users", ["tenant_id"], unique=False)

    op.create_table(
        "user_store_mapping",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=False),
        sa.Column("store_id", sa.Integer(), sa.ForeignKey("stores.store_id"), nullable=False),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.tenant_id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("user_id", "store_id", name="uq_user_store_mapping_user_store"),
    )
    op.create_index("ix_user_store_mapping_user_id", "user_store_mapping", ["user_id"], unique=False)
    op.create_index("ix_user_store_mapping_store_id", "user_store_mapping", ["store_id"], unique=False)
    op.create_index("ix_user_store_mapping_tenant_id", "user_store_mapping", ["tenant_id"], unique=False)

    op.create_table(
        "buyer_messages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.tenant_id"), nullable=False),
        sa.Column("store_id", sa.Integer(), sa.ForeignKey("stores.store_id"), nullable=False),
        sa.Column("conversation_id", sa.String(length=128), nullable=False),
        sa.Column("buyer_message", sa.Text(), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=True),
        sa.Column("sentiment", sa.String(length=32), nullable=True),
        sa.Column("risk_level", sa.String(length=32), nullable=True),
        sa.Column("product_issue", sa.Text(), nullable=True),
        sa.Column("ai_reply", sa.Text(), nullable=True),
        sa.Column("final_reply", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("approved_by_user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "tenant_id",
            "store_id",
            "conversation_id",
            "buyer_message",
            name="uq_buyer_messages_scope_conversation_message",
        ),
    )
    op.create_index("ix_buyer_messages_tenant_id", "buyer_messages", ["tenant_id"], unique=False)
    op.create_index("ix_buyer_messages_store_id", "buyer_messages", ["store_id"], unique=False)
    op.create_index("ix_buyer_messages_conversation_id", "buyer_messages", ["conversation_id"], unique=False)
    op.create_index("ix_buyer_messages_approved_by_user_id", "buyer_messages", ["approved_by_user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_buyer_messages_approved_by_user_id", table_name="buyer_messages")
    op.drop_index("ix_buyer_messages_conversation_id", table_name="buyer_messages")
    op.drop_index("ix_buyer_messages_store_id", table_name="buyer_messages")
    op.drop_index("ix_buyer_messages_tenant_id", table_name="buyer_messages")
    op.drop_table("buyer_messages")

    op.drop_index("ix_user_store_mapping_tenant_id", table_name="user_store_mapping")
    op.drop_index("ix_user_store_mapping_store_id", table_name="user_store_mapping")
    op.drop_index("ix_user_store_mapping_user_id", table_name="user_store_mapping")
    op.drop_table("user_store_mapping")

    op.drop_index("ix_users_tenant_id", table_name="users")
    op.drop_index("ix_users_role_id", table_name="users")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_table("users")

    op.drop_index("ix_stores_tenant_id", table_name="stores")
    op.drop_table("stores")

    op.drop_table("roles")
    op.drop_table("tenants")

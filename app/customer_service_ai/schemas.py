from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .db import MessageStatus


class AIReplyJSON(BaseModel):
    """Structured AI output payload required by customer-service workflow."""

    category: str
    sentiment: str
    risk_level: str
    product_issue: str
    reply: str


class BuyerMessageOut(BaseModel):
    id: int
    tenant_id: int
    store_id: int
    conversation_id: str
    buyer_message: str
    category: str | None
    sentiment: str | None
    risk_level: str | None
    product_issue: str | None
    ai_reply: str | None
    final_reply: str | None
    status: MessageStatus
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class BuyerMessageListResponse(BaseModel):
    items: list[BuyerMessageOut]


class FetchMessagesRequest(BaseModel):
    auto_process: bool = True
    auto_generate: bool | None = None
    async_mode: bool = True


class ProcessMessageRequest(BaseModel):
    async_mode: bool = True
    force_regenerate: bool = False
    allow_auto_send: bool = True


class SendAttachmentIn(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    content_base64: str = Field(min_length=1)
    content_type: str | None = Field(default=None, max_length=128)
    size: int | None = Field(default=None, ge=0)


class SendReplyRequest(BaseModel):
    async_mode: bool = True
    attachments: list[SendAttachmentIn] = Field(default_factory=list, max_length=10)


class EditReplyRequest(BaseModel):
    final_reply: str = Field(min_length=1, max_length=2000)


class TaskQueuedResponse(BaseModel):
    queued: bool = True
    task_id: str


class InlineFetchResponse(BaseModel):
    fetched_count: int
    created_count: int
    processed_count: int


class PipelineResultOut(AIReplyJSON):
    pass


class MessageOperationResponse(BaseModel):
    message_id: int
    status: MessageStatus
    pipeline: PipelineResultOut | None = None
    auto_sent: bool = False
    sp_api_result: dict[str, Any] | None = None


class SendOperationResponse(BaseModel):
    message_id: int
    status: MessageStatus
    sp_api_result: dict[str, Any]


class MailDetailResponse(BaseModel):
    message_id: int
    conversation_id: str
    subject: str | None = None
    text_html: str | None = None
    text_plain: str | None = None
    from_name: str | None = None
    from_address: str | None = None
    to_address_all: str | None = None
    cc: str | None = None
    bcc: str | None = None
    date: str | None = None
    attachments: list[dict[str, Any]] = Field(default_factory=list)
    raw_data: dict[str, Any] = Field(default_factory=dict)


class MailServerSettingsResponse(BaseModel):
    username: str = ""
    imap_host: str = ""
    imap_port: int = 993
    imap_mailbox: str = "INBOX"
    smtp_host: str = ""
    smtp_port: int = 465
    smtp_use_ssl: bool = True
    smtp_starttls: bool = False
    timeout_seconds: int = 30
    password_set: bool = False
    configured: bool = False


class MailServerSettingsUpdateRequest(BaseModel):
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(default="", max_length=255)
    imap_host: str = Field(min_length=1, max_length=255)
    imap_port: int = Field(default=993, ge=1, le=65535)
    imap_mailbox: str = Field(default="INBOX", min_length=1, max_length=255)
    smtp_host: str = Field(min_length=1, max_length=255)
    smtp_port: int = Field(default=465, ge=1, le=65535)
    smtp_use_ssl: bool = True
    smtp_starttls: bool = False
    timeout_seconds: int = Field(default=30, ge=1, le=300)

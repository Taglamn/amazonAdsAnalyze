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


class SendReplyRequest(BaseModel):
    async_mode: bool = True


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

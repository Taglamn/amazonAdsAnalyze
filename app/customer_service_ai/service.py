from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from .auto_reply_engine import AutoReplyEngine
from .db import BuyerMessage, MessageStatus
from .human_review import HumanReviewEngine
from .llm import CustomerServiceLLM
from .message_classification import MessageClassificationService
from .message_send import MessageSendService
from .message_storage import MessageStorageService, StoreMessagesResult
from .message_sync import MessageSyncService
from .reply_generation import ReplyGenerationService
from .risk_detection import ProductIssueExtractionService, RiskDetectionService
from .sentiment_analysis import SentimentAnalysisService


class MessageNotFoundError(RuntimeError):
    pass


class MessageStateError(RuntimeError):
    pass


@dataclass(frozen=True)
class PipelineResult:
    category: str
    sentiment: str
    risk_level: str
    product_issue: str
    reply: str


@dataclass(frozen=True)
class ProcessResult:
    message: BuyerMessage
    pipeline: PipelineResult
    auto_sent: bool
    sp_api_result: dict[str, Any] | None


def list_messages(
    db: Session,
    *,
    tenant_id: int,
    store_id: int,
    status: MessageStatus | None = None,
    category: str | None = None,
    risk_level: str | None = None,
    limit: int = 100,
) -> list[BuyerMessage]:
    """List scoped buyer messages with optional status/category/risk filters."""

    stmt = select(BuyerMessage).where(
        and_(
            BuyerMessage.tenant_id == tenant_id,
            BuyerMessage.store_id == store_id,
        )
    )
    if status is not None:
        stmt = stmt.where(BuyerMessage.status == status.value)
    if category:
        stmt = stmt.where(BuyerMessage.category == category)
    if risk_level:
        stmt = stmt.where(BuyerMessage.risk_level == risk_level)
    stmt = stmt.order_by(desc(BuyerMessage.created_at), desc(BuyerMessage.id)).limit(limit)
    return list(db.execute(stmt).scalars().all())


def get_message(db: Session, *, message_id: int, tenant_id: int, store_id: int) -> BuyerMessage:
    """Load a scoped buyer message by id."""

    stmt = select(BuyerMessage).where(
        and_(
            BuyerMessage.id == message_id,
            BuyerMessage.tenant_id == tenant_id,
            BuyerMessage.store_id == store_id,
        )
    )
    message = db.execute(stmt).scalar_one_or_none()
    if message is None:
        raise MessageNotFoundError(f"Buyer message {message_id} not found")
    return message


def fetch_and_store_messages(
    db: Session,
    *,
    tenant_id: int,
    store_id: int,
    sync_service: MessageSyncService,
    storage_service: MessageStorageService,
) -> StoreMessagesResult:
    """Fetch messages from SP-API and store them in scoped message table."""

    incoming = sync_service.fetch_messages()
    return storage_service.store_messages(
        db=db,
        incoming=incoming,
        tenant_id=tenant_id,
        store_id=store_id,
    )


def process_message_pipeline(
    db: Session,
    *,
    message_id: int,
    tenant_id: int,
    store_id: int,
    llm: CustomerServiceLLM,
    classification_service: MessageClassificationService,
    sentiment_service: SentimentAnalysisService,
    risk_service: RiskDetectionService,
    issue_service: ProductIssueExtractionService,
    reply_service: ReplyGenerationService,
    auto_reply_engine: AutoReplyEngine,
    human_review_engine: HumanReviewEngine,
    send_service: MessageSendService,
    force_regenerate: bool = False,
    allow_auto_send: bool = True,
) -> ProcessResult:
    """Run AI analysis/reply pipeline and optionally auto-send."""

    message = get_message(db, message_id=message_id, tenant_id=tenant_id, store_id=store_id)

    if message.status in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value} and not force_regenerate:
        return ProcessResult(
            message=message,
            pipeline=_pipeline_from_message(message),
            auto_sent=message.status == MessageStatus.AUTO_SENT.value,
            sp_api_result=None,
        )

    if message.ai_reply and message.status in {
        MessageStatus.AI_GENERATED.value,
        MessageStatus.WAITING_REVIEW.value,
        MessageStatus.APPROVED.value,
    } and not force_regenerate:
        return ProcessResult(
            message=message,
            pipeline=_pipeline_from_message(message),
            auto_sent=False,
            sp_api_result=None,
        )

    buyer_text = message.buyer_message

    classification = classification_service.classify(llm=llm, buyer_message=buyer_text)
    sentiment = sentiment_service.analyze(llm=llm, buyer_message=buyer_text)
    risk = risk_service.detect(llm=llm, buyer_message=buyer_text)
    issue = issue_service.extract(llm=llm, buyer_message=buyer_text)
    raw_reply = reply_service.generate(llm=llm, buyer_message=buyer_text, category=classification.category)
    cleaned_reply = llm.sanitize_reply(raw_reply)

    pipeline = PipelineResult(
        category=classification.category,
        sentiment=sentiment.sentiment,
        risk_level=risk.risk_level,
        product_issue=issue.product_issue,
        reply=cleaned_reply,
    )

    message.category = pipeline.category
    message.sentiment = pipeline.sentiment
    message.risk_level = pipeline.risk_level
    message.product_issue = pipeline.product_issue or None
    message.ai_reply = pipeline.reply
    message.final_reply = pipeline.reply
    message.status = MessageStatus.AI_GENERATED.value

    auto_sent = False
    sp_result: dict[str, Any] | None = None
    requires_review = human_review_engine.requires_review(
        category=pipeline.category,
        risk_level=pipeline.risk_level,
    )

    can_auto_send = auto_reply_engine.should_auto_send(
        category=pipeline.category,
        risk_level=pipeline.risk_level,
    )

    if allow_auto_send and can_auto_send and not requires_review:
        try:
            sp_result = send_service.send(
                conversation_id=message.conversation_id,
                reply=message.final_reply or message.ai_reply or "",
            )
            message.status = MessageStatus.AUTO_SENT.value
            message.sent_at = datetime.now(timezone.utc)
            auto_sent = True
        except Exception:  # noqa: BLE001
            message.status = MessageStatus.WAITING_REVIEW.value
    else:
        message.status = MessageStatus.WAITING_REVIEW.value

    db.add(message)
    db.commit()
    db.refresh(message)

    return ProcessResult(
        message=message,
        pipeline=pipeline,
        auto_sent=auto_sent,
        sp_api_result=sp_result,
    )


def update_final_reply(
    db: Session,
    *,
    message_id: int,
    tenant_id: int,
    store_id: int,
    final_reply: str,
) -> BuyerMessage:
    """Update the final editable reply for a scoped message."""

    message = get_message(db, message_id=message_id, tenant_id=tenant_id, store_id=store_id)
    if message.status in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value}:
        raise MessageStateError("Cannot edit a message that has already been sent")

    cleaned = final_reply.strip()
    if not cleaned:
        raise MessageStateError("Edited reply cannot be empty")

    message.final_reply = cleaned
    if message.status != MessageStatus.APPROVED.value:
        message.status = MessageStatus.WAITING_REVIEW.value

    db.add(message)
    db.commit()
    db.refresh(message)
    return message


def approve_reply(
    db: Session,
    *,
    message_id: int,
    tenant_id: int,
    store_id: int,
    approver_user_id: int | None = None,
) -> BuyerMessage:
    """Approve scoped reply so it can be sent through SP-API."""

    message = get_message(db, message_id=message_id, tenant_id=tenant_id, store_id=store_id)

    if message.status in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value}:
        raise MessageStateError("Message already sent")

    reply_to_use = (message.final_reply or message.ai_reply or "").strip()
    if not reply_to_use:
        raise MessageStateError("Cannot approve message without reply")

    message.final_reply = reply_to_use
    message.status = MessageStatus.APPROVED.value
    if approver_user_id is not None:
        message.approved_by_user_id = approver_user_id

    db.add(message)
    db.commit()
    db.refresh(message)
    return message


def send_approved_reply(
    db: Session,
    *,
    message_id: int,
    tenant_id: int,
    store_id: int,
    send_service: MessageSendService,
) -> tuple[BuyerMessage, dict[str, Any]]:
    """Send approved scoped reply to SP-API and mark as sent."""

    message = get_message(db, message_id=message_id, tenant_id=tenant_id, store_id=store_id)

    if message.status != MessageStatus.APPROVED.value:
        raise MessageStateError("Message must be approved before sending")

    final_reply = (message.final_reply or message.ai_reply or "").strip()
    if not final_reply:
        raise MessageStateError("Message has no reply to send")

    sp_result = send_service.send(conversation_id=message.conversation_id, reply=final_reply)

    message.status = MessageStatus.SENT.value
    message.sent_at = datetime.now(timezone.utc)
    db.add(message)
    db.commit()
    db.refresh(message)
    return message, sp_result


def _pipeline_from_message(message: BuyerMessage) -> PipelineResult:
    """Build standardized pipeline response payload from persisted message row."""

    return PipelineResult(
        category=(message.category or "other"),
        sentiment=(message.sentiment or "neutral"),
        risk_level=(message.risk_level or "medium"),
        product_issue=(message.product_issue or ""),
        reply=(message.final_reply or message.ai_reply or ""),
    )

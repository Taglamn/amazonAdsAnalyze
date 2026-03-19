from __future__ import annotations

from typing import Any

from .analysis_context import build_message_analysis_text
from .auto_reply_engine import AutoReplyEngine
from .db import MessageStatus, SessionLocal, init_customer_service_schema
from .human_review import HumanReviewEngine
from .llm import CustomerServiceLLM
from .message_classification import MessageClassificationService
from .message_meta_store import message_meta_store
from .message_send import MessageSendService
from .message_storage import MessageStorageService
from .message_sync import MessageSyncService
from .queue import celery_app
from .reply_generation import ReplyGenerationService
from .risk_detection import ProductIssueExtractionService, RiskDetectionService
from .sentiment_analysis import SentimentAnalysisService
from .service import (
    fetch_and_store_messages,
    get_message,
    list_unprocessed_message_ids,
    process_message_pipeline,
    send_approved_reply,
)


@celery_app.task(name="customer_service.fetch_buyer_messages")
def fetch_buyer_messages_task(
    tenant_id: int,
    store_id: int,
    auto_process: bool = True,
) -> dict[str, Any]:
    """Fetch and optionally process messages for specific tenant/store."""

    init_customer_service_schema()
    db = SessionLocal()
    try:
        sync_service = MessageSyncService()
        storage_service = MessageStorageService()
        result = fetch_and_store_messages(
            db=db,
            tenant_id=tenant_id,
            store_id=store_id,
            sync_service=sync_service,
            storage_service=storage_service,
        )
        message_meta_store.upsert_incoming_messages(
            tenant_id=tenant_id,
            store_id=store_id,
            incoming_messages=result.incoming_messages,
        )

        processed = 0
        if auto_process:
            pending_ids = list_unprocessed_message_ids(
                db=db,
                tenant_id=tenant_id,
                store_id=store_id,
            )
            process_ids = list(dict.fromkeys([*result.new_message_ids, *pending_ids]))
            for message_id in process_ids:
                process_message_task.delay(
                    tenant_id=tenant_id,
                    store_id=store_id,
                    message_id=message_id,
                )
                processed += 1

        return {
            "tenant_id": tenant_id,
            "store_id": store_id,
            "fetched_count": result.fetched_count,
            "created_count": result.created_count,
            "processed_count": processed,
        }
    finally:
        db.close()


@celery_app.task(name="customer_service.process_message")
def process_message_task(
    tenant_id: int,
    store_id: int,
    message_id: int,
    force_regenerate: bool = False,
    allow_auto_send: bool = True,
) -> dict[str, Any]:
    """Run AI pipeline for a message under tenant/store scope."""

    init_customer_service_schema()
    db = SessionLocal()
    try:
        scoped_message = get_message(
            db,
            message_id=message_id,
            tenant_id=tenant_id,
            store_id=store_id,
        )
        analysis_text: str | None = None
        if scoped_message.status not in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value}:
            detail = message_meta_store.get_message_detail(
                tenant_id=tenant_id,
                store_id=store_id,
                conversation_id=scoped_message.conversation_id,
            )
            analysis_text = build_message_analysis_text(message=scoped_message, detail=detail)
        result = process_message_pipeline(
            db=db,
            message_id=message_id,
            tenant_id=tenant_id,
            store_id=store_id,
            llm=CustomerServiceLLM(),
            classification_service=MessageClassificationService(),
            sentiment_service=SentimentAnalysisService(),
            risk_service=RiskDetectionService(),
            issue_service=ProductIssueExtractionService(),
            reply_service=ReplyGenerationService(),
            auto_reply_engine=AutoReplyEngine(),
            human_review_engine=HumanReviewEngine(),
            send_service=MessageSendService(
                tenant_id=tenant_id,
                store_id=store_id,
            ),
            force_regenerate=force_regenerate,
            allow_auto_send=allow_auto_send,
            analysis_text=analysis_text,
        )

        return {
            "tenant_id": tenant_id,
            "store_id": store_id,
            "message_id": result.message.id,
            "status": result.message.status,
            "category": result.pipeline.category,
            "sentiment": result.pipeline.sentiment,
            "risk_level": result.pipeline.risk_level,
            "product_issue": result.pipeline.product_issue,
            "reply": result.pipeline.reply,
            "auto_sent": result.auto_sent,
            "sp_api_result": result.sp_api_result,
        }
    finally:
        db.close()


@celery_app.task(name="customer_service.send_approved_reply")
def send_approved_reply_task(tenant_id: int, store_id: int, message_id: int) -> dict[str, Any]:
    """Send approved reply for a scoped message."""

    init_customer_service_schema()
    db = SessionLocal()
    try:
        message, sp_result = send_approved_reply(
            db=db,
            message_id=message_id,
            tenant_id=tenant_id,
            store_id=store_id,
            send_service=MessageSendService(
                tenant_id=tenant_id,
                store_id=store_id,
            ),
        )
        return {
            "tenant_id": tenant_id,
            "store_id": store_id,
            "message_id": message.id,
            "status": message.status,
            "sp_api_result": sp_result,
        }
    finally:
        db.close()

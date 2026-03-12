from __future__ import annotations

from typing import Any

from app.auth.models import Store

from .analysis_context import build_message_analysis_text
from .auto_reply_engine import AutoReplyEngine
from .db import MessageStatus, SessionLocal, init_customer_service_schema
from .human_review import HumanReviewEngine
from .llm import CustomerServiceLLM
from .message_classification import MessageClassificationService
from .message_send import MessageSendService
from .message_storage import MessageStorageService
from .message_sync import MessageSyncService
from .queue import celery_app
from .reply_generation import ReplyGenerationService
from .risk_detection import ProductIssueExtractionService, RiskDetectionService
from .sentiment_analysis import SentimentAnalysisService
from .service import fetch_and_store_messages, get_message, process_message_pipeline, send_approved_reply
from .sp_api import LingxingMessagingClient, MessagingAPIError


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
        client = LingxingMessagingClient()
        store = db.get(Store, store_id)
        if store is None:
            raise RuntimeError(f"Store {store_id} not found")
        target_store = client.resolve_store(external_store_id=store.external_store_id)
        sync_service = MessageSyncService(
            client=client,
            store_name=target_store.store_name,
            sid=target_store.sid,
            email=target_store.email,
            external_store_id=target_store.external_store_id,
        )
        storage_service = MessageStorageService()
        result = fetch_and_store_messages(
            db=db,
            tenant_id=tenant_id,
            store_id=store_id,
            sync_service=sync_service,
            storage_service=storage_service,
        )

        processed = 0
        if auto_process:
            for message_id in result.new_message_ids:
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
        client = LingxingMessagingClient()
        store = db.get(Store, store_id)
        if store is None:
            raise RuntimeError(f"Store {store_id} not found")
        target_store = client.resolve_store(external_store_id=store.external_store_id)
        scoped_message = get_message(
            db,
            message_id=message_id,
            tenant_id=tenant_id,
            store_id=store_id,
        )
        analysis_text: str | None = None
        if scoped_message.status not in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value}:
            try:
                analysis_text = build_message_analysis_text(
                    message=scoped_message,
                    client=client,
                    target_store=target_store,
                )
            except MessagingAPIError:
                analysis_text = None
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
                client=client,
                store_name=target_store.store_name,
                sid=target_store.sid,
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
        client = LingxingMessagingClient()
        store = db.get(Store, store_id)
        if store is None:
            raise RuntimeError(f"Store {store_id} not found")
        target_store = client.resolve_store(external_store_id=store.external_store_id)
        message, sp_result = send_approved_reply(
            db=db,
            message_id=message_id,
            tenant_id=tenant_id,
            store_id=store_id,
            send_service=MessageSendService(
                client=client,
                store_name=target_store.store_name,
                sid=target_store.sid,
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

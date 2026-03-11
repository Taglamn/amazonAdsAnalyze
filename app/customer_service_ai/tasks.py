from __future__ import annotations

from typing import Any

from .auto_reply_engine import AutoReplyEngine
from .db import SessionLocal, init_customer_service_schema
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
from .service import fetch_and_store_messages, process_message_pipeline, send_approved_reply
from .sp_api import AmazonSPMessagingClient


@celery_app.task(name="customer_service.fetch_buyer_messages")
def fetch_buyer_messages_task(auto_process: bool = True) -> dict[str, Any]:
    init_customer_service_schema()
    db = SessionLocal()
    try:
        sp_client = AmazonSPMessagingClient()
        sync_service = MessageSyncService(client=sp_client)
        storage_service = MessageStorageService()
        result = fetch_and_store_messages(db=db, sync_service=sync_service, storage_service=storage_service)

        processed = 0
        if auto_process:
            for message_id in result.new_message_ids:
                process_message_task.delay(message_id=message_id)
                processed += 1

        return {
            "fetched_count": result.fetched_count,
            "created_count": result.created_count,
            "processed_count": processed,
        }
    finally:
        db.close()


@celery_app.task(name="customer_service.process_message")
def process_message_task(
    message_id: int,
    force_regenerate: bool = False,
    allow_auto_send: bool = True,
) -> dict[str, Any]:
    init_customer_service_schema()
    db = SessionLocal()
    try:
        sp_client = AmazonSPMessagingClient()
        result = process_message_pipeline(
            db=db,
            message_id=message_id,
            llm=CustomerServiceLLM(),
            classification_service=MessageClassificationService(),
            sentiment_service=SentimentAnalysisService(),
            risk_service=RiskDetectionService(),
            issue_service=ProductIssueExtractionService(),
            reply_service=ReplyGenerationService(),
            auto_reply_engine=AutoReplyEngine(),
            human_review_engine=HumanReviewEngine(),
            send_service=MessageSendService(client=sp_client),
            force_regenerate=force_regenerate,
            allow_auto_send=allow_auto_send,
        )

        return {
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
def send_approved_reply_task(message_id: int) -> dict[str, Any]:
    init_customer_service_schema()
    db = SessionLocal()
    try:
        sp_client = AmazonSPMessagingClient()
        message, sp_result = send_approved_reply(
            db=db,
            message_id=message_id,
            send_service=MessageSendService(client=sp_client),
        )
        return {
            "message_id": message.id,
            "status": message.status,
            "sp_api_result": sp_result,
        }
    finally:
        db.close()

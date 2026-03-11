from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from .auto_reply_engine import AutoReplyEngine
from .db import MessageStatus, get_db_session
from .human_review import HumanReviewEngine
from .llm import CustomerServiceLLM
from .message_classification import MessageClassificationService
from .message_send import MessageSendService
from .message_storage import MessageStorageService
from .message_sync import MessageSyncService
from .reply_generation import ReplyGenerationService
from .risk_detection import ProductIssueExtractionService, RiskDetectionService
from .schemas import (
    BuyerMessageListResponse,
    EditReplyRequest,
    FetchMessagesRequest,
    InlineFetchResponse,
    MessageOperationResponse,
    PipelineResultOut,
    ProcessMessageRequest,
    SendOperationResponse,
    SendReplyRequest,
    TaskQueuedResponse,
)
from .sentiment_analysis import SentimentAnalysisService
from .service import (
    MessageNotFoundError,
    MessageStateError,
    ProcessResult,
    approve_reply,
    fetch_and_store_messages,
    list_messages,
    process_message_pipeline,
    send_approved_reply,
    update_final_reply,
)
from .sp_api import AmazonSPMessagingClient
from .tasks import fetch_buyer_messages_task, process_message_task, send_approved_reply_task

router = APIRouter(prefix="/api/customer-service", tags=["customer-service"])


@router.post("/messages/fetch", response_model=TaskQueuedResponse | InlineFetchResponse)
def fetch_messages(payload: FetchMessagesRequest, db: Session = Depends(get_db_session)):
    auto_process = payload.auto_process if payload.auto_generate is None else payload.auto_generate

    if payload.async_mode:
        task = fetch_buyer_messages_task.delay(auto_process=auto_process)
        return TaskQueuedResponse(task_id=task.id)

    sp_client = AmazonSPMessagingClient()
    result = fetch_and_store_messages(
        db=db,
        sync_service=MessageSyncService(client=sp_client),
        storage_service=MessageStorageService(),
    )

    processed_count = 0
    if auto_process:
        for message_id in result.new_message_ids:
            process_message_pipeline(
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
                force_regenerate=False,
                allow_auto_send=True,
            )
            processed_count += 1

    return InlineFetchResponse(
        fetched_count=result.fetched_count,
        created_count=result.created_count,
        processed_count=processed_count,
    )


@router.get("/messages", response_model=BuyerMessageListResponse)
def get_messages(
    status: MessageStatus | None = Query(default=None),
    category: str | None = Query(default=None),
    risk_level: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db_session),
):
    items = list_messages(db=db, status=status, category=category, risk_level=risk_level, limit=limit)
    return BuyerMessageListResponse(items=items)


@router.post("/messages/{message_id}/process", response_model=TaskQueuedResponse | MessageOperationResponse)
def process_message(
    message_id: int,
    payload: ProcessMessageRequest,
    db: Session = Depends(get_db_session),
):
    if payload.async_mode:
        task = process_message_task.delay(
            message_id=message_id,
            force_regenerate=payload.force_regenerate,
            allow_auto_send=payload.allow_auto_send,
        )
        return TaskQueuedResponse(task_id=task.id)

    try:
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
            send_service=MessageSendService(client=AmazonSPMessagingClient()),
            force_regenerate=payload.force_regenerate,
            allow_auto_send=payload.allow_auto_send,
        )
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return _process_to_response(result)


@router.post("/messages/{message_id}/generate", response_model=TaskQueuedResponse | MessageOperationResponse)
def generate_reply(
    message_id: int,
    payload: ProcessMessageRequest,
    db: Session = Depends(get_db_session),
):
    return process_message(message_id=message_id, payload=payload, db=db)


@router.patch("/messages/{message_id}/reply", response_model=MessageOperationResponse)
def edit_reply(
    message_id: int,
    payload: EditReplyRequest,
    db: Session = Depends(get_db_session),
):
    try:
        message = update_final_reply(db=db, message_id=message_id, final_reply=payload.final_reply)
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return MessageOperationResponse(message_id=message.id, status=MessageStatus(message.status))


@router.post("/messages/{message_id}/approve", response_model=MessageOperationResponse)
def approve_message(message_id: int, db: Session = Depends(get_db_session)):
    try:
        message = approve_reply(db=db, message_id=message_id)
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return MessageOperationResponse(message_id=message.id, status=MessageStatus(message.status))


@router.post("/messages/{message_id}/send", response_model=TaskQueuedResponse | SendOperationResponse)
def send_message(
    message_id: int,
    payload: SendReplyRequest,
    db: Session = Depends(get_db_session),
):
    if payload.async_mode:
        task = send_approved_reply_task.delay(message_id=message_id)
        return TaskQueuedResponse(task_id=task.id)

    try:
        message, sp_result = send_approved_reply(
            db=db,
            message_id=message_id,
            send_service=MessageSendService(client=AmazonSPMessagingClient()),
        )
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return SendOperationResponse(
        message_id=message.id,
        status=MessageStatus(message.status),
        sp_api_result=sp_result,
    )


def _process_to_response(result: ProcessResult) -> MessageOperationResponse:
    return MessageOperationResponse(
        message_id=result.message.id,
        status=MessageStatus(result.message.status),
        pipeline=PipelineResultOut(
            category=result.pipeline.category,
            sentiment=result.pipeline.sentiment,
            risk_level=result.pipeline.risk_level,
            product_issue=result.pipeline.product_issue,
            reply=result.pipeline.reply,
        ),
        auto_sent=result.auto_sent,
        sp_api_result=result.sp_api_result,
    )

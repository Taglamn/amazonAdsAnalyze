from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.auth.crud import get_or_create_store, get_store_by_external_id
from app.auth.database import get_db_session
from app.auth.dependencies import enforce_store_access, require_roles
from app.auth.models import RoleName, User

from .auto_reply_engine import AutoReplyEngine
from .db import MessageStatus
from .human_review import HumanReviewEngine
from .llm import CustomerServiceLLM
from .message_classification import MessageClassificationService
from .message_send import MessageSendService
from .message_storage import MessageStorageService
from .message_sync import MessageSyncService
from .reply_generation import ReplyGenerationService
from .risk_detection import ProductIssueExtractionService, RiskDetectionService
from .schemas import (
    AmazonEmailSettingsResponse,
    AmazonEmailSettingsUpdateRequest,
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
from .settings_store import amazon_email_settings_store
from .sp_api import AmazonSPMessagingClient
from .tasks import fetch_buyer_messages_task, process_message_task, send_approved_reply_task

router = APIRouter(prefix="/api/customer-service", tags=["customer-service"])


RoleDependency = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER, RoleName.STAFF))
ManagerRoleDependency = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER))


def _resolve_scoped_store(
    db: Session,
    *,
    current_user: User,
    external_store_id: str,
) -> int:
    """Resolve external store id to internal store id and enforce access."""

    external_store_id = external_store_id.strip()
    if not external_store_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="store_id cannot be empty")

    role_name = current_user.role.name if current_user.role else ""
    if role_name == RoleName.ADMIN.value:
        store = get_or_create_store(
            db,
            tenant_id=current_user.tenant_id,
            external_store_id=external_store_id,
            store_name=external_store_id,
        )
    else:
        store = get_store_by_external_id(
            db,
            tenant_id=current_user.tenant_id,
            external_store_id=external_store_id,
        )
        if store is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"No access to store {external_store_id}")

    enforce_store_access(db, current_user=current_user, external_store_id=external_store_id)
    return store.store_id


@router.get("/settings/amazon-email", response_model=AmazonEmailSettingsResponse)
def get_amazon_email_settings(current_user: User = ManagerRoleDependency):
    """Read Amazon messaging account configuration (manager/admin only)."""

    _ = current_user
    settings = amazon_email_settings_store.load()
    return AmazonEmailSettingsResponse(
        email_account=settings.email_account,
        has_password=bool(settings.email_password),
        ssl_enabled=settings.ssl_enabled,
        ssl_host=settings.ssl_host,
        ssl_port=settings.ssl_port,
        updated_at=settings.updated_at,
    )


@router.put("/settings/amazon-email", response_model=AmazonEmailSettingsResponse)
def update_amazon_email_settings(
    payload: AmazonEmailSettingsUpdateRequest,
    current_user: User = ManagerRoleDependency,
):
    """Update Amazon messaging account configuration (manager/admin only)."""

    _ = current_user
    current = amazon_email_settings_store.load()
    incoming_password = (payload.email_password or "").strip()

    if payload.keep_existing_password and not incoming_password:
        final_password = current.email_password
    else:
        if not incoming_password:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="email_password is required")
        final_password = incoming_password

    saved = amazon_email_settings_store.save(
        email_account=payload.email_account,
        email_password=final_password,
        ssl_enabled=payload.ssl_enabled,
        ssl_host=payload.ssl_host,
        ssl_port=payload.ssl_port,
    )
    return AmazonEmailSettingsResponse(
        email_account=saved.email_account,
        has_password=bool(saved.email_password),
        ssl_enabled=saved.ssl_enabled,
        ssl_host=saved.ssl_host,
        ssl_port=saved.ssl_port,
        updated_at=saved.updated_at,
    )


@router.post("/stores/{external_store_id}/messages/fetch", response_model=TaskQueuedResponse | InlineFetchResponse)
def fetch_messages(
    external_store_id: str,
    payload: FetchMessagesRequest,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Fetch new buyer messages from SP-API for authorized store scope."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    auto_process = payload.auto_process if payload.auto_generate is None else payload.auto_generate

    if payload.async_mode:
        task = fetch_buyer_messages_task.delay(
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            auto_process=auto_process,
        )
        return TaskQueuedResponse(task_id=task.id)

    sp_client = AmazonSPMessagingClient()
    result = fetch_and_store_messages(
        db=db,
        tenant_id=current_user.tenant_id,
        store_id=internal_store_id,
        sync_service=MessageSyncService(client=sp_client, external_store_id=external_store_id),
        storage_service=MessageStorageService(),
    )

    processed_count = 0
    if auto_process:
        for message_id in result.new_message_ids:
            process_message_pipeline(
                db=db,
                message_id=message_id,
                tenant_id=current_user.tenant_id,
                store_id=internal_store_id,
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


@router.get("/stores/{external_store_id}/messages", response_model=BuyerMessageListResponse)
def get_messages(
    external_store_id: str,
    status: MessageStatus | None = Query(default=None),
    category: str | None = Query(default=None),
    risk_level: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """List scoped buyer messages for the current store authorization."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    items = list_messages(
        db=db,
        tenant_id=current_user.tenant_id,
        store_id=internal_store_id,
        status=status,
        category=category,
        risk_level=risk_level,
        limit=limit,
    )
    return BuyerMessageListResponse(items=items)


@router.post("/stores/{external_store_id}/messages/{message_id}/process", response_model=TaskQueuedResponse | MessageOperationResponse)
def process_message(
    external_store_id: str,
    message_id: int,
    payload: ProcessMessageRequest,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Run AI processing pipeline for one scoped message."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    if payload.async_mode:
        task = process_message_task.delay(
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            message_id=message_id,
            force_regenerate=payload.force_regenerate,
            allow_auto_send=payload.allow_auto_send,
        )
        return TaskQueuedResponse(task_id=task.id)

    try:
        result = process_message_pipeline(
            db=db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return _process_to_response(result)


@router.post("/stores/{external_store_id}/messages/{message_id}/generate", response_model=TaskQueuedResponse | MessageOperationResponse)
def generate_reply(
    external_store_id: str,
    message_id: int,
    payload: ProcessMessageRequest,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Alias endpoint for process pipeline that returns structured AI reply JSON."""

    return process_message(
        external_store_id=external_store_id,
        message_id=message_id,
        payload=payload,
        current_user=current_user,
        db=db,
    )


@router.patch("/stores/{external_store_id}/messages/{message_id}/reply", response_model=MessageOperationResponse)
def edit_reply(
    external_store_id: str,
    message_id: int,
    payload: EditReplyRequest,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Save edited AI reply for scoped message."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    try:
        message = update_final_reply(
            db=db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            final_reply=payload.final_reply,
        )
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return MessageOperationResponse(message_id=message.id, status=MessageStatus(message.status))


@router.post("/stores/{external_store_id}/messages/{message_id}/approve", response_model=MessageOperationResponse)
def approve_message(
    external_store_id: str,
    message_id: int,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Approve scoped AI reply before sending."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    try:
        message = approve_reply(
            db=db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            approver_user_id=current_user.user_id,
        )
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return MessageOperationResponse(message_id=message.id, status=MessageStatus(message.status))


@router.post("/stores/{external_store_id}/messages/{message_id}/send", response_model=TaskQueuedResponse | SendOperationResponse)
def send_message(
    external_store_id: str,
    message_id: int,
    payload: SendReplyRequest,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Send approved reply for scoped message via Amazon SP-API."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    if payload.async_mode:
        task = send_approved_reply_task.delay(
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            message_id=message_id,
        )
        return TaskQueuedResponse(task_id=task.id)

    try:
        message, sp_result = send_approved_reply(
            db=db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            send_service=MessageSendService(client=AmazonSPMessagingClient()),
        )
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return SendOperationResponse(
        message_id=message.id,
        status=MessageStatus(message.status),
        sp_api_result=sp_result,
    )


@router.post("/stores/{external_store_id}/messages/{message_id}/approve-send", response_model=SendOperationResponse)
def approve_and_send_message(
    external_store_id: str,
    message_id: int,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Convenience endpoint: approve AI reply then send through SP-API."""

    _ = approve_message(
        external_store_id=external_store_id,
        message_id=message_id,
        current_user=current_user,
        db=db,
    )
    return send_message(
        external_store_id=external_store_id,
        message_id=message_id,
        payload=SendReplyRequest(async_mode=False),
        current_user=current_user,
        db=db,
    )


def _process_to_response(result: ProcessResult) -> MessageOperationResponse:
    """Serialize service process result to API response."""

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

from __future__ import annotations

import logging
from email.utils import parseaddr

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.auth.crud import get_or_create_store, get_store_by_external_id
from app.auth.database import get_db_session
from app.auth.dependencies import enforce_store_access, require_roles
from app.auth.models import RoleName, User

from .analysis_context import build_message_analysis_text
from .auto_reply_engine import AutoReplyEngine
from .db import MessageStatus
from .human_review import HumanReviewEngine
from .llm import CustomerServiceLLM, LLMGenerationError
from .mail_client import MailClientError, get_email_by_message_id
from .message_classification import MessageClassificationService
from .message_send import MessageSendService
from .message_meta_store import message_meta_store
from .message_storage import MessageStorageService
from .message_sync import MessageSyncService
from .reply_generation import ReplyGenerationService
from .risk_detection import ProductIssueExtractionService, RiskDetectionService
from .schemas import (
    BuyerMessageListResponse,
    EditReplyRequest,
    FetchMessagesRequest,
    InlineFetchResponse,
    MailDetailResponse,
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
    get_message,
    list_messages,
    list_unprocessed_message_ids,
    process_message_pipeline,
    send_approved_reply,
    update_final_reply,
)
from .tasks import fetch_buyer_messages_task, process_message_task, send_approved_reply_task

router = APIRouter(prefix="/api/customer-service", tags=["customer-service"])
logger = logging.getLogger(__name__)


ReadRoleDependency = Depends(
    require_roles(RoleName.ADMIN, RoleName.MANAGER, RoleName.STAFF, RoleName.VIEWER)
)
WriteRoleDependency = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER, RoleName.STAFF))


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


@router.post("/stores/{external_store_id}/messages/fetch", response_model=TaskQueuedResponse | InlineFetchResponse)
def fetch_messages(
    external_store_id: str,
    payload: FetchMessagesRequest,
    current_user: User = WriteRoleDependency,
    db: Session = Depends(get_db_session),
):
    """Fetch buyer messages from mailbox (IMAP) for one authorized store."""

    auto_process = payload.auto_process if payload.auto_generate is None else payload.auto_generate
    try:
        internal_store_id = _resolve_scoped_store(
            db=db,
            current_user=current_user,
            external_store_id=external_store_id,
        )
        if payload.async_mode:
            task = fetch_buyer_messages_task.delay(
                tenant_id=current_user.tenant_id,
                store_id=internal_store_id,
                auto_process=auto_process,
            )
            return TaskQueuedResponse(task_id=task.id)

        result = fetch_and_store_messages(
            db=db,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            sync_service=MessageSyncService(),
            storage_service=MessageStorageService(),
        )
        message_meta_store.upsert_incoming_messages(
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            incoming_messages=result.incoming_messages,
        )
        fetched_count = result.fetched_count
        created_count = result.created_count
        processed_count = 0

        if auto_process:
            pending_ids = list_unprocessed_message_ids(
                db=db,
                tenant_id=current_user.tenant_id,
                store_id=internal_store_id,
            )
            process_ids = list(dict.fromkeys([*result.new_message_ids, *pending_ids]))
            for message_id in process_ids:
                message = get_message(
                    db,
                    message_id=message_id,
                    tenant_id=current_user.tenant_id,
                    store_id=internal_store_id,
                )
                analysis_text: str | None = None
                detail = message_meta_store.get_message_detail(
                    tenant_id=current_user.tenant_id,
                    store_id=internal_store_id,
                    conversation_id=message.conversation_id,
                )
                analysis_text = build_message_analysis_text(message=message, detail=detail)
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
                    send_service=MessageSendService(
                        tenant_id=current_user.tenant_id,
                        store_id=internal_store_id,
                    ),
                    force_regenerate=False,
                    allow_auto_send=True,
                    analysis_text=analysis_text,
                )
                processed_count += 1

        return InlineFetchResponse(
            fetched_count=fetched_count,
            created_count=created_count,
            processed_count=processed_count,
        )
    except (MailClientError, LLMGenerationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("customer_service_fetch_messages_unexpected_error store=%s", external_store_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error while fetching/processing messages: {exc}",
        ) from exc


@router.get("/stores/{external_store_id}/messages", response_model=BuyerMessageListResponse)
def get_messages(
    external_store_id: str,
    status: MessageStatus | None = Query(default=None),
    category: str | None = Query(default=None),
    risk_level: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    current_user: User = ReadRoleDependency,
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


@router.get("/stores/{external_store_id}/messages/{message_id}/detail", response_model=MailDetailResponse)
def get_message_detail(
    external_store_id: str,
    message_id: int,
    current_user: User = ReadRoleDependency,
    db: Session = Depends(get_db_session),
):
    """Fetch cached/IMAP mail detail for one scoped message (expand on demand)."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    try:
        message = get_message(
            db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
        )
        detail = message_meta_store.get_message_detail(
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            conversation_id=message.conversation_id,
        )
        if detail is None:
            detail = get_email_by_message_id(message.conversation_id)
            if detail:
                message_meta_store.upsert_message_detail(
                    tenant_id=current_user.tenant_id,
                    store_id=internal_store_id,
                    conversation_id=message.conversation_id,
                    detail=detail,
                )
    except (MailClientError, RuntimeError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    detail = detail or {}
    attachments = detail.get("attachments")
    if not isinstance(attachments, list):
        attachments = []
    from_raw = str(detail.get("from") or detail.get("from_address") or "").strip()
    from_name, from_address = parseaddr(from_raw)
    to_raw = str(detail.get("to") or detail.get("to_address_all") or "").strip()
    return MailDetailResponse(
        message_id=message.id,
        conversation_id=message.conversation_id,
        subject=str(detail.get("subject") or "") or None,
        text_html=str(detail.get("text_html") or "") or None,
        text_plain=str(detail.get("text_plain") or detail.get("body") or "") or None,
        from_name=from_name or (str(detail.get("from_name") or "") or None),
        from_address=from_address or (str(detail.get("from_address") or "") or None),
        to_address_all=to_raw or None,
        cc=str(detail.get("cc") or "") or None,
        bcc=str(detail.get("bcc") or "") or None,
        date=str(detail.get("date") or "") or None,
        attachments=[item for item in attachments if isinstance(item, dict)],
        raw_data=detail,
    )


@router.post("/stores/{external_store_id}/messages/{message_id}/process", response_model=TaskQueuedResponse | MessageOperationResponse)
def process_message(
    external_store_id: str,
    message_id: int,
    payload: ProcessMessageRequest,
    current_user: User = WriteRoleDependency,
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
        scoped_message = get_message(
            db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
        )
        detail = message_meta_store.get_message_detail(
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            conversation_id=scoped_message.conversation_id,
        )
        analysis_text = build_message_analysis_text(message=scoped_message, detail=detail)
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
            send_service=MessageSendService(
                tenant_id=current_user.tenant_id,
                store_id=internal_store_id,
            ),
            force_regenerate=payload.force_regenerate,
            allow_auto_send=payload.allow_auto_send,
            analysis_text=analysis_text,
        )
    except (MailClientError, RuntimeError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LLMGenerationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "customer_service_process_message_unexpected_error store=%s message_id=%s",
            external_store_id,
            message_id,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error while generating reply: {exc}",
        ) from exc

    return _process_to_response(result)


@router.post("/stores/{external_store_id}/messages/{message_id}/generate", response_model=TaskQueuedResponse | MessageOperationResponse)
def generate_reply(
    external_store_id: str,
    message_id: int,
    payload: ProcessMessageRequest,
    current_user: User = WriteRoleDependency,
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
    current_user: User = WriteRoleDependency,
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

    return MessageOperationResponse(message_id=message.id, status=_safe_message_status(message.status))


@router.post("/stores/{external_store_id}/messages/{message_id}/approve", response_model=MessageOperationResponse)
def approve_message(
    external_store_id: str,
    message_id: int,
    current_user: User = WriteRoleDependency,
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

    return MessageOperationResponse(message_id=message.id, status=_safe_message_status(message.status))


@router.post("/stores/{external_store_id}/messages/{message_id}/send", response_model=TaskQueuedResponse | SendOperationResponse)
def send_message(
    external_store_id: str,
    message_id: int,
    payload: SendReplyRequest,
    current_user: User = WriteRoleDependency,
    db: Session = Depends(get_db_session),
):
    """Send approved reply for scoped message via SMTP channel."""

    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=external_store_id,
    )

    if payload.attachments:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Attachments are not supported in SMTP mode",
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
            attachments=[item.model_dump() for item in payload.attachments],
            send_service=MessageSendService(
                tenant_id=current_user.tenant_id,
                store_id=internal_store_id,
            ),
        )
    except (MailClientError, RuntimeError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except MessageStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return SendOperationResponse(
        message_id=message.id,
        status=_safe_message_status(message.status),
        sp_api_result=sp_result,
    )


@router.post("/stores/{external_store_id}/messages/{message_id}/approve-send", response_model=SendOperationResponse)
def approve_and_send_message(
    external_store_id: str,
    message_id: int,
    payload: SendReplyRequest | None = None,
    current_user: User = WriteRoleDependency,
    db: Session = Depends(get_db_session),
):
    """Convenience endpoint: approve AI reply then send through SMTP channel."""

    payload = payload or SendReplyRequest(async_mode=False)
    _ = approve_message(
        external_store_id=external_store_id,
        message_id=message_id,
        current_user=current_user,
        db=db,
    )
    return send_message(
        external_store_id=external_store_id,
        message_id=message_id,
        payload=SendReplyRequest(
            async_mode=False,
            attachments=payload.attachments,
        ),
        current_user=current_user,
        db=db,
    )


def _process_to_response(result: ProcessResult) -> MessageOperationResponse:
    """Serialize service process result to API response."""

    return MessageOperationResponse(
        message_id=result.message.id,
        status=_safe_message_status(result.message.status),
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


def _safe_message_status(raw_status: str) -> MessageStatus:
    """Map unknown status strings to a safe enum to avoid 500 response serialization errors."""

    try:
        return MessageStatus(raw_status)
    except ValueError:
        return MessageStatus.WAITING_REVIEW

from __future__ import annotations

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
from .sp_api import LingxingBoundStore, LingxingMessagingClient, MessagingAPIError
from .tasks import fetch_buyer_messages_task, process_message_task, send_approved_reply_task

router = APIRouter(prefix="/api/customer-service", tags=["customer-service"])


RoleDependency = Depends(require_roles(RoleName.ADMIN, RoleName.MANAGER, RoleName.STAFF))


def _build_message_client() -> LingxingMessagingClient:
    """Build Lingxing messaging client using environment credentials."""

    return LingxingMessagingClient()


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


def _resolve_target_fetch_store(
    *,
    db: Session,
    current_user: User,
    external_store_id: str,
    client: LingxingMessagingClient,
) -> tuple[LingxingBoundStore, int]:
    """Resolve exactly one target store for message sync."""

    selector = external_store_id.strip()
    if selector in {"all", "__all__", "*"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Please pass a concrete store_id from the current store selector",
        )

    bound_stores = client.list_bound_stores()
    selected = [
        item
        for item in bound_stores
        if item.external_store_id == selector or item.store_name == selector
    ]
    if not selected:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lingxing store not found: {selector}",
        )

    target = selected[0]
    internal_store_id = _resolve_scoped_store(
        db,
        current_user=current_user,
        external_store_id=target.external_store_id,
    )
    return target, internal_store_id


@router.post("/stores/{external_store_id}/messages/fetch", response_model=TaskQueuedResponse | InlineFetchResponse)
def fetch_messages(
    external_store_id: str,
    payload: FetchMessagesRequest,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Fetch buyer messages from Lingxing API for one authorized store."""

    auto_process = payload.auto_process if payload.auto_generate is None else payload.auto_generate
    try:
        client = _build_message_client()
        target_store, internal_store_id = _resolve_target_fetch_store(
            db=db,
            current_user=current_user,
            external_store_id=external_store_id,
            client=client,
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
            sync_service=MessageSyncService(
                client=client,
                store_name=target_store.store_name,
                sid=target_store.sid,
                email=target_store.email,
                external_store_id=target_store.external_store_id,
            ),
            storage_service=MessageStorageService(),
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
                try:
                    analysis_text = build_message_analysis_text(
                        message=message,
                        client=client,
                        target_store=target_store,
                    )
                except MessagingAPIError:
                    analysis_text = None
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
                        client=client,
                        store_name=target_store.store_name,
                        sid=target_store.sid,
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
    except (MessagingAPIError, LLMGenerationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


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


@router.get("/stores/{external_store_id}/messages/{message_id}/detail", response_model=MailDetailResponse)
def get_message_detail(
    external_store_id: str,
    message_id: int,
    current_user: User = RoleDependency,
    db: Session = Depends(get_db_session),
):
    """Fetch Lingxing mail detail for one scoped message (expand on demand)."""

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
        client = _build_message_client()
        target_store = client.resolve_store(external_store_id=external_store_id)
        detail = client.fetch_message_detail(
            webmail_uuid=message.conversation_id,
            store_name=target_store.store_name,
            sid=target_store.sid,
            email=target_store.email,
            external_store_id=target_store.external_store_id,
        )
    except MessagingAPIError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except MessageNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    attachments = detail.get("attachments")
    if not isinstance(attachments, list):
        attachments = []
    return MailDetailResponse(
        message_id=message.id,
        conversation_id=message.conversation_id,
        subject=str(detail.get("subject") or "") or None,
        text_html=str(detail.get("text_html") or "") or None,
        text_plain=str(detail.get("text_plain") or "") or None,
        from_name=str(detail.get("from_name") or "") or None,
        from_address=str(detail.get("from_address") or "") or None,
        to_address_all=str(detail.get("to_address_all") or "") or None,
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
        client = _build_message_client()
        target_store = client.resolve_store(external_store_id=external_store_id)
        scoped_message = get_message(
            db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
        )
        analysis_text: str | None = None
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
                client=client,
                store_name=target_store.store_name,
                sid=target_store.sid,
            ),
            force_regenerate=payload.force_regenerate,
            allow_auto_send=payload.allow_auto_send,
            analysis_text=analysis_text,
        )
    except MessagingAPIError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LLMGenerationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
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
    """Send approved reply for scoped message via Lingxing API."""

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
        client = _build_message_client()
        target_store = client.resolve_store(external_store_id=external_store_id)
        message, sp_result = send_approved_reply(
            db=db,
            message_id=message_id,
            tenant_id=current_user.tenant_id,
            store_id=internal_store_id,
            send_service=MessageSendService(
                client=client,
                store_name=target_store.store_name,
                sid=target_store.sid,
            ),
        )
    except MessagingAPIError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
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
    """Convenience endpoint: approve AI reply then send through Lingxing API."""

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

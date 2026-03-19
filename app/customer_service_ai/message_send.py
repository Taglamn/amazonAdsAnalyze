from __future__ import annotations

from typing import Any

from .amazon_reply import reply_to_amazon_email
from .mail_client import MailClientError, MailTransportSettings, get_email_by_message_id
from .message_meta_store import MessageMetaStore, message_meta_store


class MessageSendService:
    def __init__(
        self,
        *,
        tenant_id: int,
        store_id: int,
        meta_store: MessageMetaStore | None = None,
        transport_settings: MailTransportSettings | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.store_id = store_id
        self.meta_store = meta_store or message_meta_store
        self.transport_settings = transport_settings

    def send(
        self,
        conversation_id: str,
        reply: str,
        attachments: list[dict[str, Any]] | None = None,
        enforce_compliance: bool = True,
    ) -> dict[str, Any]:
        detail = self.meta_store.get_message_detail(
            tenant_id=self.tenant_id,
            store_id=self.store_id,
            conversation_id=conversation_id,
        )
        if detail is None:
            try:
                fetched = get_email_by_message_id(conversation_id, settings=self.transport_settings)
            except MailClientError as exc:
                raise RuntimeError(str(exc)) from exc
            if fetched is not None:
                detail = dict(fetched)
                self.meta_store.upsert_message_detail(
                    tenant_id=self.tenant_id,
                    store_id=self.store_id,
                    conversation_id=conversation_id,
                    detail=detail,
                )

        if detail is None:
            raise RuntimeError(f"Original email not found for conversation_id={conversation_id}")

        reply_result = reply_to_amazon_email(
            {
                "subject": str(detail.get("subject") or ""),
                "reply-to": str(detail.get("reply_to") or detail.get("reply-to") or ""),
                "message-id": str(detail.get("message_id") or detail.get("message-id") or conversation_id),
            },
            reply,
            transport_settings=self.transport_settings,
            enforce_compliance=enforce_compliance,
            attachments=attachments or [],
        )
        if not bool(reply_result.get("success")):
            raise RuntimeError(str(reply_result.get("message") or "Failed to send Amazon reply"))
        return reply_result

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from .db import BuyerMessage, MessageStatus
from .sp_api import IncomingBuyerMessage


@dataclass(frozen=True)
class StoreMessagesResult:
    fetched_count: int
    created_count: int
    new_message_ids: list[int]


class MessageStorageService:
    """Persist incoming buyer messages with tenant/store scoping."""

    def store_messages(
        self,
        db: Session,
        incoming: list[IncomingBuyerMessage],
        *,
        tenant_id: int,
        store_id: int,
    ) -> StoreMessagesResult:
        """Insert new messages and return counts + new record ids."""

        created_count = 0
        new_message_ids: list[int] = []
        for item in incoming:
            message, is_created = self._get_or_create_message(
                db=db,
                incoming=item,
                tenant_id=tenant_id,
                store_id=store_id,
            )
            if is_created:
                created_count += 1
                new_message_ids.append(message.id)

        return StoreMessagesResult(
            fetched_count=len(incoming),
            created_count=created_count,
            new_message_ids=new_message_ids,
        )

    def _get_or_create_message(
        self,
        db: Session,
        incoming: IncomingBuyerMessage,
        *,
        tenant_id: int,
        store_id: int,
    ) -> tuple[BuyerMessage, bool]:
        """Upsert-like lookup by scoped uniqueness key."""

        stmt = select(BuyerMessage).where(
            and_(
                BuyerMessage.tenant_id == tenant_id,
                BuyerMessage.store_id == store_id,
                BuyerMessage.conversation_id == incoming.conversation_id,
                BuyerMessage.buyer_message == incoming.buyer_message,
            )
        )
        existing = db.execute(stmt).scalar_one_or_none()
        if existing is not None:
            return existing, False

        message = BuyerMessage(
            tenant_id=tenant_id,
            store_id=store_id,
            conversation_id=incoming.conversation_id,
            buyer_message=incoming.buyer_message,
            category="other",
            sentiment="neutral",
            risk_level="medium",
            product_issue=None,
            ai_reply=None,
            final_reply=None,
            status=MessageStatus.NEW.value,
        )
        db.add(message)
        db.commit()
        db.refresh(message)
        return message, True

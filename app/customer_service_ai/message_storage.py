from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import BuyerMessage, MessageStatus
from .sp_api import IncomingBuyerMessage


@dataclass(frozen=True)
class StoreMessagesResult:
    fetched_count: int
    created_count: int
    new_message_ids: list[int]


class MessageStorageService:
    def store_messages(self, db: Session, incoming: list[IncomingBuyerMessage]) -> StoreMessagesResult:
        created_count = 0
        new_message_ids: list[int] = []
        for item in incoming:
            message, is_created = self._get_or_create_message(db=db, incoming=item)
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
    ) -> tuple[BuyerMessage, bool]:
        stmt = select(BuyerMessage).where(
            BuyerMessage.conversation_id == incoming.conversation_id,
            BuyerMessage.buyer_message == incoming.buyer_message,
        )
        existing = db.execute(stmt).scalar_one_or_none()
        if existing is not None:
            return existing, False

        message = BuyerMessage(
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

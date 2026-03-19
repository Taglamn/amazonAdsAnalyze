from __future__ import annotations

from .amazon_message_filter import filter_amazon_messages
from .mail_client import get_unread_emails
from .message_types import IncomingBuyerMessage


class MessageSyncService:
    """Fetch and normalize buyer messages from mailbox (IMAP)."""

    def fetch_messages(self) -> list[IncomingBuyerMessage]:
        raw_emails = get_unread_emails()
        filtered = filter_amazon_messages(raw_emails)
        incoming: list[IncomingBuyerMessage] = []

        for item in filtered:
            message_id = str(item.get("message_id") or "").strip()
            if not message_id:
                continue
            clean_body = str(item.get("clean_body") or "").strip()
            if not clean_body:
                continue

            incoming.append(
                IncomingBuyerMessage(
                    conversation_id=message_id,
                    buyer_message=clean_body,
                    mailbox_flag="receive",
                    subject=str(item.get("subject") or ""),
                    from_address=str(item.get("from") or ""),
                    reply_to=str(item.get("reply_to") or item.get("buyer_email") or ""),
                    message_id=message_id,
                    raw_data={
                        "headers": item.get("headers") or {},
                        "subject": str(item.get("subject") or ""),
                        "from": str(item.get("from") or ""),
                        "reply_to": str(item.get("reply_to") or item.get("buyer_email") or ""),
                        "body": str(item.get("body") or ""),
                        "clean_body": clean_body,
                        "buyer_message_id": str(item.get("buyer_message_id") or ""),
                    },
                )
            )
        return incoming

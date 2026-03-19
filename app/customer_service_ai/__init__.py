from __future__ import annotations

from .amazon_message_filter import AmazonFilterBlacklist, filter_amazon_messages
from .amazon_reply import reply_to_amazon_email
from .mail_client import (
    MailClientError,
    get_unread_emails,
    parse_email,
    send_email,
)

__all__ = [
    "AmazonFilterBlacklist",
    "MailClientError",
    "filter_amazon_messages",
    "get_unread_emails",
    "parse_email",
    "reply_to_amazon_email",
    "send_email",
]

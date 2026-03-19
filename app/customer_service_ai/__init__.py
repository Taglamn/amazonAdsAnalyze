from __future__ import annotations

from .mail_client import (
    MailClientError,
    get_unread_emails,
    parse_email,
    send_email,
)

__all__ = [
    "MailClientError",
    "get_unread_emails",
    "parse_email",
    "send_email",
]

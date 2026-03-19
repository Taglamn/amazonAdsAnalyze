from __future__ import annotations

import html
import re
from typing import Any

from .db import BuyerMessage, MessageStatus

_MAX_ANALYSIS_TEXT_CHARS = 12000


def build_message_analysis_text(
    *,
    message: BuyerMessage,
    detail: dict[str, Any] | None = None,
) -> str | None:
    """Build analysis text from stored message + optional IMAP detail."""

    if message.status in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value}:
        return None

    detail = detail or {}

    subject = str(detail.get("subject") or "").strip()
    text_plain = str(detail.get("text_plain") or detail.get("clean_body") or "").strip()
    text_html = str(detail.get("text_html") or "").strip()
    content = text_plain or _html_to_text(text_html) or str(message.buyer_message or "").strip()

    parts: list[str] = []
    if subject:
        parts.append(f"Subject: {subject}")
    if content:
        parts.append(f"Body:\n{content}")

    merged = "\n\n".join(parts).strip()
    if len(merged) > _MAX_ANALYSIS_TEXT_CHARS:
        merged = merged[:_MAX_ANALYSIS_TEXT_CHARS].rstrip()
    return merged or None


def _html_to_text(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    text = re.sub(r"(?i)</p>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

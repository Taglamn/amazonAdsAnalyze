from __future__ import annotations

import html
import re

from .db import BuyerMessage, MessageStatus
from .sp_api import LingxingBoundStore, LingxingMessagingClient

_MAX_ANALYSIS_TEXT_CHARS = 12000


def build_message_analysis_text(
    *,
    message: BuyerMessage,
    client: LingxingMessagingClient,
    target_store: LingxingBoundStore,
) -> str | None:
    """Build analysis text from Lingxing mail detail for AI pipeline input."""

    if message.status in {MessageStatus.SENT.value, MessageStatus.AUTO_SENT.value}:
        return None

    conversation_id = str(message.conversation_id or "").strip()
    if not conversation_id:
        return None

    detail = client.fetch_message_detail(
        webmail_uuid=conversation_id,
        store_name=target_store.store_name,
        sid=target_store.sid,
        email=target_store.email,
        external_store_id=target_store.external_store_id,
    )

    subject = str(detail.get("subject") or "").strip()
    text_plain = str(detail.get("text_plain") or "").strip()
    text_html = str(detail.get("text_html") or "").strip()
    content = text_plain or _html_to_text(text_html)

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

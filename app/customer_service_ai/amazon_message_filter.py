from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from email.utils import parseaddr
from typing import Any, Iterable

logger = logging.getLogger(__name__)

AMAZON_SENDER_KEYWORDS = ("amazon.com", "marketplace.amazon.com")
AMAZON_SUBJECT_KEYWORDS = ("buyer-seller messaging", "you have received a new message")
AMAZON_HEADER_HINTS = ("x-amazon", "amazon")
BODY_CUTOFF_MARKERS = (
    "-----original message-----",
    "from:",
    "sent:",
    "to:",
    "subject:",
)


@dataclass(frozen=True)
class AmazonFilterBlacklist:
    """Extensible blacklist config for sender/subject/header match."""

    sender_contains: tuple[str, ...] = field(default_factory=tuple)
    subject_contains: tuple[str, ...] = field(default_factory=tuple)
    header_contains: tuple[str, ...] = field(default_factory=tuple)


def filter_amazon_messages(
    email_list: Iterable[dict[str, Any]],
    *,
    blacklist: AmazonFilterBlacklist | None = None,
) -> list[dict[str, str]]:
    """
    Keep only Amazon Buyer-Seller Messaging emails.

    Returned fields:
    - subject
    - buyer_email (reply-to)
    - message_id
    - clean_body
    - buyer_message_id (optional when recognized)
    """

    blocked = blacklist or AmazonFilterBlacklist()
    filtered: list[dict[str, str]] = []

    for item in email_list:
        sender = _to_text(item.get("from"))
        subject = _to_text(item.get("subject"))
        reply_to = _to_text(item.get("reply_to"))
        body = _to_text(item.get("body"))
        headers = _normalize_headers(item.get("headers"), fallback_item=item)
        message_id = _to_text(item.get("message_id")) or _lookup_header(headers, "message-id")

        reject_reason = _reject_reason(
            sender=sender,
            subject=subject,
            headers=headers,
            blocked=blocked,
        )
        if reject_reason:
            _log_filtered_email(
                reason=reject_reason,
                sender=sender,
                subject=subject,
                message_id=message_id,
            )
            continue

        buyer_message_id = _extract_buyer_message_id(body)
        clean_body = _clean_body(body)
        normalized = {
            "subject": subject,
            "buyer_email": _extract_email_or_raw(reply_to),
            "message_id": message_id,
            "clean_body": clean_body,
        }
        if buyer_message_id:
            normalized["buyer_message_id"] = buyer_message_id
        filtered.append(normalized)

    return filtered


def _reject_reason(
    *,
    sender: str,
    subject: str,
    headers: dict[str, str],
    blocked: AmazonFilterBlacklist,
) -> str:
    sender_l = sender.lower()
    subject_l = subject.lower()

    if _matches_any(sender_l, blocked.sender_contains):
        return "blacklist_sender"
    if _matches_any(subject_l, blocked.subject_contains):
        return "blacklist_subject"
    if _headers_match(headers, blocked.header_contains):
        return "blacklist_header"

    if not _matches_any(sender_l, AMAZON_SENDER_KEYWORDS):
        return "sender_not_amazon"
    if not _matches_any(subject_l, AMAZON_SUBJECT_KEYWORDS):
        return "subject_not_buyer_seller"
    if not _has_amazon_header(headers):
        return "missing_amazon_header"
    return ""


def _normalize_headers(raw_headers: Any, *, fallback_item: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}

    if isinstance(raw_headers, dict):
        for key, value in raw_headers.items():
            k = _to_text(key).lower()
            v = _to_text(value)
            if k:
                headers[k] = v
    elif isinstance(raw_headers, str):
        for line in raw_headers.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            k = key.strip().lower()
            if k:
                headers[k] = value.strip()

    # Fallback for flattened item fields (e.g. x-amazon-trace-id at top level).
    for key, value in fallback_item.items():
        if not isinstance(key, str):
            continue
        key_l = key.strip().lower()
        if key_l.startswith("x-amazon") or key_l.startswith("x-amz"):
            headers[key_l] = _to_text(value)
        if key_l == "message-id" and "message-id" not in headers:
            headers["message-id"] = _to_text(value)

    return headers


def _lookup_header(headers: dict[str, str], key: str) -> str:
    return _to_text(headers.get(key.lower()))


def _has_amazon_header(headers: dict[str, str]) -> bool:
    for key, value in headers.items():
        k = _to_text(key).lower()
        v = _to_text(value).lower()
        if k.startswith("x-amazon") or k.startswith("x-amz"):
            return True
        if "amazon" in k:
            return True
        # Only trust value keyword when it belongs to an explicit custom x-* header.
        if k.startswith("x-") and _matches_any(v, AMAZON_HEADER_HINTS):
            return True
    return False


def _headers_match(headers: dict[str, str], rules: Iterable[str]) -> bool:
    rule_list = [item.lower().strip() for item in rules if item and str(item).strip()]
    if not rule_list:
        return False
    for key, value in headers.items():
        check = f"{key}:{value}".lower()
        if _matches_any(check, rule_list):
            return True
    return False


def _extract_buyer_message_id(body: str) -> str:
    if not body:
        return ""
    patterns = (
        r"buyer\s*message\s*id\s*[:：]\s*([A-Za-z0-9_\-\.]+)",
        r"buyer-message-id\s*[:：]\s*([A-Za-z0-9_\-\.]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, body, flags=re.IGNORECASE)
        if match:
            return _to_text(match.group(1))
    return ""


def _clean_body(body: str) -> str:
    if not body:
        return ""

    text = body.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in text.split("\n"):
        if line.strip().startswith(">"):
            continue
        lines.append(line)
    text = "\n".join(lines)

    lower = text.lower()
    for marker in BODY_CUTOFF_MARKERS:
        idx = lower.find(marker)
        if idx > 0:
            text = text[:idx]
            break

    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_email_or_raw(value: str) -> str:
    _, parsed = parseaddr(value)
    parsed = _to_text(parsed)
    if parsed:
        return parsed
    return _to_text(value)


def _matches_any(text: str, terms: Iterable[str]) -> bool:
    value = _to_text(text).lower()
    for term in terms:
        normalized = _to_text(term).lower()
        if normalized and normalized in value:
            return True
    return False


def _log_filtered_email(*, reason: str, sender: str, subject: str, message_id: str) -> None:
    logger.info(
        "amazon_mail_filtered reason=%s sender=%s subject=%s message_id=%s",
        reason,
        sender,
        subject,
        message_id,
    )


def _to_text(value: Any) -> str:
    return str(value or "").strip()

from __future__ import annotations

import logging
import re
from typing import Any

from .mail_client import MailClientError, MailTransportSettings, send_email

logger = logging.getLogger(__name__)

_AMAZON_RELAY_DOMAINS = ("amazon.com", "marketplace.amazon.com")
_SENSITIVE_TERMS = (
    "refund without return",
    "outside amazon",
    "contact me directly",
)
_MARKETING_TERMS = (
    "discount",
    "coupon",
    "promo",
    "promotion",
    "follow our store",
    "五星好评",
    "好评返现",
    "优惠码",
)


def reply_to_amazon_email(
    original_email: dict[str, Any],
    reply_text: str,
    *,
    transport_settings: MailTransportSettings | None = None,
    enforce_compliance: bool = True,
) -> dict[str, Any]:
    """
    Reply through Amazon Buyer-Seller Messaging relay address.

    Rules enforced:
    - send only to original reply-to relay address
    - preserve thread headers using original message-id
    - subject must be "Re: <subject>"
    - plain-text only body
    - compliance checks block risky content
    """

    reply_to = _get_first(original_email, "reply-to", "reply_to")
    subject = _get_first(original_email, "subject")
    message_id = _get_first(original_email, "message-id", "message_id")
    body = str(reply_text or "").strip()

    if not reply_to:
        return _blocked("missing_reply_to", "Missing reply-to address from original email.")
    if not _is_amazon_relay_address(reply_to):
        return _blocked("invalid_reply_to", "Reply-to is not an Amazon relay address.")
    if not message_id:
        return _blocked("missing_message_id", "Missing original message-id for threading.")
    if not body:
        return _blocked("empty_reply", "Reply text cannot be empty.")

    compliance = _validate_reply_compliance(body)
    if not compliance["ok"] and enforce_compliance:
        return _blocked("compliance_violation", compliance["message"], violations=compliance["violations"])
    if not compliance["ok"] and not enforce_compliance:
        logger.warning(
            "amazon_reply_compliance_bypassed violations=%s",
            compliance["violations"],
        )

    headers = {
        "In-Reply-To": message_id,
        "References": message_id,
    }
    final_subject = _build_reply_subject(subject)

    try:
        sent_message_id = send_email(
            to_address=reply_to,
            subject=final_subject,
            body=_build_plain_text_reply(body),
            headers=headers,
            settings=transport_settings,
        )
    except MailClientError as exc:
        logger.warning("amazon_reply_send_failed reason=%s", exc)
        return {
            "success": False,
            "blocked": False,
            "error_code": "send_failed",
            "message": str(exc),
        }

    return {
        "success": True,
        "blocked": False,
        "to": reply_to,
        "subject": final_subject,
        "message_id": sent_message_id,
        "compliance_bypassed": bool(not compliance["ok"] and not enforce_compliance),
        "compliance_violations": compliance["violations"],
    }


def _build_reply_subject(subject: str) -> str:
    normalized = str(subject or "").strip()
    if not normalized:
        return "Re:"
    if normalized.lower().startswith("re:"):
        return normalized
    return f"Re: {normalized}"


def _build_plain_text_reply(reply_text: str) -> str:
    # Placeholder for AI-generated content integration.
    # Caller should pass generated text in `reply_text`.
    text = str(reply_text or "").replace("\r\n", "\n").replace("\r", "\n")
    return text.strip()


def _validate_reply_compliance(text: str) -> dict[str, Any]:
    violations: list[str] = []
    lowered = text.lower()

    if re.search(r"(https?://|www\.)", text, flags=re.IGNORECASE):
        violations.append("external_link")
    if re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text):
        violations.append("email_contact")
    if re.search(r"(?:\+?\d[\d\s\-\(\)]{7,}\d)", text):
        violations.append("phone_contact")

    for term in _MARKETING_TERMS:
        if term and term.lower() in lowered:
            violations.append("marketing_content")
            break

    for term in _SENSITIVE_TERMS:
        if term and term.lower() in lowered:
            violations.append("sensitive_term")
            break

    if violations:
        return {
            "ok": False,
            "violations": sorted(set(violations)),
            "message": f"Reply blocked by compliance checks: {', '.join(sorted(set(violations)))}",
        }
    return {"ok": True, "violations": [], "message": ""}


def _is_amazon_relay_address(value: str) -> bool:
    email = _extract_email(value).lower()
    if not email or "@" not in email:
        return False
    domain = email.split("@", 1)[1]
    return any(domain.endswith(item) for item in _AMAZON_RELAY_DOMAINS)


def _extract_email(value: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"<([^>]+)>", text)
    if match:
        return match.group(1).strip()
    return text


def _get_first(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key in payload:
            value = str(payload.get(key) or "").strip()
            if value:
                return value
    return ""


def _blocked(code: str, message: str, *, violations: list[str] | None = None) -> dict[str, Any]:
    logger.info("amazon_reply_blocked code=%s message=%s violations=%s", code, message, violations or [])
    return {
        "success": False,
        "blocked": True,
        "error_code": code,
        "message": message,
        "violations": violations or [],
    }

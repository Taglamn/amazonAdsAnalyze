from __future__ import annotations

import html
import imaplib
import os
import re
import ssl
import smtplib
from dataclasses import dataclass
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage, Message
from email.parser import BytesParser, Parser
from email.utils import make_msgid
from typing import Any


class MailClientError(RuntimeError):
    """Raised when IMAP/SMTP operations fail."""


@dataclass(frozen=True)
class MailTransportSettings:
    """Transport settings loaded from environment variables."""

    username: str
    password: str
    imap_host: str
    imap_port: int
    imap_mailbox: str
    smtp_host: str
    smtp_port: int
    smtp_use_ssl: bool
    smtp_starttls: bool
    timeout_seconds: int


def load_mail_transport_settings() -> MailTransportSettings:
    """Load IMAP/SMTP settings from environment."""

    username = os.getenv("CUSTOMER_SERVICE_EMAIL_USERNAME", "").strip()
    password = os.getenv("CUSTOMER_SERVICE_EMAIL_PASSWORD", "").strip()
    imap_host = os.getenv("CUSTOMER_SERVICE_EMAIL_IMAP_HOST", "").strip()
    smtp_host = os.getenv("CUSTOMER_SERVICE_EMAIL_SMTP_HOST", "").strip()

    if not username or not password:
        raise MailClientError(
            "Missing email credentials. Please configure CUSTOMER_SERVICE_EMAIL_USERNAME and "
            "CUSTOMER_SERVICE_EMAIL_PASSWORD."
        )
    if not imap_host:
        raise MailClientError("Missing CUSTOMER_SERVICE_EMAIL_IMAP_HOST.")
    if not smtp_host:
        raise MailClientError("Missing CUSTOMER_SERVICE_EMAIL_SMTP_HOST.")

    return MailTransportSettings(
        username=username,
        password=password,
        imap_host=imap_host,
        imap_port=_int_env("CUSTOMER_SERVICE_EMAIL_IMAP_PORT", 993, minimum=1),
        imap_mailbox=os.getenv("CUSTOMER_SERVICE_EMAIL_IMAP_MAILBOX", "INBOX").strip() or "INBOX",
        smtp_host=smtp_host,
        smtp_port=_int_env("CUSTOMER_SERVICE_EMAIL_SMTP_PORT", 465, minimum=1),
        smtp_use_ssl=_bool_env("CUSTOMER_SERVICE_EMAIL_SMTP_USE_SSL", True),
        smtp_starttls=_bool_env("CUSTOMER_SERVICE_EMAIL_SMTP_STARTTLS", False),
        timeout_seconds=_int_env("CUSTOMER_SERVICE_EMAIL_TIMEOUT_SECONDS", 30, minimum=1),
    )


def parse_email(raw_email: bytes | str) -> dict[str, Any]:
    """Parse raw RFC822 email bytes/string into normalized fields."""

    message = _parse_raw_message(raw_email)
    headers: dict[str, str] = {}
    for key, value in message.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        headers[normalized_key] = _decode_header_value(value)

    return {
        "subject": _decode_header_value(message.get("Subject")),
        "from": _decode_header_value(message.get("From")),
        "reply_to": _decode_header_value(message.get("Reply-To")),
        "reply-to": _decode_header_value(message.get("Reply-To")),
        "message_id": _decode_header_value(message.get("Message-ID")),
        "message-id": _decode_header_value(message.get("Message-ID")),
        "to": _decode_header_value(message.get("To")),
        "cc": _decode_header_value(message.get("Cc")),
        "bcc": _decode_header_value(message.get("Bcc")),
        "date": _decode_header_value(message.get("Date")),
        "body": _extract_plain_text_body(message),
        "text_plain": _extract_plain_text_body(message),
        "text_html": _extract_html_body(message),
        "attachments": _extract_attachments(message),
        "headers": headers,
    }


def get_unread_emails_with_settings(settings: MailTransportSettings) -> list[dict[str, Any]]:
    """Fetch unread emails using explicit transport settings."""

    records: list[dict[str, Any]] = []
    client: imaplib.IMAP4_SSL | None = None
    try:
        client = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port)
        client.login(settings.username, settings.password)
        status, _ = client.select(settings.imap_mailbox)
        if status != "OK":
            raise MailClientError(f"Failed to select mailbox: {settings.imap_mailbox}")

        status, data = client.search(None, "UNSEEN")
        if status != "OK":
            raise MailClientError("Failed to search unread emails.")

        message_ids = data[0].split() if data and data[0] else []
        for message_id in message_ids:
            fetch_status, payload = client.fetch(message_id, "(BODY.PEEK[])")
            if fetch_status != "OK":
                continue
            parsed_ok = False
            for part in payload:
                if isinstance(part, tuple) and len(part) > 1:
                    records.append(parse_email(part[1]))
                    parsed_ok = True
                    break
            if parsed_ok:
                try:
                    client.store(message_id, "+FLAGS", "\\Seen")
                except Exception:
                    pass
    except imaplib.IMAP4.error as exc:
        raise MailClientError(f"IMAP operation failed: {exc}") from exc
    except OSError as exc:
        raise MailClientError(f"IMAP network error: {exc}") from exc
    finally:
        _safe_imap_logout(client)
    return records


def get_unread_emails(settings: MailTransportSettings | None = None) -> list[dict[str, Any]]:
    """Fetch unread emails from IMAP mailbox using UNSEEN query."""

    effective = settings or load_mail_transport_settings()
    return get_unread_emails_with_settings(effective)


def test_mail_server_login(settings: MailTransportSettings) -> dict[str, Any]:
    """Test IMAP/SMTP login flow with provided transport settings."""

    client: imaplib.IMAP4_SSL | None = None
    try:
        client = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port)
        client.login(settings.username, settings.password)
        status, _ = client.select(settings.imap_mailbox)
        if status != "OK":
            raise MailClientError(f"Failed to select mailbox: {settings.imap_mailbox}")
    except imaplib.IMAP4.error as exc:
        raise MailClientError(f"IMAP login test failed: {exc}") from exc
    except OSError as exc:
        raise MailClientError(f"IMAP network test failed: {exc}") from exc
    finally:
        _safe_imap_logout(client)

    _smtp_test_login_with_fallback(settings)

    return {
        "imap_ok": True,
        "smtp_ok": True,
        "message": "IMAP/SMTP login test passed.",
    }


def get_email_by_message_id(message_id: str, settings: MailTransportSettings | None = None) -> dict[str, Any] | None:
    """Fetch one email from mailbox using Message-ID header lookup."""

    settings = settings or load_mail_transport_settings()
    needle = str(message_id or "").strip()
    if not needle:
        return None

    candidates = [needle]
    if needle.startswith("<") and needle.endswith(">"):
        candidates.append(needle[1:-1])
    else:
        candidates.append(f"<{needle}>")

    client: imaplib.IMAP4_SSL | None = None
    try:
        client = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port)
        client.login(settings.username, settings.password)
        status, _ = client.select(settings.imap_mailbox)
        if status != "OK":
            raise MailClientError(f"Failed to select mailbox: {settings.imap_mailbox}")

        for candidate in candidates:
            search_status, data = client.search(None, "HEADER", "Message-ID", candidate)
            if search_status != "OK":
                continue
            ids = data[0].split() if data and data[0] else []
            if not ids:
                continue
            fetch_status, payload = client.fetch(ids[-1], "(BODY.PEEK[])")
            if fetch_status != "OK":
                continue
            for part in payload:
                if isinstance(part, tuple) and len(part) > 1:
                    return parse_email(part[1])
    except imaplib.IMAP4.error as exc:
        raise MailClientError(f"IMAP operation failed: {exc}") from exc
    except OSError as exc:
        raise MailClientError(f"IMAP network error: {exc}") from exc
    finally:
        _safe_imap_logout(client)

    return None


def send_email(
    to_address: str,
    subject: str,
    body: str,
    headers: dict[str, Any] | None = None,
    settings: MailTransportSettings | None = None,
) -> str:
    """Send an email via SMTP and return the message-id."""

    settings = settings or load_mail_transport_settings()
    if not to_address.strip():
        raise MailClientError("to_address cannot be empty")

    message = EmailMessage()
    message["From"] = settings.username
    message["To"] = to_address.strip()
    message["Subject"] = subject
    message["Message-ID"] = make_msgid()

    for key, value in (headers or {}).items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        if normalized_key.lower() == "message-id":
            message.replace_header("Message-ID", str(value))
            continue
        if normalized_key in message:
            message.replace_header(normalized_key, str(value))
        else:
            message[normalized_key] = str(value)

    message.set_content(body or "")

    _smtp_send_with_fallback(settings, message)

    return str(message["Message-ID"] or "").strip()


def _smtp_test_login_with_fallback(settings: MailTransportSettings) -> None:
    attempts = _build_smtp_attempts(settings)
    failures: list[str] = []
    for mode, use_ssl, use_starttls in attempts:
        try:
            _smtp_login_once(settings=settings, use_ssl=use_ssl, use_starttls=use_starttls)
            return
        except MailClientError as exc:
            failures.append(f"{mode}: {exc}")

    detail = "; ".join(failures) if failures else "unknown error"
    raise MailClientError(f"SMTP login test failed for all strategies ({detail})")


def _smtp_send_with_fallback(settings: MailTransportSettings, message: EmailMessage) -> None:
    attempts = _build_smtp_attempts(settings)
    failures: list[str] = []
    for mode, use_ssl, use_starttls in attempts:
        try:
            _smtp_send_once(settings, message, use_ssl=use_ssl, use_starttls=use_starttls)
            return
        except MailClientError as exc:
            failures.append(f"{mode}: {exc}")

    detail = "; ".join(failures) if failures else "unknown error"
    raise MailClientError(f"SMTP operation failed for all strategies ({detail})")


def _build_smtp_attempts(settings: MailTransportSettings) -> list[tuple[str, bool, bool]]:
    attempts: list[tuple[str, bool, bool]] = []
    seen: set[tuple[bool, bool]] = set()

    def add(name: str, use_ssl: bool, use_starttls: bool) -> None:
        key = (use_ssl, use_starttls)
        if key in seen:
            return
        seen.add(key)
        attempts.append((name, use_ssl, use_starttls))

    if settings.smtp_use_ssl:
        add("ssl", True, False)
        add("starttls", False, True)
        add("plain", False, False)
        return attempts

    if settings.smtp_starttls:
        add("starttls", False, True)
        add("ssl", True, False)
        add("plain", False, False)
        return attempts

    add("plain", False, False)
    if settings.smtp_port == 465:
        add("ssl", True, False)
        add("starttls", False, True)
    elif settings.smtp_port == 587:
        add("starttls", False, True)
        add("ssl", True, False)
    else:
        add("ssl", True, False)
        add("starttls", False, True)
    return attempts


def _smtp_login_once(*, settings: MailTransportSettings, use_ssl: bool, use_starttls: bool) -> None:
    try:
        if use_ssl:
            with smtplib.SMTP_SSL(
                settings.smtp_host,
                settings.smtp_port,
                timeout=settings.timeout_seconds,
            ) as client:
                client.ehlo()
                client.login(settings.username, settings.password)
            return

        with smtplib.SMTP(
            settings.smtp_host,
            settings.smtp_port,
            timeout=settings.timeout_seconds,
        ) as client:
            client.ehlo()
            if use_starttls:
                client.starttls(context=ssl.create_default_context())
                client.ehlo()
            client.login(settings.username, settings.password)
    except smtplib.SMTPException as exc:
        raise MailClientError(str(exc)) from exc
    except OSError as exc:
        raise MailClientError(str(exc)) from exc


def _smtp_send_once(
    settings: MailTransportSettings,
    message: EmailMessage,
    *,
    use_ssl: bool,
    use_starttls: bool,
) -> None:
    try:
        if use_ssl:
            with smtplib.SMTP_SSL(
                settings.smtp_host,
                settings.smtp_port,
                timeout=settings.timeout_seconds,
            ) as client:
                client.ehlo()
                client.login(settings.username, settings.password)
                client.send_message(message)
            return

        with smtplib.SMTP(
            settings.smtp_host,
            settings.smtp_port,
            timeout=settings.timeout_seconds,
        ) as client:
            client.ehlo()
            if use_starttls:
                client.starttls(context=ssl.create_default_context())
                client.ehlo()
            client.login(settings.username, settings.password)
            client.send_message(message)
    except smtplib.SMTPException as exc:
        raise MailClientError(str(exc)) from exc
    except OSError as exc:
        raise MailClientError(str(exc)) from exc


def _safe_imap_logout(client: imaplib.IMAP4_SSL | None) -> None:
    if client is None:
        return
    try:
        client.logout()
    except Exception:
        # Some servers close TCP directly and cause EOF on LOGOUT.
        # Treat this as non-fatal because IMAP operations already succeeded.
        pass


def _parse_raw_message(raw_email: bytes | str) -> Message:
    if isinstance(raw_email, bytes):
        return BytesParser(policy=policy.default).parsebytes(raw_email)
    if isinstance(raw_email, str):
        return Parser(policy=policy.default).parsestr(raw_email)
    raise TypeError("raw_email must be bytes or str")


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value))).strip()
    except Exception:
        return str(value).strip()


def _extract_plain_text_body(message: Message) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    if message.is_multipart():
        for part in message.walk():
            disposition = str(part.get_content_disposition() or "").lower()
            if disposition == "attachment":
                continue
            content_type = str(part.get_content_type() or "").lower()
            if content_type == "text/plain":
                text = _extract_part_text(part)
                if text:
                    plain_parts.append(text)
            elif content_type == "text/html":
                text = _extract_part_text(part)
                if text:
                    html_parts.append(text)
    else:
        content_type = str(message.get_content_type() or "").lower()
        text = _extract_part_text(message)
        if content_type == "text/html" and text:
            html_parts.append(text)
        elif text:
            plain_parts.append(text)

    if plain_parts:
        return "\n".join(item.strip() for item in plain_parts if item.strip()).strip()
    if html_parts:
        return _html_to_text("\n".join(html_parts))
    return ""


def _extract_html_body(message: Message) -> str:
    html_parts: list[str] = []
    if message.is_multipart():
        for part in message.walk():
            disposition = str(part.get_content_disposition() or "").lower()
            if disposition == "attachment":
                continue
            content_type = str(part.get_content_type() or "").lower()
            if content_type == "text/html":
                text = _extract_part_text(part)
                if text:
                    html_parts.append(text)
    else:
        if str(message.get_content_type() or "").lower() == "text/html":
            text = _extract_part_text(message)
            if text:
                html_parts.append(text)
    return "\n".join(item.strip() for item in html_parts if item.strip()).strip()


def _extract_attachments(message: Message) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for part in message.walk():
        disposition = str(part.get_content_disposition() or "").lower()
        if disposition != "attachment":
            continue
        filename = _decode_header_value(part.get_filename())
        payload = part.get_payload(decode=True) or b""
        attachments.append(
            {
                "name": filename,
                "content_type": str(part.get_content_type() or ""),
                "size": len(payload),
            }
        )
    return attachments


def _extract_part_text(part: Message) -> str:
    try:
        content = part.get_content()
        if isinstance(content, str):
            return content
    except Exception:
        pass

    payload = part.get_payload(decode=True)
    if payload is None:
        raw_payload = part.get_payload()
        return str(raw_payload or "")

    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def _html_to_text(value: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", value)
    text = re.sub(r"(?s)<br\s*/?>", "\n", text)
    text = re.sub(r"(?s)</p>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(minimum, value)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}

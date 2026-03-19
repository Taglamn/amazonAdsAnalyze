from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from .mail_client import MailClientError, MailTransportSettings

_DEFAULT_SETTINGS_DIR = Path(__file__).resolve().parent.parent / "data" / "customer_service_mail_settings"


class UserMailSettingsStore:
    """Persist per-user/per-store IMAP/SMTP settings in local JSON files."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or _DEFAULT_SETTINGS_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def get_user_settings(self, *, tenant_id: int, user_id: int, store_id: int | None = None) -> dict[str, Any]:
        with self._lock:
            data = self._read_locked(tenant_id=tenant_id, user_id=user_id, store_id=store_id)
        return {
            "username": str(data.get("username") or ""),
            "imap_host": str(data.get("imap_host") or ""),
            "imap_port": int(data.get("imap_port") or 993),
            "imap_mailbox": str(data.get("imap_mailbox") or "INBOX"),
            "smtp_host": str(data.get("smtp_host") or ""),
            "smtp_port": int(data.get("smtp_port") or 465),
            "smtp_use_ssl": bool(data.get("smtp_use_ssl", True)),
            "smtp_starttls": bool(data.get("smtp_starttls", False)),
            "timeout_seconds": int(data.get("timeout_seconds") or 30),
            "password_set": bool(str(data.get("password") or "").strip()),
            "configured": bool(data),
        }

    def upsert_user_settings(
        self,
        *,
        tenant_id: int,
        user_id: int,
        store_id: int | None = None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            existing = self._read_locked(tenant_id=tenant_id, user_id=user_id, store_id=store_id)
            merged = dict(existing)

            for key in (
                "username",
                "imap_host",
                "imap_mailbox",
                "smtp_host",
            ):
                if key in payload:
                    merged[key] = str(payload.get(key) or "").strip()

            for key, default in (("imap_port", 993), ("smtp_port", 465), ("timeout_seconds", 30)):
                if key in payload and payload.get(key) is not None:
                    try:
                        merged[key] = max(1, int(payload.get(key)))
                    except (TypeError, ValueError):
                        merged[key] = default

            for key, default in (("smtp_use_ssl", True), ("smtp_starttls", False)):
                if key in payload and payload.get(key) is not None:
                    merged[key] = bool(payload.get(key))

            if "password" in payload:
                raw_password = str(payload.get("password") or "")
                if raw_password.strip():
                    merged["password"] = raw_password
                elif not str(existing.get("password") or "").strip():
                    merged["password"] = ""

            self._validate_merged(merged)
            self._write_locked(tenant_id=tenant_id, user_id=user_id, store_id=store_id, data=merged)

        return self.get_user_settings(tenant_id=tenant_id, user_id=user_id, store_id=store_id)

    def resolve_transport_settings_for_user(
        self,
        *,
        tenant_id: int,
        user_id: int,
        store_id: int | None = None,
    ) -> MailTransportSettings:
        with self._lock:
            stored = self._read_locked(tenant_id=tenant_id, user_id=user_id, store_id=store_id)

        username = str(stored.get("username") or os.getenv("CUSTOMER_SERVICE_EMAIL_USERNAME", "")).strip()
        password = str(stored.get("password") or os.getenv("CUSTOMER_SERVICE_EMAIL_PASSWORD", "")).strip()
        imap_host = str(stored.get("imap_host") or os.getenv("CUSTOMER_SERVICE_EMAIL_IMAP_HOST", "")).strip()
        smtp_host = str(stored.get("smtp_host") or os.getenv("CUSTOMER_SERVICE_EMAIL_SMTP_HOST", "")).strip()
        imap_mailbox = str(stored.get("imap_mailbox") or os.getenv("CUSTOMER_SERVICE_EMAIL_IMAP_MAILBOX", "INBOX")).strip() or "INBOX"
        imap_port = _to_int(stored.get("imap_port"), _to_int(os.getenv("CUSTOMER_SERVICE_EMAIL_IMAP_PORT"), 993))
        smtp_port = _to_int(stored.get("smtp_port"), _to_int(os.getenv("CUSTOMER_SERVICE_EMAIL_SMTP_PORT"), 465))
        timeout_seconds = _to_int(
            stored.get("timeout_seconds"),
            _to_int(os.getenv("CUSTOMER_SERVICE_EMAIL_TIMEOUT_SECONDS"), 30),
        )
        smtp_use_ssl = _to_bool(stored.get("smtp_use_ssl"), _to_bool(os.getenv("CUSTOMER_SERVICE_EMAIL_SMTP_USE_SSL"), True))
        smtp_starttls = _to_bool(stored.get("smtp_starttls"), _to_bool(os.getenv("CUSTOMER_SERVICE_EMAIL_SMTP_STARTTLS"), False))

        if not username or not password or not imap_host or not smtp_host:
            raise MailClientError(
                "Mail server settings are incomplete for current scope. "
                "Please configure IMAP/SMTP settings for this store first."
            )

        return self._to_transport_settings(
            username=username,
            password=password,
            imap_host=imap_host,
            imap_port=imap_port,
            imap_mailbox=imap_mailbox,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_use_ssl=bool(smtp_use_ssl),
            smtp_starttls=bool(smtp_starttls),
            timeout_seconds=timeout_seconds,
        )

    def build_transport_settings_for_test(
        self,
        *,
        tenant_id: int,
        user_id: int,
        store_id: int | None = None,
        payload: dict[str, Any],
    ) -> MailTransportSettings:
        """Build test transport settings from payload + existing stored password fallback."""

        with self._lock:
            existing = self._read_locked(tenant_id=tenant_id, user_id=user_id, store_id=store_id)

        merged = dict(existing)
        for key in (
            "username",
            "imap_host",
            "imap_mailbox",
            "smtp_host",
            "imap_port",
            "smtp_port",
            "smtp_use_ssl",
            "smtp_starttls",
            "timeout_seconds",
        ):
            if key in payload:
                merged[key] = payload.get(key)

        if "password" in payload:
            raw_password = str(payload.get("password") or "")
            if raw_password.strip():
                merged["password"] = raw_password
            elif not str(merged.get("password") or "").strip():
                merged["password"] = ""

        self._validate_merged(merged)

        return self._to_transport_settings(
            username=str(merged.get("username") or "").strip(),
            password=str(merged.get("password") or "").strip(),
            imap_host=str(merged.get("imap_host") or "").strip(),
            imap_port=_to_int(merged.get("imap_port"), 993),
            imap_mailbox=str(merged.get("imap_mailbox") or "INBOX").strip() or "INBOX",
            smtp_host=str(merged.get("smtp_host") or "").strip(),
            smtp_port=_to_int(merged.get("smtp_port"), 465),
            smtp_use_ssl=_to_bool(merged.get("smtp_use_ssl"), True),
            smtp_starttls=_to_bool(merged.get("smtp_starttls"), False),
            timeout_seconds=_to_int(merged.get("timeout_seconds"), 30),
        )

    def _validate_merged(self, data: dict[str, Any]) -> None:
        required_fields = ("username", "imap_host", "smtp_host", "imap_mailbox")
        missing = [name for name in required_fields if not str(data.get(name) or "").strip()]
        if missing:
            raise MailClientError(f"Missing required fields: {', '.join(missing)}")
        if not str(data.get("password") or "").strip():
            raise MailClientError("Password is required (or keep existing password by leaving it blank).")

    @staticmethod
    def _to_transport_settings(
        *,
        username: str,
        password: str,
        imap_host: str,
        imap_port: int,
        imap_mailbox: str,
        smtp_host: str,
        smtp_port: int,
        smtp_use_ssl: bool,
        smtp_starttls: bool,
        timeout_seconds: int,
    ) -> MailTransportSettings:
        return MailTransportSettings(
            username=username,
            password=password,
            imap_host=imap_host,
            imap_port=max(1, int(imap_port)),
            imap_mailbox=imap_mailbox,
            smtp_host=smtp_host,
            smtp_port=max(1, int(smtp_port)),
            smtp_use_ssl=bool(smtp_use_ssl),
            smtp_starttls=bool(smtp_starttls),
            timeout_seconds=max(1, int(timeout_seconds)),
        )

    def _path(self, *, tenant_id: int, user_id: int, store_id: int | None = None) -> Path:
        if store_id is None:
            return self.base_dir / f"tenant_{tenant_id}_user_{user_id}.json"
        return self.base_dir / f"tenant_{tenant_id}_user_{user_id}_store_{store_id}.json"

    def _read_json_file(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return data if isinstance(data, dict) else {}

    def _read_locked(self, *, tenant_id: int, user_id: int, store_id: int | None = None) -> dict[str, Any]:
        path = self._path(tenant_id=tenant_id, user_id=user_id, store_id=store_id)
        current = self._read_json_file(path)
        if current:
            return current
        if store_id is not None:
            # Backward compatibility: bootstrap from legacy per-user settings.
            legacy = self._read_json_file(self._path(tenant_id=tenant_id, user_id=user_id))
            if legacy:
                return legacy
        return {}

    def _write_locked(
        self,
        *,
        tenant_id: int,
        user_id: int,
        store_id: int | None = None,
        data: dict[str, Any],
    ) -> None:
        path = self._path(tenant_id=tenant_id, user_id=user_id, store_id=store_id)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _to_bool(value: Any, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


user_mail_settings_store = UserMailSettingsStore()

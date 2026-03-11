from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from ..data_access import DATA_DIR

SETTINGS_DIR = DATA_DIR / "customer_service"
SETTINGS_FILE = SETTINGS_DIR / "amazon_email_config.json"


@dataclass(frozen=True)
class AmazonEmailSettings:
    email_account: str
    email_password: str
    ssl_enabled: bool
    ssl_host: str
    ssl_port: int
    updated_at: str | None


class AmazonEmailSettingsStore:
    def load(self) -> AmazonEmailSettings:
        if not SETTINGS_FILE.exists():
            return AmazonEmailSettings(
                email_account="",
                email_password="",
                ssl_enabled=True,
                ssl_host="",
                ssl_port=993,
                updated_at=None,
            )

        try:
            payload = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return AmazonEmailSettings(
                email_account="",
                email_password="",
                ssl_enabled=True,
                ssl_host="",
                ssl_port=993,
                updated_at=None,
            )

        if not isinstance(payload, dict):
            return AmazonEmailSettings(
                email_account="",
                email_password="",
                ssl_enabled=True,
                ssl_host="",
                ssl_port=993,
                updated_at=None,
            )

        # Backward compatibility: migrate old ssh_* keys if present.
        raw_port = payload.get("ssl_port", payload.get("ssh_port", 993))
        try:
            ssl_port = int(raw_port)
        except (TypeError, ValueError):
            ssl_port = 993
        ssl_port = min(max(1, ssl_port), 65535)

        return AmazonEmailSettings(
            email_account=str(payload.get("email_account") or "").strip(),
            email_password=str(payload.get("email_password") or ""),
            ssl_enabled=bool(payload.get("ssl_enabled", payload.get("ssh_enabled", True))),
            ssl_host=str(payload.get("ssl_host", payload.get("ssh_host", "")) or "").strip(),
            ssl_port=ssl_port,
            updated_at=str(payload.get("updated_at") or "").strip() or None,
        )

    def save(
        self,
        email_account: str,
        email_password: str,
        ssl_enabled: bool,
        ssl_host: str,
        ssl_port: int,
    ) -> AmazonEmailSettings:
        SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
        updated_at = datetime.now(timezone.utc).isoformat()
        safe_port = min(max(1, int(ssl_port)), 65535)

        payload = {
            "email_account": email_account.strip(),
            "email_password": email_password,
            "ssl_enabled": bool(ssl_enabled),
            "ssl_host": ssl_host.strip(),
            "ssl_port": safe_port,
            "updated_at": updated_at,
        }
        SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        return AmazonEmailSettings(
            email_account=payload["email_account"],
            email_password=payload["email_password"],
            ssl_enabled=bool(payload["ssl_enabled"]),
            ssl_host=payload["ssl_host"],
            ssl_port=safe_port,
            updated_at=updated_at,
        )


amazon_email_settings_store = AmazonEmailSettingsStore()

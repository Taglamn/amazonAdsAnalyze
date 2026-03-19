from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Iterable

from .message_types import IncomingBuyerMessage

_DEFAULT_META_DIR = Path(__file__).resolve().parent.parent / "data" / "customer_service_meta"


class MessageMetaStore:
    """Persist lightweight message metadata for IMAP detail/sending lookup."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or _DEFAULT_META_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def upsert_incoming_messages(
        self,
        *,
        tenant_id: int,
        store_id: int,
        incoming_messages: Iterable[IncomingBuyerMessage],
    ) -> None:
        with self._lock:
            payload = self._read_payload_locked(tenant_id=tenant_id, store_id=store_id)
            for item in incoming_messages:
                key = str(item.conversation_id or "").strip()
                if not key:
                    continue
                payload[key] = {
                    "subject": str(item.subject or ""),
                    "from": str(item.from_address or ""),
                    "reply_to": str(item.reply_to or ""),
                    "message_id": str(item.message_id or key),
                    "body": str(item.buyer_message or ""),
                    "headers": (item.raw_data or {}).get("headers") or {},
                    "raw_data": item.raw_data or {},
                }
            self._write_payload_locked(tenant_id=tenant_id, store_id=store_id, payload=payload)

    def upsert_message_detail(
        self,
        *,
        tenant_id: int,
        store_id: int,
        conversation_id: str,
        detail: dict[str, Any],
    ) -> None:
        key = str(conversation_id or "").strip()
        if not key:
            return
        with self._lock:
            payload = self._read_payload_locked(tenant_id=tenant_id, store_id=store_id)
            merged = dict(payload.get(key) or {})
            merged.update(detail or {})
            payload[key] = merged
            self._write_payload_locked(tenant_id=tenant_id, store_id=store_id, payload=payload)

    def get_message_detail(
        self,
        *,
        tenant_id: int,
        store_id: int,
        conversation_id: str,
    ) -> dict[str, Any] | None:
        key = str(conversation_id or "").strip()
        if not key:
            return None
        with self._lock:
            payload = self._read_payload_locked(tenant_id=tenant_id, store_id=store_id)
            entry = payload.get(key)
            if not isinstance(entry, dict):
                return None
            return dict(entry)

    def _file_path(self, *, tenant_id: int, store_id: int) -> Path:
        return self.base_dir / f"tenant_{tenant_id}_store_{store_id}.json"

    def _read_payload_locked(self, *, tenant_id: int, store_id: int) -> dict[str, Any]:
        path = self._file_path(tenant_id=tenant_id, store_id=store_id)
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return data if isinstance(data, dict) else {}

    def _write_payload_locked(self, *, tenant_id: int, store_id: int, payload: dict[str, Any]) -> None:
        path = self._file_path(tenant_id=tenant_id, store_id=store_id)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


message_meta_store = MessageMetaStore()


from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.lingxing_client import LingxingApiError, LingxingClient, LingxingCredentials

from .config import CustomerServiceSettings, get_customer_service_settings


class MessagingAPIError(RuntimeError):
    """Raised when Lingxing message APIs return an error."""


@dataclass(frozen=True)
class IncomingBuyerMessage:
    conversation_id: str
    buyer_message: str


@dataclass(frozen=True)
class LingxingBoundStore:
    sid: int
    external_store_id: str
    store_name: str


class LingxingMessagingClient:
    """Lingxing message client used to fetch and send buyer messages."""

    def __init__(
        self,
        credentials: LingxingCredentials | None = None,
        settings: CustomerServiceSettings | None = None,
    ) -> None:
        self.settings = settings or get_customer_service_settings()
        self.client = LingxingClient(credentials=credentials or LingxingCredentials.from_env())
        self._access_token = ""

    def list_bound_stores(self) -> list[LingxingBoundStore]:
        """List active stores under current Lingxing account."""

        access_token = self._ensure_access_token()
        try:
            sellers = self.client.list_sellers(access_token=access_token)
        except LingxingApiError as exc:
            raise MessagingAPIError(str(exc)) from exc

        stores: list[LingxingBoundStore] = []
        for item in sellers:
            if int(item.get("status", 0) or 0) != 1:
                continue

            sid_raw = item.get("sid")
            try:
                sid = int(sid_raw)
            except (TypeError, ValueError):
                continue

            store_name = str(item.get("name") or "").strip() or f"lingxing_{sid}"
            stores.append(
                LingxingBoundStore(
                    sid=sid,
                    external_store_id=f"lingxing_{sid}",
                    store_name=store_name,
                )
            )
        stores.sort(key=lambda x: x.store_name.lower())
        return stores

    def resolve_store(
        self,
        *,
        external_store_id: str | None = None,
        store_name: str | None = None,
    ) -> LingxingBoundStore:
        """Resolve one store using external_store_id or store_name."""

        stores = self.list_bound_stores()
        store_id = str(external_store_id or "").strip()
        name = str(store_name or "").strip()

        if store_id:
            for store in stores:
                if store.external_store_id == store_id or store.store_name == store_id:
                    return store
            raise MessagingAPIError(f"Lingxing store not found: {store_id}")

        if name:
            for store in stores:
                if store.store_name == name:
                    return store
            raise MessagingAPIError(f"Lingxing store not found by name: {name}")

        raise MessagingAPIError("Missing store selector")

    def fetch_buyer_messages(self, *, store_name: str, sid: int | None = None) -> list[IncomingBuyerMessage]:
        """Fetch buyer messages for one Lingxing store."""

        payload: dict[str, Any] = {}
        if self.settings.lingxing_list_messages_store_name_field:
            payload[self.settings.lingxing_list_messages_store_name_field] = store_name
        if sid is not None and self.settings.lingxing_list_messages_sid_field:
            payload[self.settings.lingxing_list_messages_sid_field] = sid

        method = self.settings.lingxing_list_messages_method or "POST"
        method = method.upper()
        query = payload if method == "GET" else None
        body = None if method == "GET" else payload

        resp = self._call_openapi_with_fallback(
            path=self.settings.lingxing_list_messages_path,
            method=method,
            query=query,
            body=body,
        )
        return self._extract_messages(resp)

    def send_reply(
        self,
        conversation_id: str,
        reply: str,
        *,
        store_name: str,
        sid: int | None = None,
    ) -> dict[str, Any]:
        """Send reply for one conversation in one Lingxing store."""

        payload: dict[str, Any] = {}
        if self.settings.lingxing_send_message_conversation_field:
            payload[self.settings.lingxing_send_message_conversation_field] = conversation_id
        if self.settings.lingxing_send_message_reply_field:
            payload[self.settings.lingxing_send_message_reply_field] = reply
        if self.settings.lingxing_send_message_store_name_field:
            payload[self.settings.lingxing_send_message_store_name_field] = store_name
        if sid is not None and self.settings.lingxing_send_message_sid_field:
            payload[self.settings.lingxing_send_message_sid_field] = sid

        path = self.settings.lingxing_send_message_path
        if "{conversation_id}" in path:
            path = path.format(conversation_id=conversation_id)

        method = self.settings.lingxing_send_message_method or "POST"
        method = method.upper()
        query = payload if method == "GET" else None
        body = None if method == "GET" else payload

        return self._call_openapi_with_fallback(path=path, method=method, query=query, body=body)

    def _ensure_access_token(self) -> str:
        if self._access_token:
            return self._access_token
        try:
            self._access_token = self.client.generate_access_token()
        except LingxingApiError as exc:
            raise MessagingAPIError(str(exc)) from exc
        return self._access_token

    def _call_openapi(
        self,
        *,
        path: str,
        method: str,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        access_token = self._ensure_access_token()
        try:
            return self.client.call_openapi(
                access_token=access_token,
                path=path,
                method=method,
                query=query,
                body=body,
            )
        except LingxingApiError as exc:
            raise MessagingAPIError(str(exc)) from exc

    def _call_openapi_with_fallback(
        self,
        *,
        path: str,
        method: str,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Call OpenAPI with conservative path fallbacks for Lingxing endpoint variants."""

        candidates = self._build_path_candidates(path)
        last_error: MessagingAPIError | None = None
        for candidate in candidates:
            try:
                return self._call_openapi(path=candidate, method=method, query=query, body=body)
            except MessagingAPIError as exc:
                last_error = exc
                lowered = str(exc).lower()
                # Only try next candidate for "service not found" style errors.
                if ("服务不存在" not in str(exc)) and ("not_found" not in lowered) and ("404" not in lowered):
                    raise

        if last_error is not None:
            raise last_error
        raise MessagingAPIError("Lingxing API call failed without detailed error")

    @staticmethod
    def _build_path_candidates(path: str) -> list[str]:
        """Build possible Lingxing API path variants while preserving explicit config priority."""

        base = (path or "").strip() or "/erp/sc/message/lists"
        candidates: list[str] = [base]

        if base.endswith("/lists"):
            candidates.append(base[:-1])  # /lists -> /list
        if "/message/" in base:
            candidates.append(base.replace("/message/", "/mail/"))
        if base.endswith("/reply"):
            candidates.append(base[:-5] + "send")

        # Deduplicate while preserving order.
        seen: set[str] = set()
        ordered: list[str] = []
        for item in candidates:
            if item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return ordered

    def _extract_messages(self, payload: dict[str, Any]) -> list[IncomingBuyerMessage]:
        seen: set[tuple[str, str]] = set()
        normalized: list[IncomingBuyerMessage] = []

        def add_message(node: dict[str, Any], fallback_cid: str = "") -> None:
            text = self._extract_message_text(node)
            if not text:
                return
            if not self._is_buyer_message(node):
                return

            cid = self._extract_conversation_id(node) or fallback_cid
            if not cid:
                return

            key = (cid, text)
            if key in seen:
                return
            seen.add(key)
            normalized.append(IncomingBuyerMessage(conversation_id=cid, buyer_message=text))

        def walk(node: Any, fallback_cid: str = "") -> None:
            if isinstance(node, dict):
                cid = self._extract_conversation_id(node) or fallback_cid

                messages = node.get("messages")
                if isinstance(messages, list):
                    for item in messages:
                        if isinstance(item, dict):
                            add_message(item, fallback_cid=cid)

                if self._extract_message_text(node):
                    add_message(node, fallback_cid=fallback_cid)

                for key in ("data", "payload", "result", "list", "items", "rows", "conversations"):
                    child = node.get(key)
                    if child is not None:
                        walk(child, fallback_cid=cid)
            elif isinstance(node, list):
                for item in node:
                    walk(item, fallback_cid=fallback_cid)
            elif isinstance(node, str):
                stripped = node.strip()
                if stripped.startswith("{") or stripped.startswith("["):
                    try:
                        walk(json.loads(stripped), fallback_cid=fallback_cid)
                    except json.JSONDecodeError:
                        return

        walk(payload)
        return normalized

    @staticmethod
    def _extract_conversation_id(payload: dict[str, Any]) -> str:
        for key in ("conversation_id", "conversationId", "thread_id", "threadId", "session_id", "sessionId"):
            raw = payload.get(key)
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                return text
        return ""

    @staticmethod
    def _extract_message_text(payload: dict[str, Any]) -> str:
        for key in ("buyerMessage", "message", "text", "content", "body", "question", "buyer_message"):
            raw = payload.get(key)
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                return text
        return ""

    @staticmethod
    def _is_buyer_message(payload: dict[str, Any]) -> bool:
        sender = str(
            payload.get("senderType")
            or payload.get("sender")
            or payload.get("sender_role")
            or payload.get("from_type")
            or payload.get("fromType")
            or ""
        ).strip().lower()
        if not sender:
            return True
        return sender in {"buyer", "customer", "amazon_customer", "user", "client"}

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request

from .config import CustomerServiceSettings, get_customer_service_settings


class SPAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class IncomingBuyerMessage:
    conversation_id: str
    buyer_message: str


class AmazonSPMessagingClient:
    def __init__(self, settings: CustomerServiceSettings | None = None) -> None:
        self.settings = settings or get_customer_service_settings()

    def fetch_buyer_messages(self) -> list[IncomingBuyerMessage]:
        payload = self._request_json("GET", self.settings.sp_api_list_messages_path)
        return self._extract_messages(payload)

    def send_reply(self, conversation_id: str, reply: str) -> dict[str, Any]:
        raw_path = self.settings.sp_api_send_message_path
        if "{conversation_id}" in raw_path:
            path = raw_path.format(conversation_id=conversation_id)
        else:
            path = raw_path
        body = {
            "conversationId": conversation_id,
            "message": reply,
        }
        return self._request_json("POST", path, body=body)

    def _request_json(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        token = self.settings.sp_api_access_token
        if not token:
            raise SPAPIError("CUSTOMER_SERVICE_SP_API_ACCESS_TOKEN is not set")

        base_url = self.settings.sp_api_base_url.rstrip("/")
        final_path = path if path.startswith("/") else f"/{path}"
        url = f"{base_url}{final_path}"

        if method.upper() == "GET" and self.settings.sp_api_marketplace_id:
            query = parse.urlencode({"marketplaceIds": self.settings.sp_api_marketplace_id})
            connector = "&" if "?" in url else "?"
            url = f"{url}{connector}{query}"

        data_bytes = None
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if body is not None:
            data_bytes = json.dumps(body).encode("utf-8")

        req = request.Request(url, data=data_bytes, headers=headers, method=method.upper())
        try:
            with request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise SPAPIError(f"SP-API HTTP error {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise SPAPIError(f"SP-API network error: {exc.reason}") from exc

        if not raw.strip():
            return {}

        try:
            parsed_payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SPAPIError(f"SP-API returned non-JSON payload: {raw[:200]}") from exc

        if not isinstance(parsed_payload, dict):
            raise SPAPIError("SP-API payload must be a JSON object")
        return parsed_payload

    def _extract_messages(self, payload: dict[str, Any]) -> list[IncomingBuyerMessage]:
        seen: set[tuple[str, str]] = set()
        normalized: list[IncomingBuyerMessage] = []

        conversations = payload.get("conversations")
        if isinstance(conversations, list):
            for conversation in conversations:
                if not isinstance(conversation, dict):
                    continue
                conversation_id = str(
                    conversation.get("conversationId")
                    or conversation.get("conversation_id")
                    or ""
                ).strip()
                messages = conversation.get("messages")
                if not isinstance(messages, list):
                    continue
                for msg in messages:
                    if not isinstance(msg, dict):
                        continue
                    text = self._extract_message_text(msg)
                    if not text:
                        continue
                    if not self._is_buyer_message(msg):
                        continue
                    cid = conversation_id or str(
                        msg.get("conversationId") or msg.get("conversation_id") or ""
                    ).strip()
                    if not cid:
                        continue
                    key = (cid, text)
                    if key in seen:
                        continue
                    seen.add(key)
                    normalized.append(
                        IncomingBuyerMessage(
                            conversation_id=cid,
                            buyer_message=text,
                        )
                    )

        messages = payload.get("messages")
        if isinstance(messages, list):
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                text = self._extract_message_text(msg)
                if not text:
                    continue
                if not self._is_buyer_message(msg):
                    continue
                cid = str(msg.get("conversationId") or msg.get("conversation_id") or "").strip()
                if not cid:
                    continue
                key = (cid, text)
                if key in seen:
                    continue
                seen.add(key)
                normalized.append(IncomingBuyerMessage(conversation_id=cid, buyer_message=text))

        if not normalized:
            payload_obj = payload.get("payload")
            if isinstance(payload_obj, dict):
                return self._extract_messages(payload_obj)

        return normalized

    @staticmethod
    def _extract_message_text(payload: dict[str, Any]) -> str:
        for key in ("buyerMessage", "message", "text", "content", "body"):
            raw = payload.get(key)
            if raw is not None:
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
            or ""
        ).strip().lower()

        if not sender:
            return True
        return sender in {"buyer", "customer", "amazon_customer"}

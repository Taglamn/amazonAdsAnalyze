from __future__ import annotations

from typing import Any

from .sp_api import LingxingMessagingClient


class MessageSendService:
    def __init__(self, client: LingxingMessagingClient, store_name: str, sid: int | None = None) -> None:
        self.client = client
        self.store_name = store_name
        self.sid = sid

    def send(self, conversation_id: str, reply: str) -> dict[str, Any]:
        return self.client.send_reply(
            conversation_id=conversation_id,
            reply=reply,
            store_name=self.store_name,
            sid=self.sid,
        )

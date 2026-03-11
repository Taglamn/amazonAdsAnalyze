from __future__ import annotations

from typing import Any

from .sp_api import AmazonSPMessagingClient


class MessageSendService:
    def __init__(self, client: AmazonSPMessagingClient) -> None:
        self.client = client

    def send(self, conversation_id: str, reply: str) -> dict[str, Any]:
        return self.client.send_reply(conversation_id=conversation_id, reply=reply)

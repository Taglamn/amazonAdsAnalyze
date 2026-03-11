from __future__ import annotations

from .sp_api import AmazonSPMessagingClient, IncomingBuyerMessage


class MessageSyncService:
    def __init__(self, client: AmazonSPMessagingClient) -> None:
        self.client = client

    def fetch_messages(self) -> list[IncomingBuyerMessage]:
        return self.client.fetch_buyer_messages()

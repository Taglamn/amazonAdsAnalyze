from __future__ import annotations

from .sp_api import IncomingBuyerMessage, LingxingMessagingClient


class MessageSyncService:
    """Wrapper around Lingxing client for buyer-message synchronization."""

    def __init__(self, client: LingxingMessagingClient, store_name: str, sid: int | None = None) -> None:
        self.client = client
        self.store_name = store_name
        self.sid = sid

    def fetch_messages(self) -> list[IncomingBuyerMessage]:
        """Fetch buyer messages for the scoped store."""

        return self.client.fetch_buyer_messages(store_name=self.store_name, sid=self.sid)

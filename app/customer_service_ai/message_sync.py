from __future__ import annotations

from .sp_api import AmazonSPMessagingClient, IncomingBuyerMessage


class MessageSyncService:
    """Wrapper around SP-API client for buyer-message synchronization."""

    def __init__(self, client: AmazonSPMessagingClient, external_store_id: str | None = None) -> None:
        self.client = client
        self.external_store_id = external_store_id

    def fetch_messages(self) -> list[IncomingBuyerMessage]:
        """Fetch buyer messages, optionally scoped to a store identifier."""

        return self.client.fetch_buyer_messages(external_store_id=self.external_store_id)

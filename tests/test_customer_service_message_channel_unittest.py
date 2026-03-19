from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.customer_service_ai.message_meta_store import MessageMetaStore
from app.customer_service_ai.message_send import MessageSendService
from app.customer_service_ai.message_sync import MessageSyncService
from app.customer_service_ai.message_types import IncomingBuyerMessage


class CustomerServiceMessageChannelTest(unittest.TestCase):
    def test_message_sync_converts_filtered_rows(self) -> None:
        raw = [{"subject": "x"}]
        filtered = [
            {
                "subject": "Buyer-Seller Messaging: New Buyer Message",
                "from": "buyer-seller-messaging@amazon.com",
                "reply_to": "relay@marketplace.amazon.com",
                "message_id": "<m1@amazon.com>",
                "clean_body": "Need help",
                "body": "Need help",
                "headers": {"x-amazon-trace-id": "abc"},
                "buyer_message_id": "BID-1",
            }
        ]
        with patch("app.customer_service_ai.message_sync.get_unread_emails", return_value=raw):
            with patch("app.customer_service_ai.message_sync.filter_amazon_messages", return_value=filtered):
                rows = MessageSyncService().fetch_messages()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].conversation_id, "<m1@amazon.com>")
        self.assertEqual(rows[0].reply_to, "relay@marketplace.amazon.com")
        self.assertEqual(rows[0].buyer_message, "Need help")

    def test_message_send_uses_meta_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            meta_store = MessageMetaStore(base_dir=Path(tmp_dir))
            meta_store.upsert_incoming_messages(
                tenant_id=1,
                store_id=2,
                incoming_messages=[
                    IncomingBuyerMessage(
                        conversation_id="<m2@amazon.com>",
                        buyer_message="Question",
                        subject="Buyer-Seller Messaging: New Buyer Message",
                        from_address="buyer-seller-messaging@amazon.com",
                        reply_to="relay@marketplace.amazon.com",
                        message_id="<m2@amazon.com>",
                    )
                ],
            )
            service = MessageSendService(tenant_id=1, store_id=2, meta_store=meta_store)
            with patch(
                "app.customer_service_ai.message_send.reply_to_amazon_email",
                return_value={"success": True, "message_id": "<sent@amazon.com>"},
            ) as reply_mock:
                result = service.send(conversation_id="<m2@amazon.com>", reply="Thanks for your message.")

        self.assertTrue(result["success"])
        args, _ = reply_mock.call_args
        self.assertEqual(args[0]["reply-to"], "relay@marketplace.amazon.com")


if __name__ == "__main__":
    unittest.main()

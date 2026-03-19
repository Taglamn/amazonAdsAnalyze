from __future__ import annotations

import unittest

from app.customer_service_ai.amazon_message_filter import (
    AmazonFilterBlacklist,
    filter_amazon_messages,
)


class AmazonMessageFilterTest(unittest.TestCase):
    def test_keep_valid_amazon_buyer_message(self) -> None:
        emails = [
            {
                "subject": "Buyer-Seller Messaging: New Buyer Message",
                "from": "Amazon Messaging <buyer-seller-messaging@marketplace.amazon.com>",
                "reply_to": "buyer_123@marketplace.amazon.com",
                "message_id": "<msg-1@amazon.com>",
                "body": "Hello seller.\nBuyer Message ID: B0TEST123\nPlease help.",
                "headers": {
                    "X-AMAZON-TRACE-ID": "abc123",
                    "Message-ID": "<msg-1@amazon.com>",
                },
            }
        ]

        result = filter_amazon_messages(emails)
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertEqual(item["subject"], "Buyer-Seller Messaging: New Buyer Message")
        self.assertEqual(item["buyer_email"], "buyer_123@marketplace.amazon.com")
        self.assertEqual(item["message_id"], "<msg-1@amazon.com>")
        self.assertIn("Hello seller.", item["clean_body"])
        self.assertEqual(item["buyer_message_id"], "B0TEST123")

    def test_filter_non_amazon_sender_and_log(self) -> None:
        emails = [
            {
                "subject": "Buyer-Seller Messaging: New Buyer Message",
                "from": "spam@example.org",
                "reply_to": "a@b.com",
                "message_id": "<spam-1@example.org>",
                "body": "test",
                "headers": {
                    "X-AMAZON-TRACE-ID": "abc123",
                },
            }
        ]

        with self.assertLogs("app.customer_service_ai.amazon_message_filter", level="INFO") as logs:
            result = filter_amazon_messages(emails)
        self.assertEqual(result, [])
        self.assertTrue(any("sender_not_amazon" in line for line in logs.output))

    def test_filter_missing_amazon_header(self) -> None:
        emails = [
            {
                "subject": "You have received a new message",
                "from": "buyer-seller-messaging@amazon.com",
                "reply_to": "buyer@marketplace.amazon.com",
                "message_id": "<msg-2@amazon.com>",
                "body": "Buyer Message ID: BID-2",
                "headers": {
                    "Message-ID": "<msg-2@amazon.com>",
                    "X-Test": "foo",
                },
            }
        ]

        result = filter_amazon_messages(emails)
        self.assertEqual(result, [])

    def test_blacklist_extension(self) -> None:
        emails = [
            {
                "subject": "Buyer-Seller Messaging: New Buyer Message",
                "from": "buyer-seller-messaging@amazon.com",
                "reply_to": "buyer@marketplace.amazon.com",
                "message_id": "<msg-3@amazon.com>",
                "body": "Buyer Message ID: BID-3",
                "headers": {
                    "X-AMAZON-TRACE-ID": "abc123",
                    "Message-ID": "<msg-3@amazon.com>",
                },
            }
        ]
        result = filter_amazon_messages(
            emails,
            blacklist=AmazonFilterBlacklist(sender_contains=("buyer-seller-messaging@amazon.com",)),
        )
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()


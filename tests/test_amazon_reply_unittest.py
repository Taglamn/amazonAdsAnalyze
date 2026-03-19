from __future__ import annotations

import unittest
from unittest.mock import patch

from app.customer_service_ai.amazon_reply import reply_to_amazon_email


class AmazonReplyTest(unittest.TestCase):
    def test_reply_success_with_thread_headers(self) -> None:
        original = {
            "subject": "Buyer-Seller Messaging: New Buyer Message",
            "reply-to": "Amazon Relay <abc123@marketplace.amazon.com>",
            "message-id": "<origin-1@amazon.com>",
        }

        with patch("app.customer_service_ai.amazon_reply.send_email", return_value="<sent-1@amazon.com>") as send_mock:
            result = reply_to_amazon_email(original, "Hello, we will help with your order.")

        self.assertTrue(result["success"])
        self.assertFalse(result["blocked"])
        args, kwargs = send_mock.call_args
        _ = args
        self.assertEqual(kwargs["to_address"], "Amazon Relay <abc123@marketplace.amazon.com>")
        self.assertEqual(kwargs["subject"], "Re: Buyer-Seller Messaging: New Buyer Message")
        self.assertEqual(kwargs["headers"]["In-Reply-To"], "<origin-1@amazon.com>")
        self.assertEqual(kwargs["headers"]["References"], "<origin-1@amazon.com>")

    def test_reply_blocked_when_not_amazon_relay(self) -> None:
        original = {
            "subject": "Buyer-Seller Messaging: New Buyer Message",
            "reply-to": "buyer@gmail.com",
            "message-id": "<origin-2@amazon.com>",
        }
        result = reply_to_amazon_email(original, "Hello")
        self.assertFalse(result["success"])
        self.assertTrue(result["blocked"])
        self.assertEqual(result["error_code"], "invalid_reply_to")

    def test_reply_blocked_by_external_link(self) -> None:
        original = {
            "subject": "Buyer-Seller Messaging: New Buyer Message",
            "reply-to": "abc123@marketplace.amazon.com",
            "message-id": "<origin-3@amazon.com>",
        }
        result = reply_to_amazon_email(original, "Please check https://example.com for details")
        self.assertFalse(result["success"])
        self.assertTrue(result["blocked"])
        self.assertIn("external_link", result["violations"])

    def test_reply_blocked_by_sensitive_term(self) -> None:
        original = {
            "subject": "Buyer-Seller Messaging: New Buyer Message",
            "reply-to": "abc123@marketplace.amazon.com",
            "message-id": "<origin-4@amazon.com>",
        }
        result = reply_to_amazon_email(original, "We can offer refund without return for this case.")
        self.assertFalse(result["success"])
        self.assertTrue(result["blocked"])
        self.assertIn("sensitive_term", result["violations"])


if __name__ == "__main__":
    unittest.main()


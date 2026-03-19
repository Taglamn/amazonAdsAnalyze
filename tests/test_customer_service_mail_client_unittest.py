from __future__ import annotations

import unittest
from email.message import EmailMessage
from unittest.mock import patch

from app.customer_service_ai.mail_client import get_unread_emails, parse_email, send_email


class _FakeIMAP4SSL:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def __enter__(self) -> "_FakeIMAP4SSL":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def login(self, username: str, password: str) -> tuple[str, list[bytes]]:
        return ("OK", [b"logged"])

    def select(self, mailbox: str) -> tuple[str, list[bytes]]:
        return ("OK", [b"2"])

    def search(self, charset, *criteria) -> tuple[str, list[bytes]]:
        _ = charset, criteria
        return ("OK", [b"1 2"])

    def fetch(self, message_id: bytes, command: str):
        _ = command
        msg = EmailMessage()
        msg["Subject"] = f"Order Update {message_id.decode()}"
        msg["From"] = "buyer@example.com"
        msg["Message-ID"] = f"<msg-{message_id.decode()}@example.com>"
        msg.set_content(f"Body {message_id.decode()}")
        return ("OK", [(b"RFC822", msg.as_bytes())])


class _FakeSMTPSSL:
    sent_messages: list[EmailMessage] = []

    def __init__(self, host: str, port: int, timeout: int):
        self.host = host
        self.port = port
        self.timeout = timeout

    def __enter__(self) -> "_FakeSMTPSSL":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def login(self, username: str, password: str) -> tuple[int, bytes]:
        _ = username, password
        return (235, b"ok")

    def send_message(self, message: EmailMessage) -> None:
        self.__class__.sent_messages.append(message)


class MailClientTest(unittest.TestCase):
    def setUp(self) -> None:
        _FakeSMTPSSL.sent_messages.clear()
        self.env = {
            "CUSTOMER_SERVICE_EMAIL_USERNAME": "seller@example.com",
            "CUSTOMER_SERVICE_EMAIL_PASSWORD": "pwd",
            "CUSTOMER_SERVICE_EMAIL_IMAP_HOST": "imap.example.com",
            "CUSTOMER_SERVICE_EMAIL_SMTP_HOST": "smtp.example.com",
            "CUSTOMER_SERVICE_EMAIL_IMAP_PORT": "993",
            "CUSTOMER_SERVICE_EMAIL_SMTP_PORT": "465",
            "CUSTOMER_SERVICE_EMAIL_SMTP_USE_SSL": "true",
        }

    def test_parse_email_extracts_required_fields(self) -> None:
        msg = EmailMessage()
        msg["Subject"] = "Amazon buyer question"
        msg["From"] = "buyer@example.com"
        msg["Reply-To"] = "reply@example.com"
        msg["Message-ID"] = "<abc123@example.com>"
        msg.set_content("Hello, where is my package?")

        parsed = parse_email(msg.as_bytes())

        self.assertEqual(parsed["subject"], "Amazon buyer question")
        self.assertEqual(parsed["from"], "buyer@example.com")
        self.assertEqual(parsed["reply_to"], "reply@example.com")
        self.assertEqual(parsed["message_id"], "<abc123@example.com>")
        self.assertIn("where is my package", parsed["body"])

    def test_get_unread_emails_returns_normalized_list(self) -> None:
        with patch.dict("os.environ", self.env, clear=False):
            with patch("app.customer_service_ai.mail_client.imaplib.IMAP4_SSL", _FakeIMAP4SSL):
                items = get_unread_emails()

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["from"], "buyer@example.com")
        self.assertTrue(items[0]["subject"].startswith("Order Update"))
        self.assertIn("Body", items[0]["body"])

    def test_send_email_supports_custom_headers(self) -> None:
        with patch.dict("os.environ", self.env, clear=False):
            with patch("app.customer_service_ai.mail_client.smtplib.SMTP_SSL", _FakeSMTPSSL):
                message_id = send_email(
                    to_address="buyer@example.com",
                    subject="Re: Your order",
                    body="Thanks for your message.",
                    headers={
                        "In-Reply-To": "<origin@example.com>",
                        "References": "<origin@example.com>",
                    },
                )

        self.assertTrue(message_id.startswith("<"))
        self.assertEqual(len(_FakeSMTPSSL.sent_messages), 1)
        sent = _FakeSMTPSSL.sent_messages[0]
        self.assertEqual(sent["To"], "buyer@example.com")
        self.assertEqual(sent["In-Reply-To"], "<origin@example.com>")
        self.assertIn("Thanks for your message.", sent.get_content())


if __name__ == "__main__":
    unittest.main()


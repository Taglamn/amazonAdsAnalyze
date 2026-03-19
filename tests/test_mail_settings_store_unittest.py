from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.customer_service_ai.mail_settings_store import UserMailSettingsStore


class MailSettingsStoreTest(unittest.TestCase):
    def test_upsert_and_resolve(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = UserMailSettingsStore(base_dir=Path(tmp_dir))
            saved = store.upsert_user_settings(
                tenant_id=1,
                user_id=9,
                payload={
                    "username": "seller@example.com",
                    "password": "abc",
                    "imap_host": "imap.example.com",
                    "imap_port": 993,
                    "imap_mailbox": "INBOX",
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 465,
                    "smtp_use_ssl": True,
                    "smtp_starttls": False,
                    "timeout_seconds": 20,
                },
            )
            self.assertTrue(saved["configured"])
            self.assertTrue(saved["password_set"])

            resolved = store.resolve_transport_settings_for_user(tenant_id=1, user_id=9)
            self.assertEqual(resolved.username, "seller@example.com")
            self.assertEqual(resolved.imap_host, "imap.example.com")
            self.assertEqual(resolved.smtp_host, "smtp.example.com")

    def test_empty_password_keeps_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = UserMailSettingsStore(base_dir=Path(tmp_dir))
            store.upsert_user_settings(
                tenant_id=1,
                user_id=9,
                payload={
                    "username": "seller@example.com",
                    "password": "abc",
                    "imap_host": "imap.example.com",
                    "imap_port": 993,
                    "imap_mailbox": "INBOX",
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 465,
                    "smtp_use_ssl": True,
                    "smtp_starttls": False,
                    "timeout_seconds": 20,
                },
            )
            saved = store.upsert_user_settings(
                tenant_id=1,
                user_id=9,
                payload={
                    "username": "seller2@example.com",
                    "password": "",
                    "imap_host": "imap2.example.com",
                    "imap_port": 993,
                    "imap_mailbox": "INBOX",
                    "smtp_host": "smtp2.example.com",
                    "smtp_port": 465,
                    "smtp_use_ssl": True,
                    "smtp_starttls": False,
                    "timeout_seconds": 20,
                },
            )
            self.assertTrue(saved["password_set"])
            resolved = store.resolve_transport_settings_for_user(tenant_id=1, user_id=9)
            self.assertEqual(resolved.password, "abc")

    def test_env_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = UserMailSettingsStore(base_dir=Path(tmp_dir))
            with patch.dict(
                "os.environ",
                {
                    "CUSTOMER_SERVICE_EMAIL_USERNAME": "env_user@example.com",
                    "CUSTOMER_SERVICE_EMAIL_PASSWORD": "env_pwd",
                    "CUSTOMER_SERVICE_EMAIL_IMAP_HOST": "env-imap.example.com",
                    "CUSTOMER_SERVICE_EMAIL_SMTP_HOST": "env-smtp.example.com",
                    "CUSTOMER_SERVICE_EMAIL_IMAP_PORT": "993",
                    "CUSTOMER_SERVICE_EMAIL_SMTP_PORT": "465",
                },
                clear=False,
            ):
                resolved = store.resolve_transport_settings_for_user(tenant_id=1, user_id=99)
            self.assertEqual(resolved.username, "env_user@example.com")
            self.assertEqual(resolved.password, "env_pwd")


if __name__ == "__main__":
    unittest.main()


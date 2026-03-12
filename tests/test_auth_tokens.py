from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from jose import jwt

from app.auth.config import get_auth_settings
from app.auth.security import (
    AuthError,
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
)


class AuthTokenTests(unittest.TestCase):
    def test_access_and_refresh_token_types(self) -> None:
        access_token = create_access_token(user_id=1, tenant_id=2, role="admin", email="a@example.com")
        refresh_token = create_refresh_token(user_id=1, tenant_id=2, role="admin", email="a@example.com")

        access_payload = decode_access_token(access_token)
        self.assertEqual(str(access_payload.get("typ")), "access")

        refresh_payload = decode_refresh_token(refresh_token)
        self.assertEqual(str(refresh_payload.get("typ")), "refresh")

        with self.assertRaises(AuthError):
            decode_access_token(refresh_token)
        with self.assertRaises(AuthError):
            decode_refresh_token(access_token)

    def test_decode_access_token_supports_legacy_token_without_type(self) -> None:
        settings = get_auth_settings()
        now = datetime.now(timezone.utc)
        payload = {
            "sub": "99",
            "tenant_id": 100,
            "role": "viewer",
            "email": "legacy@example.com",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
        }
        legacy_token = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
        decoded = decode_access_token(legacy_token)
        self.assertEqual(decoded["sub"], "99")


if __name__ == "__main__":
    unittest.main()

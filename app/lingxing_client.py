from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import ssl
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, parse, request

try:
    from Crypto.Cipher import AES
except ImportError as exc:  # pragma: no cover - runtime guard
    raise ImportError(
        "Missing dependency 'pycryptodome'. Install requirements with:\n"
        "  ./.venv/bin/pip install -r requirements.txt\n"
        "or run scripts with the project venv:\n"
        "  ./.venv/bin/python -m scripts.lingxing_sync --report-date YYYY-MM-DD"
    ) from exc

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


class LingxingApiError(RuntimeError):
    """Raised when Lingxing OpenAPI returns an error."""


@dataclass
class LingxingCredentials:
    app_id: str
    app_secret: str
    erp_username: str
    erp_password: str
    base_url: str = "https://openapi.lingxing.com"
    timeout_seconds: int = 90
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5
    tls_mode: str = "default"
    proxy_url: str = ""
    insecure_skip_verify: bool = False

    @classmethod
    def from_env(cls) -> "LingxingCredentials":
        if load_dotenv is not None:
            load_dotenv()

        return cls(
            app_id=os.getenv("LINGXING_APP_ID", "").strip(),
            app_secret=os.getenv("LINGXING_APP_SECRET", "").strip(),
            erp_username=os.getenv("LINGXING_ERP_USERNAME", "").strip(),
            erp_password=os.getenv("LINGXING_ERP_PASSWORD", "").strip(),
            base_url=os.getenv("LINGXING_BASE_URL", "https://openapi.lingxing.com").strip(),
            timeout_seconds=int(os.getenv("LINGXING_TIMEOUT_SECONDS", "90")),
            max_retries=max(1, int(os.getenv("LINGXING_MAX_RETRIES", "3"))),
            retry_backoff_seconds=float(os.getenv("LINGXING_RETRY_BACKOFF_SECONDS", "1.5")),
            tls_mode=os.getenv("LINGXING_TLS_MODE", "default").strip().lower(),
            proxy_url=os.getenv("LINGXING_PROXY_URL", "").strip(),
            insecure_skip_verify=os.getenv("LINGXING_INSECURE_SKIP_VERIFY", "false").strip().lower()
            in {"1", "true", "yes", "on"},
        )

    def validate(self) -> None:
        missing: List[str] = []
        if not self.erp_username:
            missing.append("LINGXING_ERP_USERNAME")
        if not self.erp_password:
            missing.append("LINGXING_ERP_PASSWORD")
        if not self.app_id:
            missing.append("LINGXING_APP_ID")
        if not self.app_secret:
            missing.append("LINGXING_APP_SECRET")

        if missing:
            raise ValueError(f"Missing Lingxing credentials: {', '.join(missing)}")


class LingxingClient:
    def __init__(self, credentials: LingxingCredentials):
        credentials.validate()
        self.credentials = credentials
        self._cached_access_token: str = ""

    @staticmethod
    def _pkcs5_pad(content: str) -> bytes:
        data = content.encode("utf-8")
        block_size = 16
        pad_len = block_size - (len(data) % block_size)
        return data + bytes([pad_len]) * pad_len

    def _aes_encrypt(self, plaintext: str) -> str:
        key = self.credentials.app_id.encode("utf-8")
        if len(key) not in (16, 24, 32):
            raise ValueError(
                "LINGXING_APP_ID must be 16/24/32 bytes for AES signing; "
                f"got {len(key)} bytes"
            )

        cipher = AES.new(key, AES.MODE_ECB)
        encrypted = cipher.encrypt(self._pkcs5_pad(plaintext))
        return base64.b64encode(encrypted).decode("utf-8")

    @staticmethod
    def _canonical_value(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return str(value)

    def _format_for_sign(self, request_params: Dict[str, Any]) -> str:
        chunks: List[str] = []
        for key in sorted(request_params.keys()):
            value = request_params[key]
            if value == "":
                continue
            chunks.append(f"{key}={self._canonical_value(value)}")
        return "&".join(chunks)

    def _generate_sign(self, params_for_sign: Dict[str, Any]) -> str:
        canonical = self._format_for_sign(params_for_sign)
        md5_upper = hashlib.md5(canonical.encode("utf-8")).hexdigest().upper()
        return self._aes_encrypt(md5_upper)

    def _request_json(
        self,
        method: str,
        path: str,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.credentials.base_url.rstrip('/')}{path}"
        if query:
            url = f"{url}?{parse.urlencode(query)}"

        req_headers = headers.copy() if headers else {}
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
            req_headers.setdefault("Content-Type", "application/json")

        req = request.Request(url=url, data=data, method=method.upper(), headers=req_headers)
        opener = self._build_opener()

        payload = ""
        attempts = self.credentials.max_retries
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                with opener.open(req, timeout=self.credentials.timeout_seconds) as resp:
                    payload = resp.read().decode("utf-8")
                    last_error = None
                    break
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                # Retry 5xx and gateway-like errors; fail fast for most 4xx.
                if exc.code >= 500 and attempt < attempts:
                    last_error = exc
                    time.sleep(self.credentials.retry_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                raise LingxingApiError(f"Lingxing HTTP error {exc.code}: {detail}") from exc
            except (error.URLError, TimeoutError, socket.timeout, ssl.SSLError, ConnectionResetError) as exc:
                last_error = exc
                if attempt < attempts:
                    time.sleep(self.credentials.retry_backoff_seconds * (2 ** (attempt - 1)))
                    continue
                break

        if last_error is not None:
            reason = getattr(last_error, "reason", None) or str(last_error)
            raise LingxingApiError(
                "Lingxing network error: "
                f"{reason}. host={self.credentials.base_url}. "
                "Please check outbound network/DNS/proxy and try increasing "
                "LINGXING_TIMEOUT_SECONDS. You can also try LINGXING_TLS_MODE=tls1_2 "
                "or set LINGXING_PROXY_URL."
            ) from last_error

        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise LingxingApiError(f"Lingxing non-JSON response: {payload[:300]}") from exc

    def _build_ssl_context(self) -> ssl.SSLContext:
        context = ssl.create_default_context()
        if self.credentials.insecure_skip_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if self.credentials.tls_mode == "tls1_2":
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.maximum_version = ssl.TLSVersion.TLSv1_2
        elif self.credentials.tls_mode in ("default", ""):
            pass
        else:
            raise ValueError(
                "Unsupported LINGXING_TLS_MODE. Use 'default' or 'tls1_2'."
            )

        return context

    def _build_opener(self):
        context = self._build_ssl_context()
        handlers = [request.HTTPSHandler(context=context)]
        if self.credentials.proxy_url:
            handlers.insert(0, request.ProxyHandler({"https": self.credentials.proxy_url}))
        return request.build_opener(*handlers)

    def generate_access_token(self) -> str:
        resp = self._request_json(
            method="POST",
            path="/api/auth-server/oauth/access-token",
            query={
                "appId": self.credentials.app_id,
                "appSecret": self.credentials.app_secret,
            },
        )

        if int(resp.get("code", -1)) != 200:
            raise LingxingApiError(
                f"Failed to get access token. code={resp.get('code')} msg={resp.get('msg') or resp.get('message')}"
            )

        data = resp.get("data") or {}
        token = data.get("access_token")
        if not token:
            raise LingxingApiError(f"access_token missing in response: {resp}")
        token_text = str(token).strip()
        self._cached_access_token = token_text
        return token_text

    @staticmethod
    def _is_token_expired_error(code: int, message: str) -> bool:
        msg = (message or "").strip().lower()
        if code in (2001003, 2001004):
            return True
        if "access token" in msg and ("expire" in msg or "expired" in msg or "missing" in msg):
            return True
        return False

    def call_openapi(
        self,
        access_token: str,
        path: str,
        method: str,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if not self._cached_access_token and access_token:
            self._cached_access_token = access_token.strip()

        retried_with_new_token = False
        while True:
            token_to_use = (self._cached_access_token or access_token or "").strip()
            if not token_to_use:
                raise LingxingApiError("Lingxing access token is empty")

            query_params = (query or {}).copy()
            body_params = body or {}

            sign_material: Dict[str, Any] = {}
            sign_material.update(query_params)
            sign_material.update(body_params)

            fixed = {
                "access_token": token_to_use,
                "app_key": self.credentials.app_id,
                "timestamp": str(int(time.time())),
            }
            sign_material.update(fixed)

            query_params.update(fixed)
            query_params["sign"] = self._generate_sign(sign_material)

            req_headers = {"X-API-VERSION": "2"}
            if headers:
                req_headers.update(headers)

            resp = self._request_json(
                method=method,
                path=path,
                query=query_params,
                body=body,
                headers=req_headers,
            )

            code = int(resp.get("code", -1))
            if code == 0:
                return resp

            message = str(resp.get("message") or resp.get("msg") or "")
            if not retried_with_new_token and self._is_token_expired_error(code, message):
                self.generate_access_token()
                retried_with_new_token = True
                continue

            raise LingxingApiError(
                f"Lingxing business error. path={path} code={code} message={message}"
            )

    def _post_paginated(
        self,
        access_token: str,
        path: str,
        body: Dict[str, Any],
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0
        next_token = ""

        while True:
            req_body = dict(body)
            req_body["length"] = page_size

            if next_token:
                req_body["next_token"] = next_token
                req_body.pop("offset", None)
            else:
                req_body["offset"] = offset

            resp = self.call_openapi(
                access_token=access_token,
                path=path,
                method="POST",
                body=req_body,
            )

            data = resp.get("data") or []
            total = int(resp.get("total", 0) or 0)
            rows.extend(data)

            returned_next = str(resp.get("next_token") or "").strip()
            if returned_next:
                next_token = returned_next
                if not data:
                    break
                if total and len(rows) >= total:
                    break
                continue

            next_token = ""
            offset += page_size
            if not data:
                break
            if total and offset >= total:
                break
            if len(data) < page_size:
                break

        return rows

    def list_sellers(self, access_token: str) -> List[Dict[str, Any]]:
        resp = self.call_openapi(
            access_token=access_token,
            path="/erp/sc/data/seller/lists",
            method="GET",
        )
        return list(resp.get("data") or [])

    def fetch_ad_reports_for_day(
        self,
        access_token: str,
        sid: int,
        report_date: str,
    ) -> List[Dict[str, Any]]:
        specs = [
            ("sp", "/pb/openapi/newad/spAdGroupReports", {"show_detail": 1}),
            ("sb", "/pb/openapi/newad/hsaAdGroupReports", {}),
            ("sd", "/pb/openapi/newad/sdAdGroupReports", {"show_detail": 1}),
        ]

        rows: List[Dict[str, Any]] = []
        for sponsored_type, path, extra in specs:
            payload: Dict[str, Any] = {
                "sid": sid,
                "report_date": report_date,
            }
            payload.update(extra)

            data = self._post_paginated(
                access_token=access_token,
                path=path,
                body=payload,
            )
            for item in data:
                row = dict(item)
                row["sponsored_type"] = sponsored_type
                rows.append(row)

        return rows

    def fetch_operation_logs(
        self,
        access_token: str,
        sid: int,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        for sponsored_type in ("sp", "sb", "sd"):
            for operate_type in ("adGroups", "campaigns", "keywords", "targets"):
                payload = {
                    "sid": sid,
                    "log_source": "all",
                    "sponsored_type": sponsored_type,
                    "operate_type": operate_type,
                    "start_date": start_date,
                    "end_date": end_date,
                }
                data = self._post_paginated(
                    access_token=access_token,
                    path="/pb/openapi/newad/apiLogStandard",
                    body=payload,
                )
                for item in data:
                    row = dict(item)
                    row["sponsored_type"] = sponsored_type
                    row["operate_type"] = operate_type
                    rows.append(row)

        return rows

    def fetch_bid_snapshots(self, access_token: str, sid: int) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        for sponsored_type, path in (
            ("sp", "/pb/openapi/newad/spAdGroups"),
            ("sb", "/pb/openapi/newad/hsaAdGroups"),
            ("sd", "/pb/openapi/newad/sdAdGroups"),
        ):
            payload = {"sid": sid}
            try:
                data = self._post_paginated(
                    access_token=access_token,
                    path=path,
                    body=payload,
                )
            except LingxingApiError:
                # Some ad types/accounts may not support this endpoint.
                continue
            for item in data:
                default_bid = item.get("default_bid")
                if default_bid is None:
                    default_bid = item.get("bid")
                if default_bid is None:
                    default_bid = item.get("base_bid")
                if default_bid is None:
                    continue

                campaign_id = (
                    item.get("campaign_id")
                    or item.get("campaignId")
                    or item.get("campaign")
                )
                campaign_name = (
                    item.get("campaign_name")
                    or item.get("campaignName")
                    or item.get("campaign")
                )
                ad_group_id = item.get("ad_group_id") or item.get("adGroupId")
                ad_group_name = (
                    item.get("name")
                    or item.get("ad_group_name")
                    or item.get("adGroupName")
                )
                rows.append(
                    {
                        "sponsored_type": sponsored_type,
                        "campaign_id": campaign_id,
                        "campaign_name": campaign_name,
                        "ad_group_id": ad_group_id,
                        "ad_group": ad_group_name or f"{sponsored_type}_{ad_group_id}",
                        "current_bid": default_bid,
                    }
                )

        return rows

    def fetch_campaign_names(self, access_token: str, sid: int) -> Dict[Tuple[str, int], str]:
        mapping: Dict[Tuple[str, int], str] = {}

        specs = [
            ("sp", "/pb/openapi/newad/spCampaigns"),
            ("sb", "/pb/openapi/newad/hsaCampaigns"),
            ("sd", "/pb/openapi/newad/sdCampaigns"),
        ]

        for sponsored_type, path in specs:
            try:
                data = self._post_paginated(
                    access_token=access_token,
                    path=path,
                    body={"sid": sid},
                )
            except LingxingApiError:
                continue

            for item in data:
                campaign_id = item.get("campaign_id") or item.get("campaignId") or item.get("id")
                try:
                    campaign_id_int = int(float(campaign_id))
                except (TypeError, ValueError):
                    continue

                name = (
                    item.get("name")
                    or item.get("campaign_name")
                    or item.get("campaignName")
                )
                if name is None:
                    continue

                campaign_name = str(name).strip()
                if not campaign_name:
                    continue

                mapping[(sponsored_type, campaign_id_int)] = campaign_name

        return mapping

    def fetch_query_word_reports_for_day(
        self,
        access_token: str,
        sid: int,
        report_date: str,
    ) -> List[Dict[str, Any]]:
        specs = [
            ("sp", "/pb/openapi/newad/queryWordReports", {"show_detail": 1}),
            ("sb", "/pb/openapi/newad/hsaQueryWordReports", {}),
            ("sd", "/pb/openapi/newad/sdQueryWordReports", {"show_detail": 1}),
        ]

        rows: List[Dict[str, Any]] = []
        for sponsored_type, path, extra in specs:
            payload: Dict[str, Any] = {
                "sid": sid,
                "report_date": report_date,
            }
            payload.update(extra)

            try:
                data = self._post_paginated(
                    access_token=access_token,
                    path=path,
                    body=payload,
                )
            except LingxingApiError:
                continue

            for item in data:
                row = dict(item)
                row["sponsored_type"] = sponsored_type
                rows.append(row)

        return rows

    def fetch_campaign_placement_reports_for_day(
        self,
        access_token: str,
        sid: int,
        report_date: str,
    ) -> List[Dict[str, Any]]:
        specs = [
            ("sp", "/pb/openapi/newad/campaignPlacementReports", {"show_detail": 1}),
            ("sb", "/pb/openapi/newad/hsaCampaignPlacementReports", {}),
            ("sd", "/pb/openapi/newad/sdCampaignPlacementReports", {"show_detail": 1}),
        ]

        rows: List[Dict[str, Any]] = []
        for sponsored_type, path, extra in specs:
            payload: Dict[str, Any] = {
                "sid": sid,
                "report_date": report_date,
            }
            payload.update(extra)

            try:
                data = self._post_paginated(
                    access_token=access_token,
                    path=path,
                    body=payload,
                )
            except LingxingApiError:
                continue

            for item in data:
                row = dict(item)
                row["sponsored_type"] = sponsored_type
                rows.append(row)

        return rows

    def fetch_ad_group_product_links(self, access_token: str, sid: int) -> List[Dict[str, Any]]:
        specs = [
            ("sp", "/pb/openapi/newad/spProductAds"),
            ("sb", "/pb/openapi/newad/sbAdHasProductAds"),
            ("sd", "/pb/openapi/newad/sdProductAds"),
        ]

        rows: List[Dict[str, Any]] = []
        for sponsored_type, path in specs:
            try:
                data = self._post_paginated(
                    access_token=access_token,
                    path=path,
                    body={"sid": sid},
                )
            except LingxingApiError:
                continue

            for item in data:
                row = dict(item)
                row["sponsored_type"] = sponsored_type
                rows.append(row)

        return rows

    def fetch_product_listings(
        self,
        access_token: str,
        sid: int,
        asins: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        candidate_paths = [
            "/erp/sc/data/product/lists",
            "/erp/sc/data/local_product/lists",
            "/erp/sc/data/local_inventory/product/lists",
        ]

        payload: Dict[str, Any] = {"sid": sid}
        if asins:
            payload["asins"] = asins

        for path in candidate_paths:
            try:
                return self._post_paginated(
                    access_token=access_token,
                    path=path,
                    body=payload,
                )
            except LingxingApiError:
                continue

        return []

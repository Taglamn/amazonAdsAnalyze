from __future__ import annotations

import json
import os
import socket
import ssl
import sys
from urllib import parse, request

from dotenv import load_dotenv


def _extract_host_from_base_url(base_url: str) -> str:
    cleaned = base_url.strip()
    if cleaned.startswith("https://"):
        cleaned = cleaned[len("https://") :]
    if cleaned.startswith("http://"):
        cleaned = cleaned[len("http://") :]
    return cleaned.split("/")[0].strip() or "openapi.lingxing.com"


def _tls_handshake(host: str, mode: str, timeout: int) -> tuple[bool, str]:
    try:
        ctx = ssl.create_default_context()
        if mode == "tls1_2":
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            ctx.maximum_version = ssl.TLSVersion.TLSv1_2

        with socket.create_connection((host, 443), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                return True, f"OK, TLS version: {ssock.version()}"
    except Exception as exc:  # noqa: BLE001
        return False, f"FAIL: {exc}"


def _token_request(base_url: str, app_id: str, app_secret: str, timeout: int) -> tuple[bool, str]:
    try:
        query = parse.urlencode({"appId": app_id, "appSecret": app_secret})
        url = f"{base_url.rstrip('/')}/api/auth-server/oauth/access-token?{query}"
        req = request.Request(url=url, method="POST")
        with request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        code = payload.get("code")
        msg = payload.get("msg") or payload.get("message")
        return True, f"OK, response code={code}, message={msg}"
    except Exception as exc:  # noqa: BLE001
        return False, f"FAIL: {exc}"


def _token_request_with_proxy(
    base_url: str,
    app_id: str,
    app_secret: str,
    timeout: int,
    proxy_url: str,
) -> tuple[bool, str]:
    try:
        query = parse.urlencode({"appId": app_id, "appSecret": app_secret})
        url = f"{base_url.rstrip('/')}/api/auth-server/oauth/access-token?{query}"
        opener = request.build_opener(request.ProxyHandler({"http": proxy_url, "https": proxy_url}))
        req = request.Request(url=url, method="POST")
        with opener.open(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        code = payload.get("code")
        msg = payload.get("msg") or payload.get("message")
        return True, f"OK, response code={code}, message={msg}"
    except Exception as exc:  # noqa: BLE001
        return False, f"FAIL: {exc}"


def main() -> int:
    load_dotenv()

    base_url = os.getenv("LINGXING_BASE_URL", "https://openapi.lingxing.com").strip()
    host = _extract_host_from_base_url(base_url)

    timeout = int(os.getenv("LINGXING_TIMEOUT_SECONDS", "60"))
    proxy_url = os.getenv("LINGXING_PROXY_URL", "").strip()

    print("[0] Runtime hints")
    print(f"    base_url: {base_url}")
    print(f"    timeout: {timeout}s")
    print(f"    env HTTPS_PROXY: {os.getenv('HTTPS_PROXY', '') or '<empty>'}")
    print(f"    env HTTP_PROXY: {os.getenv('HTTP_PROXY', '') or '<empty>'}")
    print(f"    LINGXING_PROXY_URL: {proxy_url or '<empty>'}")

    print(f"[1] DNS resolve: {host}")
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        addrs = sorted({item[4][0] for item in infos})
        print(f"    OK, addresses: {', '.join(addrs[:6])}")
    except Exception as exc:  # noqa: BLE001
        print(f"    FAIL: {exc}")
        return 2

    print("[2] TCP connect: host:443")
    try:
        with socket.create_connection((host, 443), timeout=15):
            print("    OK")
    except Exception as exc:  # noqa: BLE001
        print(f"    FAIL: {exc}")
        return 3

    print("[3] TLS handshake (default)")
    ok_default, msg_default = _tls_handshake(host=host, mode="default", timeout=20)
    print(f"    {msg_default}")

    print("[4] TLS handshake (force TLS1.2)")
    ok_tls12, msg_tls12 = _tls_handshake(host=host, mode="tls1_2", timeout=20)
    print(f"    {msg_tls12}")

    app_id = os.getenv("LINGXING_APP_ID", "")
    app_secret = os.getenv("LINGXING_APP_SECRET", "")
    if not app_id or not app_secret:
        print("[5] Token API skipped: missing LINGXING_APP_ID or LINGXING_APP_SECRET")
        return 0

    print("[5] Token API request")
    ok_token, msg_token = _token_request(
        base_url=base_url,
        app_id=app_id,
        app_secret=app_secret,
        timeout=max(timeout, 30),
    )
    print(f"    {msg_token}")

    ok_proxy = False
    if proxy_url:
        print("[6] Token API request via LINGXING_PROXY_URL")
        ok_proxy, msg_proxy = _token_request_with_proxy(
            base_url=base_url,
            app_id=app_id,
            app_secret=app_secret,
            timeout=max(timeout, 30),
            proxy_url=proxy_url,
        )
        print(f"    {msg_proxy}")
    else:
        print("[6] Token API request via LINGXING_PROXY_URL")
        print("    SKIP: LINGXING_PROXY_URL is empty")

    if not ok_default and ok_tls12:
        print("[hint] Default TLS failed but TLS1.2 works. Set LINGXING_TLS_MODE=tls1_2.")

    if not ok_token and not ok_proxy:
        print("[hint] Direct and proxy calls both failed. Verify proxy availability and company egress policy.")

    return 0 if (ok_token or ok_proxy) else 4


if __name__ == "__main__":
    sys.exit(main())

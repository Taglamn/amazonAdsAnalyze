from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


@dataclass(frozen=True)
class CustomerServiceSettings:
    database_url: str
    redis_url: str
    llm_provider: str
    llm_model: str
    openai_api_key: str
    gemini_api_key: str
    sp_api_base_url: str
    sp_api_access_token: str
    sp_api_marketplace_id: str
    sp_api_list_messages_path: str
    sp_api_send_message_path: str
    max_reply_chars: int


@lru_cache(maxsize=1)
def get_customer_service_settings() -> CustomerServiceSettings:
    if load_dotenv is not None:
        load_dotenv()

    llm_provider = os.getenv("CUSTOMER_SERVICE_LLM_PROVIDER", "gemini").strip().lower()
    if llm_provider not in {"openai", "gemini"}:
        raise ValueError("CUSTOMER_SERVICE_LLM_PROVIDER must be openai or gemini")

    max_reply_chars_raw = os.getenv("CUSTOMER_SERVICE_MAX_REPLY_CHARS", "1200").strip()
    try:
        max_reply_chars = max(200, int(max_reply_chars_raw))
    except ValueError:
        max_reply_chars = 1200

    return CustomerServiceSettings(
        database_url=(
            os.getenv("CUSTOMER_SERVICE_DATABASE_URL", "").strip()
            or "postgresql+psycopg2://postgres:postgres@localhost:5432/amazon_ads"
        ),
        redis_url=os.getenv("CUSTOMER_SERVICE_REDIS_URL", "redis://localhost:6379/0").strip(),
        llm_provider=llm_provider,
        llm_model=(
            os.getenv("CUSTOMER_SERVICE_LLM_MODEL", "").strip()
            or ("gpt-4o-mini" if llm_provider == "openai" else "gemini-2.5-flash")
        ),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
        sp_api_base_url=os.getenv(
            "CUSTOMER_SERVICE_SP_API_BASE_URL",
            "https://sellingpartnerapi-na.amazon.com",
        ).strip(),
        sp_api_access_token=os.getenv("CUSTOMER_SERVICE_SP_API_ACCESS_TOKEN", "").strip(),
        sp_api_marketplace_id=os.getenv("CUSTOMER_SERVICE_SP_API_MARKETPLACE_ID", "").strip(),
        sp_api_list_messages_path=os.getenv(
            "CUSTOMER_SERVICE_SP_API_LIST_MESSAGES_PATH",
            "/messaging/v1/buyerMessages",
        ).strip(),
        sp_api_send_message_path=os.getenv(
            "CUSTOMER_SERVICE_SP_API_SEND_MESSAGE_PATH",
            "/messaging/v1/conversations/{conversation_id}/messages",
        ).strip(),
        max_reply_chars=max_reply_chars,
    )

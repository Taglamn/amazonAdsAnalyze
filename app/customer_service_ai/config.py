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
    lingxing_list_messages_path: str
    lingxing_list_messages_method: str
    lingxing_list_messages_store_name_field: str
    lingxing_list_messages_sid_field: str
    lingxing_send_message_path: str
    lingxing_send_message_method: str
    lingxing_send_message_store_name_field: str
    lingxing_send_message_sid_field: str
    lingxing_send_message_conversation_field: str
    lingxing_send_message_reply_field: str
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
            or os.getenv("DATABASE_URL", "").strip()
            or "postgresql+psycopg2:///amazon_ads"
        ),
        redis_url=os.getenv("CUSTOMER_SERVICE_REDIS_URL", "redis://localhost:6379/0").strip(),
        llm_provider=llm_provider,
        llm_model=(
            os.getenv("CUSTOMER_SERVICE_LLM_MODEL", "").strip()
            or ("gpt-4o-mini" if llm_provider == "openai" else "gemini-2.5-flash")
        ),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
        lingxing_list_messages_path=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_PATH",
            "/erp/sc/message/lists",
        ).strip(),
        lingxing_list_messages_method=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_METHOD",
            "POST",
        ).strip().upper(),
        lingxing_list_messages_store_name_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_STORE_NAME_FIELD",
            "seller_name",
        ).strip(),
        lingxing_list_messages_sid_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_SID_FIELD",
            "sid",
        ).strip(),
        lingxing_send_message_path=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_PATH",
            "/erp/sc/message/reply",
        ).strip(),
        lingxing_send_message_method=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_METHOD",
            "POST",
        ).strip().upper(),
        lingxing_send_message_store_name_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_STORE_NAME_FIELD",
            "seller_name",
        ).strip(),
        lingxing_send_message_sid_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_SID_FIELD",
            "sid",
        ).strip(),
        lingxing_send_message_conversation_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_CONVERSATION_FIELD",
            "conversation_id",
        ).strip(),
        lingxing_send_message_reply_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_REPLY_FIELD",
            "reply",
        ).strip(),
        max_reply_chars=max_reply_chars,
    )

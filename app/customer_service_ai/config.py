from __future__ import annotations

import json
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
    lingxing_list_messages_flag_field: str
    lingxing_list_messages_flag_value: str
    lingxing_list_messages_email_field: str
    lingxing_list_messages_email_value: str
    lingxing_list_messages_email_map: dict[str, str]
    lingxing_list_messages_start_date_field: str
    lingxing_list_messages_end_date_field: str
    lingxing_list_messages_default_days: int
    lingxing_list_messages_offset_field: str
    lingxing_list_messages_length_field: str
    lingxing_list_messages_length_value: int
    lingxing_list_messages_store_name_field: str
    lingxing_list_messages_sid_field: str
    lingxing_mail_detail_path: str
    lingxing_mail_detail_method: str
    lingxing_mail_detail_uuid_field: str
    lingxing_mail_detail_email_field: str
    lingxing_mail_detail_store_name_field: str
    lingxing_mail_detail_sid_field: str
    lingxing_send_message_path: str
    lingxing_send_message_method: str
    lingxing_send_message_store_name_field: str
    lingxing_send_message_sid_field: str
    lingxing_send_message_conversation_field: str
    lingxing_send_message_reply_field: str
    lingxing_send_message_attachments_field: str
    lingxing_send_message_attachment_name_field: str
    lingxing_send_message_attachment_content_field: str
    lingxing_send_message_attachment_content_type_field: str
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

    list_days_raw = os.getenv("CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_DEFAULT_DAYS", "30").strip()
    list_len_raw = os.getenv("CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_LENGTH_VALUE", "20").strip()
    try:
        list_days = max(0, int(list_days_raw))
    except ValueError:
        list_days = 30
    try:
        list_len = max(1, int(list_len_raw))
    except ValueError:
        list_len = 20

    email_map_raw = os.getenv("CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_EMAIL_MAP", "").strip()
    email_map: dict[str, str] = {}
    if email_map_raw:
        try:
            parsed = json.loads(email_map_raw)
            if isinstance(parsed, dict):
                email_map = {
                    str(k).strip(): str(v).strip()
                    for k, v in parsed.items()
                    if str(k).strip() and str(v).strip()
                }
        except json.JSONDecodeError:
            # Allow simple format: key1=email1,key2=email2
            for pair in email_map_raw.split(","):
                if "=" not in pair:
                    continue
                k, v = pair.split("=", 1)
                key = k.strip()
                val = v.strip()
                if key and val:
                    email_map[key] = val

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
            "/erp/sc/data/mail/lists",
        ).strip(),
        lingxing_list_messages_method=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_METHOD",
            "POST",
        ).strip().upper(),
        lingxing_list_messages_flag_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_FLAG_FIELD",
            "flag",
        ).strip(),
        lingxing_list_messages_flag_value=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_FLAG_VALUE",
            "receive",
        ).strip(),
        lingxing_list_messages_email_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_EMAIL_FIELD",
            "email",
        ).strip(),
        lingxing_list_messages_email_value=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_EMAIL_VALUE",
            "",
        ).strip(),
        lingxing_list_messages_email_map=email_map,
        lingxing_list_messages_start_date_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_START_DATE_FIELD",
            "start_date",
        ).strip(),
        lingxing_list_messages_end_date_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_END_DATE_FIELD",
            "end_date",
        ).strip(),
        lingxing_list_messages_default_days=list_days,
        lingxing_list_messages_offset_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_OFFSET_FIELD",
            "offset",
        ).strip(),
        lingxing_list_messages_length_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_LENGTH_FIELD",
            "length",
        ).strip(),
        lingxing_list_messages_length_value=list_len,
        lingxing_list_messages_store_name_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_STORE_NAME_FIELD",
            "",
        ).strip(),
        lingxing_list_messages_sid_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_LIST_MESSAGES_SID_FIELD",
            "",
        ).strip(),
        lingxing_mail_detail_path=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_MAIL_DETAIL_PATH",
            "/erp/sc/data/mail/detail",
        ).strip(),
        lingxing_mail_detail_method=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_MAIL_DETAIL_METHOD",
            "POST",
        ).strip().upper(),
        lingxing_mail_detail_uuid_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_MAIL_DETAIL_UUID_FIELD",
            "webmail_uuid",
        ).strip(),
        lingxing_mail_detail_email_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_MAIL_DETAIL_EMAIL_FIELD",
            "email",
        ).strip(),
        lingxing_mail_detail_store_name_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_MAIL_DETAIL_STORE_NAME_FIELD",
            "",
        ).strip(),
        lingxing_mail_detail_sid_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_MAIL_DETAIL_SID_FIELD",
            "",
        ).strip(),
        lingxing_send_message_path=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_PATH",
            "/erp/sc/data/mail/lists",
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
        lingxing_send_message_attachments_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_ATTACHMENTS_FIELD",
            "attachments",
        ).strip(),
        lingxing_send_message_attachment_name_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_ATTACHMENT_NAME_FIELD",
            "name",
        ).strip(),
        lingxing_send_message_attachment_content_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_ATTACHMENT_CONTENT_FIELD",
            "content",
        ).strip(),
        lingxing_send_message_attachment_content_type_field=os.getenv(
            "CUSTOMER_SERVICE_LINGXING_SEND_MESSAGE_ATTACHMENT_CONTENT_TYPE_FIELD",
            "content_type",
        ).strip(),
        max_reply_chars=max_reply_chars,
    )

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class IncomingBuyerMessage:
    conversation_id: str
    buyer_message: str
    mailbox_flag: str = "receive"
    subject: str = ""
    from_address: str = ""
    reply_to: str = ""
    message_id: str = ""
    raw_data: dict[str, Any] = field(default_factory=dict)


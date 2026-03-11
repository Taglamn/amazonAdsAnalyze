from __future__ import annotations

AUTO_REPLY_CATEGORIES = {"shipping", "tracking", "product_usage"}


class AutoReplyEngine:
    def should_auto_send(self, category: str, risk_level: str) -> bool:
        return risk_level == "low" and category in AUTO_REPLY_CATEGORIES

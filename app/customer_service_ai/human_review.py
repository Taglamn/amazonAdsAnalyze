from __future__ import annotations

REQUIRES_REVIEW_CATEGORIES = {"damage", "defective", "complaint", "angry_customer"}


class HumanReviewEngine:
    def requires_review(self, category: str, risk_level: str) -> bool:
        return risk_level == "high" or category in REQUIRES_REVIEW_CATEGORIES

from __future__ import annotations

from dataclasses import dataclass

from .llm import CustomerServiceLLM
from .prompts import CATEGORY_OPTIONS, CLASSIFICATION_PROMPT


@dataclass(frozen=True)
class ClassificationResult:
    category: str
    confidence: str


class MessageClassificationService:
    def classify(self, llm: CustomerServiceLLM, buyer_message: str) -> ClassificationResult:
        prompt = CLASSIFICATION_PROMPT.replace("{buyer_message}", buyer_message)
        payload = llm.generate_json(prompt)
        raw_category = str(payload.get("category") or "").strip().lower()
        category = raw_category if raw_category in CATEGORY_OPTIONS else "other"
        confidence = str(payload.get("confidence") or "0").strip() or "0"
        return ClassificationResult(category=category, confidence=confidence)

from __future__ import annotations

from dataclasses import dataclass

from .llm import CustomerServiceLLM
from .prompts import SENTIMENT_OPTIONS, SENTIMENT_PROMPT


@dataclass(frozen=True)
class SentimentResult:
    sentiment: str
    confidence: str


class SentimentAnalysisService:
    def analyze(self, llm: CustomerServiceLLM, buyer_message: str) -> SentimentResult:
        prompt = SENTIMENT_PROMPT.replace("{buyer_message}", buyer_message)
        payload = llm.generate_json(prompt)
        raw_sentiment = str(payload.get("sentiment") or "").strip().lower()
        sentiment = raw_sentiment if raw_sentiment in SENTIMENT_OPTIONS else "neutral"
        confidence = str(payload.get("confidence") or "0").strip() or "0"
        return SentimentResult(sentiment=sentiment, confidence=confidence)

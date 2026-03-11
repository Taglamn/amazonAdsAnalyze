from __future__ import annotations

from .llm import CustomerServiceLLM
from .prompts import FALLBACK_REPLY_PROMPT, REPLY_PROMPT_BY_CATEGORY


class ReplyGenerationService:
    def generate(self, llm: CustomerServiceLLM, buyer_message: str, category: str) -> str:
        prompt = REPLY_PROMPT_BY_CATEGORY.get(category, FALLBACK_REPLY_PROMPT)
        payload = llm.generate_json(prompt.format(buyer_message=buyer_message))
        return str(payload.get("reply") or "").strip()

from __future__ import annotations

from .llm import CustomerServiceLLM
from .prompts import FALLBACK_REPLY_PROMPT, REPLY_PROMPT_BY_CATEGORY


class ReplyGenerationService:
    def generate(self, llm: CustomerServiceLLM, buyer_message: str, category: str) -> str:
        prompt_template = REPLY_PROMPT_BY_CATEGORY.get(category, FALLBACK_REPLY_PROMPT)
        prompt = prompt_template.replace("{buyer_message}", buyer_message)
        payload = llm.generate_json(prompt)
        return str(payload.get("reply") or "").strip()

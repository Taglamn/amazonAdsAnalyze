from __future__ import annotations

import json
import re
from typing import Any
from urllib import error, request

from .config import CustomerServiceSettings, get_customer_service_settings


class LLMGenerationError(RuntimeError):
    pass


class CustomerServiceLLM:
    def __init__(self, settings: CustomerServiceSettings | None = None) -> None:
        self.settings = settings or get_customer_service_settings()

    def generate_json(self, prompt: str) -> dict[str, Any]:
        raw_text = self._generate_text(prompt=prompt)
        return self._safe_parse_json(raw_text)

    def _generate_text(self, prompt: str) -> str:
        if self.settings.llm_provider == "openai":
            return self._call_openai(prompt)
        if self.settings.llm_provider == "gemini":
            return self._call_gemini(prompt)
        raise LLMGenerationError(f"Unsupported LLM provider: {self.settings.llm_provider}")

    def _call_openai(self, prompt: str) -> str:
        api_key = self.settings.openai_api_key
        if not api_key:
            raise LLMGenerationError("OPENAI_API_KEY is not set")

        endpoint = "https://api.openai.com/v1/chat/completions"
        payload = {
            "model": self.settings.llm_model,
            "temperature": 0.1,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Return valid JSON only. "
                        "No markdown, no code fences, no extra explanation."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }

        req = request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=60) as resp:
                response_json = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise LLMGenerationError(f"OpenAI HTTP error {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise LLMGenerationError(f"OpenAI network error: {exc.reason}") from exc

        choices = response_json.get("choices") or []
        if not choices:
            raise LLMGenerationError(f"OpenAI returned no choices: {response_json}")

        text = str(choices[0].get("message", {}).get("content") or "").strip()
        if not text:
            raise LLMGenerationError(f"OpenAI returned empty content: {response_json}")
        return text

    def _call_gemini(self, prompt: str) -> str:
        api_key = self.settings.gemini_api_key
        if not api_key:
            raise LLMGenerationError("GEMINI_API_KEY is not set")

        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.settings.llm_model}"
            f":generateContent?key={api_key}"
        )
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": (
                                "Return valid JSON only. "
                                "No markdown, no code fences, no extra explanation.\n\n"
                                f"{prompt}"
                            )
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 512,
            },
        }

        req = request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=60) as resp:
                response_json = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise LLMGenerationError(f"Gemini HTTP error {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise LLMGenerationError(f"Gemini network error: {exc.reason}") from exc

        candidates = response_json.get("candidates") or []
        if not candidates:
            raise LLMGenerationError(f"Gemini returned no candidates: {response_json}")

        parts = candidates[0].get("content", {}).get("parts") or []
        text = "\n".join(str(part.get("text") or "") for part in parts).strip()
        if not text:
            raise LLMGenerationError(f"Gemini returned empty content: {response_json}")
        return text

    def sanitize_reply(self, text: str) -> str:
        reply = (text or "").replace("\r\n", "\n").strip()
        reply = re.sub(r"https?://\S+", "", reply, flags=re.IGNORECASE)
        reply = re.sub(r"www\.\S+", "", reply, flags=re.IGNORECASE)
        reply = re.sub(r"\s+", " ", reply).strip()

        blocked_patterns = (
            "leave a review",
            "leave us a review",
            "5-star",
            "5 star",
            "rating",
            "discount",
            "promotion",
        )
        lowered = reply.lower()
        if any(pattern in lowered for pattern in blocked_patterns):
            reply = "Thank you for your message. We are here to help and will assist you right away."

        if not reply:
            reply = "Thank you for your message. Could you share a bit more detail so we can help quickly?"

        if len(reply) > self.settings.max_reply_chars:
            reply = reply[: self.settings.max_reply_chars].rstrip()
        return reply

    @staticmethod
    def _safe_parse_json(raw_text: str) -> dict[str, Any]:
        cleaned = (raw_text or "").strip()
        if not cleaned:
            return {}

        cleaned = cleaned.replace("```json", "").replace("```", "").strip()

        try:
            payload = json.loads(cleaned)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            return {}

        snippet = match.group(0)
        try:
            payload = json.loads(snippet)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Dict, List
from urllib import error, request

from .data_access import PLAYBOOK_DIR


def load_playbook(store_id: str) -> Dict[str, Any]:
    path = PLAYBOOK_DIR / f"store_playbook_{store_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Playbook file not found: {path.name}")

    with path.open("r", encoding="utf-8") as f:
        playbook = json.load(f)

    if str(playbook.get("store_id")) != store_id:
        raise ValueError("Playbook store_id mismatch")

    return playbook


def validate_metrics_store(metrics: Dict[str, Any], store_id: str) -> None:
    metric_store_id = str(metrics.get("store_id", ""))
    if metric_store_id != store_id:
        raise ValueError(
            f"Metrics store_id ({metric_store_id}) does not match playbook store_id ({store_id})"
        )


def normalize_language(language: str | None) -> str:
    if not language:
        return "zh"

    lowered = language.strip().lower()
    if lowered.startswith("zh"):
        return "zh"
    if lowered.startswith("en"):
        return "en"
    raise ValueError("Unsupported language. Use zh or en.")


def _language_directive(language: str) -> str:
    if language == "zh":
        return (
            "Respond in Simplified Chinese using local Amazon ads operator terms, "
            "such as 出价, 否定关键词, 预算倾斜, 搜索词清洗, 控损, 放量."
        )
    return (
        "Respond in clear business English using Amazon ads terms such as bid, "
        "negative keyword, budget shift, search term cleanup, and scaling."
    )


def build_advice_prompt(
    store_id: str,
    rules: Dict[str, Any],
    metrics: Dict[str, Any],
    language: str = "zh",
) -> str:
    language = normalize_language(language)

    return (
        f"You are the expert for Store {store_id}. "
        f"Based on these rules {json.dumps(rules, ensure_ascii=False)} and yesterday's metrics "
        f"{json.dumps(metrics, ensure_ascii=False)}, provide 3 specific bid adjustments "
        f"and 1 negative keyword suggestion. Return concise bullet points. "
        f"{_language_directive(language)}"
    )


def build_whitepaper_prompt(
    store_id: str,
    rules: Dict[str, Any],
    performance_rows: List[Dict[str, Any]],
    cases: List[Dict[str, Any]],
    language: str = "zh",
) -> str:
    language = normalize_language(language)

    sections = (
        "Current Diagnosis, Bid Strategy, Budget Reallocation, Search Term Hygiene, "
        "7-Day Action Plan, Risks"
    )
    if language == "zh":
        sections = "现状诊断、出价策略、预算倾斜、搜索词清洗、7天行动计划、风险提示"

    return (
        f"You are an Amazon Ads strategist for Store {store_id}. "
        f"Create an optimization strategy whitepaper with sections: {sections}. "
        f"Follow playbook rules {json.dumps(rules, ensure_ascii=False)}. "
        f"Use performance data {json.dumps(performance_rows, ensure_ascii=False)} and historical cases "
        f"{json.dumps(cases, ensure_ascii=False)}. Keep it actionable and numeric. "
        f"{_language_directive(language)}"
    )


def call_gemini(prompt: str, model: str = "gemini-1.5-flash") -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")

    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 1024},
    }

    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=40) as resp:
            response_json = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Gemini API HTTP error {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Gemini API network error: {exc.reason}") from exc

    candidates = response_json.get("candidates", [])
    if not candidates:
        raise RuntimeError(f"Gemini API returned no candidates: {response_json}")

    parts = candidates[0].get("content", {}).get("parts", [])
    text = "\n".join(part.get("text", "") for part in parts if part.get("text"))
    if not text:
        raise RuntimeError(f"Gemini API returned empty text: {response_json}")

    return text


def yesterday_metrics_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        raise ValueError("No performance rows available")

    latest = max(rows, key=lambda x: date.fromisoformat(str(x["date"])))
    return latest

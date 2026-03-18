from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Dict, List, Tuple
from urllib import error, request

from .data_access import PLAYBOOK_DIR

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


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
    whitepaper_context: str = "",
    language: str = "zh",
) -> str:
    language = normalize_language(language)

    whitepaper_part = ""
    if whitepaper_context.strip():
        whitepaper_part = (
            "First read and strictly follow this store Lingxing auto-rule blueprint before giving advice.\n"
            "Store Lingxing auto-rule blueprint:\n"
            "<<WHITEPAPER_START>>\n"
            f"{whitepaper_context}\n"
            "<<WHITEPAPER_END>>\n"
        )

    return (
        f"You are the expert for Store {store_id}. "
        f"{whitepaper_part}"
        f"Based on these playbook rules {json.dumps(rules, ensure_ascii=False)} and latest metrics "
        f"{json.dumps(metrics, ensure_ascii=False)}, output optimization changes for Lingxing auto rules. "
        "Return markdown with these sections: "
        "Rule Change Summary, Rule-by-Rule Adjustments, Risk Controls, and 7-Day Validation Plan. "
        "For each adjusted rule, include: rule_name, what_changed, trigger/threshold change, action change, expected impact. "
        "Also include exactly one JSON code block named `rule_patch` with format: "
        "{\"store_id\":\"...\",\"updates\":[{\"rule_name\":\"...\",\"operation\":\"add|update|disable\","
        "\"fields\":{\"conditions\":[],\"action\":{},\"frequency_days\":1},\"reason\":\"...\"}]}. "
        "If no change is needed, return an empty updates array with reason. "
        "Do not output a single-line response. "
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

    return (
        f"You are an Amazon Ads automation strategist for Store {store_id}. "
        "Generate a Lingxing Auto-Rule Blueprint that operations can configure directly in Lingxing ERP. "
        f"Follow playbook rules {json.dumps(rules, ensure_ascii=False)}. "
        f"Use performance data {json.dumps(performance_rows, ensure_ascii=False)} and historical cases {json.dumps(cases, ensure_ascii=False)}. "
        "Auto-rule constraints to follow: include ad type/campaign type/targeting strategy scope, "
        "define execution mode (all conditions or any condition), each rule can have up to 5 conditions, "
        "and include execution frequency (manual or every N days). "
        "Output must be markdown with sections in this order: "
        "1) Rule Design Principles, 2) Rule Catalog, 3) Implementation Checklist, 4) Monitoring KPI & Rollback. "
        "Rule Catalog must be a table with columns: "
        "rule_name, objective, apply_scope, trigger_window, conditions, action, frequency_days, priority, guardrail. "
        "Then output exactly one JSON code block named `lingxing_auto_rules` in this schema: "
        "{\"store_id\":\"...\",\"generated_at\":\"YYYY-MM-DD\",\"rules\":["
        "{\"rule_name\":\"...\",\"ad_type\":\"SP|SB|SD\",\"campaign_type\":\"auto|manual|all\","
        "\"targeting_strategy\":\"keyword|product|audience|all\","
        "\"apply_scope\":{\"campaign_names\":[],\"ad_group_names\":[]},"
        "\"execution_mode\":\"all_match|any_match\","
        "\"time_window_mode\":\"yesterday|last_7_days|last_14_days|last_30_days\","
        "\"conditions\":[{\"metric\":\"acos|ctr|clicks|cpc|spend|sales|top_of_search_is\",\"operator\":\">|<|>=|<=|=\",\"value\":0}],"
        "\"action\":{\"type\":\"adjust_bid_pct|adjust_placement_pct|add_negative_keyword|pause_target|budget_shift_pct\",\"value\":0},"
        "\"frequency_days\":1,\"priority\":\"high|medium|low\",\"notes\":\"...\"}]}. "
        "Keep every rule specific and numeric so ops can copy into Lingxing without rewriting. "
        f"{_language_directive(language)}"
    )


def resolve_gemini_model(model: str | None = None) -> str:
    if load_dotenv is not None:
        load_dotenv()

    resolved = (model or "").strip() or os.getenv("GEMINI_MODEL", "").strip() or "gemini-2.5-flash"
    return resolved


def resolve_max_output_tokens() -> int:
    if load_dotenv is not None:
        load_dotenv()

    raw = os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "").strip()
    if not raw:
        return 8192
    try:
        value = int(raw)
    except ValueError:
        return 8192
    return max(512, min(value, 16384))


def resolve_gemini_continuation_rounds() -> int:
    if load_dotenv is not None:
        load_dotenv()

    raw = os.getenv("GEMINI_CONTINUATION_ROUNDS", "").strip()
    if not raw:
        return 6
    try:
        value = int(raw)
    except ValueError:
        return 6
    return max(0, min(12, value))


def _normalize_gemini_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    if "\\n" in normalized:
        normalized = normalized.replace("\\n", "\n")
    if "\\t" in normalized:
        normalized = normalized.replace("\\t", "\t")
    return normalized


def _call_gemini_once(
    prompt: str,
    resolved_model: str,
    api_key: str,
    max_output_tokens: int,
) -> Tuple[str, str | None, Dict[str, Any]]:
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{resolved_model}:generateContent?key={api_key}"
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": max_output_tokens},
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
        raise RuntimeError(f"Gemini API HTTP error {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Gemini API network error: {exc.reason}") from exc

    candidates = response_json.get("candidates", [])
    if not candidates:
        raise RuntimeError(f"Gemini API returned no candidates: {response_json}")

    first_candidate = candidates[0]
    parts = first_candidate.get("content", {}).get("parts", [])
    text = "\n".join(part.get("text", "") for part in parts if part.get("text"))
    if not text:
        raise RuntimeError(f"Gemini API returned empty text: {response_json}")

    usage = response_json.get("usageMetadata") or {}
    finish_reason = first_candidate.get("finishReason")
    return _normalize_gemini_text(text), finish_reason, usage


def _append_without_overlap(current: str, extra: str) -> str:
    base = current.rstrip()
    incoming = extra.lstrip()
    if not base:
        return incoming
    if not incoming:
        return base

    max_overlap = min(len(base), len(incoming), 400)
    overlap = 0
    for size in range(max_overlap, 0, -1):
        if base[-size:] == incoming[:size]:
            overlap = size
            break

    merged = base + "\n" + incoming[overlap:].lstrip()
    return merged


def call_gemini_with_meta(prompt: str, model: str | None = None) -> Tuple[str, Dict[str, Any]]:
    if load_dotenv is not None:
        load_dotenv()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")

    resolved_model = resolve_gemini_model(model)
    max_output_tokens = resolve_max_output_tokens()
    continuation_rounds = resolve_gemini_continuation_rounds()

    text, finish_reason, usage = _call_gemini_once(
        prompt=prompt,
        resolved_model=resolved_model,
        api_key=api_key,
        max_output_tokens=max_output_tokens,
    )

    finish_reasons = [finish_reason]
    total_prompt_tokens = int(usage.get("promptTokenCount", 0) or 0)
    total_candidate_tokens = int(usage.get("candidatesTokenCount", 0) or 0)
    total_tokens = int(usage.get("totalTokenCount", 0) or 0)

    round_count = 1
    while round_count <= continuation_rounds and finish_reason == "MAX_TOKENS":
        tail_excerpt = text[-4000:] if len(text) > 4000 else text
        continue_prompt = (
            "Continue the same response from exactly where it stopped.\n"
            "Do not repeat prior sentences.\n"
            "Return only the continuation text.\n\n"
            f"Original request:\n{prompt}\n\n"
            f"Current partial response tail (latest part):\n{tail_excerpt}\n"
        )

        extra_text, finish_reason, extra_usage = _call_gemini_once(
            prompt=continue_prompt,
            resolved_model=resolved_model,
            api_key=api_key,
            max_output_tokens=max_output_tokens,
        )

        text = _append_without_overlap(text, extra_text)
        finish_reasons.append(finish_reason)
        total_prompt_tokens += int(extra_usage.get("promptTokenCount", 0) or 0)
        total_candidate_tokens += int(extra_usage.get("candidatesTokenCount", 0) or 0)
        total_tokens += int(extra_usage.get("totalTokenCount", 0) or 0)
        round_count += 1

    meta: Dict[str, Any] = {
        "model": resolved_model,
        "max_output_tokens": max_output_tokens,
        "continuation_rounds": continuation_rounds,
        "rounds_used": len(finish_reasons),
        "finish_reason": finish_reasons[-1],
        "finish_reasons": finish_reasons,
        "prompt_token_count": total_prompt_tokens,
        "candidates_token_count": total_candidate_tokens,
        "total_token_count": total_tokens,
        "char_count": len(text),
        "line_count": text.count("\n") + 1,
        "truncated": finish_reasons[-1] == "MAX_TOKENS",
    }

    return text, meta


def call_gemini(prompt: str, model: str | None = None) -> str:
    text, _ = call_gemini_with_meta(prompt=prompt, model=model)

    return text


def yesterday_metrics_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        raise ValueError("No performance rows available")

    latest = max(rows, key=lambda x: date.fromisoformat(str(x["date"])))
    return latest

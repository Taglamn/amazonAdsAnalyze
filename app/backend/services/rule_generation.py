from __future__ import annotations

from typing import Any, Dict, List


def _bid_adjustment_text(pct: float) -> str:
    rounded = round(float(pct), 2)
    sign = "+" if rounded > 0 else ""
    return f"{sign}{rounded}%"


def to_lingxing_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    condition = rule.get("condition", {})
    action = rule.get("action", {})
    bid_adjustment_pct = float(action.get("bid_adjustment_pct", 0.0))

    lingxing_rule = {
        "rule_name": rule.get("rule_name"),
        "scope": {
            "ad_type": "SP",
            "campaign_type": "all",
            "targeting_strategy": "keyword",
        },
        "execution_mode": "all_match",
        "frequency_days": 1,
        "condition": condition,
        "action": {
            "bid_adjustment": _bid_adjustment_text(bid_adjustment_pct),
            "bid_adjustment_pct": round(bid_adjustment_pct, 2),
        },
        "guardrails": {
            "max_daily_adjustment_pct": 30,
            "cooldown_days": 2,
            "manual_review_when_spend_spike_pct": 35,
        },
    }

    return {
        "rule_id": rule.get("rule_id"),
        "store_id": rule.get("store_id"),
        "rule_name": rule.get("rule_name"),
        "condition": condition,
        "action": action,
        "lingxing_rule": lingxing_rule,
        "source": rule.get("source", "learned"),
        "status": rule.get("status", "active"),
        "confidence": float(rule.get("confidence", 0.0)),
        "win_rate": float(rule.get("win_rate", 0.0)),
        "sample_size": int(rule.get("sample_size", 0)),
        "bid_adjustment_pct": round(bid_adjustment_pct, 2),
        "updated_at": rule.get("updated_at", ""),
    }


def build_lingxing_rules(rules: List[Dict[str, Any]], max_rules: int) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for row in rules[:max_rules]:
        result.append(to_lingxing_rule(row))
    return result

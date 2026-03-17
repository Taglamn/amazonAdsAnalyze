from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def evaluate_single_observation(row: Dict[str, Any]) -> Dict[str, Any]:
    before_acos = float(row.get("before_acos", 0.0))
    after_acos = float(row.get("after_acos", 0.0))
    before_clicks = float(row.get("before_clicks", 0.0))
    after_clicks = float(row.get("after_clicks", 0.0))
    before_cvr = float(row.get("before_cvr", 0.0))
    after_cvr = float(row.get("after_cvr", 0.0))

    effect = "neutral"
    suggestion = "Keep current rule settings."
    action_patch: Dict[str, Any] = {}
    condition_patch: Dict[str, Any] = {}

    if after_acos <= before_acos * 0.92 and after_clicks >= before_clicks * 0.9:
        effect = "effective"
        suggestion = "Rule is effective. You can expand traffic slightly (+2% bid adjustment)."
        action_patch["bid_adjustment_pct_delta"] = 2.0
    elif after_acos >= before_acos * 1.10 and after_cvr <= before_cvr * 0.95:
        effect = "harmful"
        suggestion = "Rule appears harmful. Reduce adjustment strength and tighten clicks threshold."
        action_patch["bid_adjustment_pct_scale"] = 0.5
        condition_patch["clicks"] = "> 25"
    else:
        effect = "neutral"
        suggestion = "Rule effect is neutral. Keep adjustment but observe for 7 more days."

    return {
        "rule_id": str(row["rule_id"]),
        "before_acos": before_acos,
        "after_acos": after_acos,
        "before_clicks": before_clicks,
        "after_clicks": after_clicks,
        "before_cvr": before_cvr,
        "after_cvr": after_cvr,
        "effect": effect,
        "suggestion": suggestion,
        "detail": {
            "delta_acos": round(after_acos - before_acos, 4),
            "delta_clicks": round(after_clicks - before_clicks, 2),
            "delta_cvr": round(after_cvr - before_cvr, 6),
        },
        "action_patch": action_patch,
        "condition_patch": condition_patch,
        "evaluated_at": _utc_now_iso(),
    }


def evaluate_observations(observations: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for row in observations:
        results.append(evaluate_single_observation(row))
    return results


def merge_action_patch(current_action: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    if not patch:
        return dict(current_action)
    action = dict(current_action)
    current_pct = float(action.get("bid_adjustment_pct", 0.0))
    if "bid_adjustment_pct_scale" in patch:
        current_pct = current_pct * float(patch["bid_adjustment_pct_scale"])
    if "bid_adjustment_pct_delta" in patch:
        current_pct = current_pct + float(patch["bid_adjustment_pct_delta"])
    action["bid_adjustment_pct"] = round(max(min(current_pct, 30.0), -30.0), 2)
    return action

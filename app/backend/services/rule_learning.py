from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def _condition_from_levels(row: pd.Series) -> Dict[str, Any]:
    acos_level = str(row["acos_level"])
    traffic_level = str(row["traffic_level"])
    conversion_level = str(row["conversion_level"])
    upper = float(row["upper_acos"])
    lower = float(row["lower_acos"])

    acos_condition = "lower_acos <= acos <= upper_acos"
    if acos_level == "high":
        acos_condition = f"acos > {upper:.2f}"
    elif acos_level == "low":
        acos_condition = f"acos < {lower:.2f}"

    clicks_condition = "clicks <= 20"
    if traffic_level == "high_clicks":
        clicks_condition = "clicks > 20"

    cvr_condition = "cvr < 0.10"
    if conversion_level == "high_cvr":
        cvr_condition = "cvr >= 0.10"

    return {
        "acos": acos_condition,
        "clicks": clicks_condition,
        "cvr": cvr_condition,
        "acos_level": acos_level,
        "traffic_level": traffic_level,
        "conversion_level": conversion_level,
    }


def _rule_name(action: str, acos_level: str, traffic_level: str) -> str:
    if action == "decrease_bid":
        return f"Reduce Bid | {acos_level} ACOS | {traffic_level}"
    if action == "increase_bid":
        return f"Increase Bid | {acos_level} ACOS | {traffic_level}"
    return f"Fine Tune Bid | {acos_level} ACOS | {traffic_level}"


def learn_rules_from_samples(
    store_id: str,
    samples_df: pd.DataFrame,
    min_samples: int,
    min_win_rate: float,
    max_rules: int,
) -> List[Dict[str, Any]]:
    if samples_df.empty:
        return []

    grouped = (
        samples_df.groupby(
            ["action", "acos_level", "traffic_level", "conversion_level", "upper_acos", "lower_acos"],
            as_index=False,
        )
        .agg(
            sample_size=("performance_change", "count"),
            better_count=("performance_change", lambda s: int((s == "better").sum())),
            neutral_count=("performance_change", lambda s: int((s == "neutral").sum())),
            avg_bid_change_pct=("bid_change_pct", "mean"),
            median_bid_change_pct=("bid_change_pct", "median"),
        )
        .sort_values(["sample_size", "better_count"], ascending=[False, False])
    )
    if grouped.empty:
        return []

    rules: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        sample_size = int(row["sample_size"])
        if sample_size < min_samples:
            continue
        win_rate = float(row["better_count"]) / float(sample_size)
        if win_rate < min_win_rate:
            continue
        action = str(row["action"])
        raw_adjustment = float(row["median_bid_change_pct"])
        if action == "decrease_bid":
            bid_adjustment_pct = -abs(raw_adjustment or -10.0)
        elif action == "increase_bid":
            bid_adjustment_pct = abs(raw_adjustment or 10.0)
        else:
            bid_adjustment_pct = 0.0
        bid_adjustment_pct = max(min(bid_adjustment_pct, 30.0), -30.0)
        condition = _condition_from_levels(row)
        rule_name = _rule_name(
            action=action,
            acos_level=str(row["acos_level"]),
            traffic_level=str(row["traffic_level"]),
        )
        confidence = min(0.99, round(win_rate * min(1.0, sample_size / 20.0), 4))
        rules.append(
            {
                "store_id": store_id,
                "rule_name": rule_name,
                "condition": condition,
                "action": {"bid_adjustment_pct": round(bid_adjustment_pct, 2), "action_type": action},
                "source": "learned",
                "status": "active",
                "confidence": confidence,
                "win_rate": round(win_rate, 4),
                "sample_size": sample_size,
                "bid_adjustment_pct": round(bid_adjustment_pct, 2),
            }
        )
        if len(rules) >= max_rules:
            break
    return rules


def build_strategy_rules(
    store_id: str,
    default_strategy: Dict[str, float],
) -> List[Dict[str, Any]]:
    upper = float(default_strategy["upper_acos"])
    lower = float(default_strategy["lower_acos"])
    target = float(default_strategy["target_acos"])
    return [
        {
            "store_id": store_id,
            "rule_name": "Strategy | High ACOS Reduce Bid",
            "condition": {"acos": f"> {upper:.2f}", "source": "strategy_layer"},
            "action": {"bid_adjustment_pct": -10.0, "action_type": "decrease_bid"},
            "source": "strategy",
            "status": "active",
            "confidence": 0.9,
            "win_rate": 0.9,
            "sample_size": 0,
            "bid_adjustment_pct": -10.0,
        },
        {
            "store_id": store_id,
            "rule_name": "Strategy | Low ACOS Increase Bid",
            "condition": {"acos": f"< {lower:.2f}", "source": "strategy_layer"},
            "action": {"bid_adjustment_pct": 8.0, "action_type": "increase_bid"},
            "source": "strategy",
            "status": "active",
            "confidence": 0.9,
            "win_rate": 0.9,
            "sample_size": 0,
            "bid_adjustment_pct": 8.0,
        },
        {
            "store_id": store_id,
            "rule_name": "Strategy | ACOS In Target Keep/Fine Tune",
            "condition": {"acos": f"{lower:.2f}~{upper:.2f}", "target_acos": target, "source": "strategy_layer"},
            "action": {"bid_adjustment_pct": 0.0, "action_type": "keep_bid"},
            "source": "strategy",
            "status": "active",
            "confidence": 0.85,
            "win_rate": 0.85,
            "sample_size": 0,
            "bid_adjustment_pct": 0.0,
        },
    ]

from __future__ import annotations

from typing import Any, Dict, List, Tuple

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


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _apply_click_threshold_filter(samples_df: pd.DataFrame, min_clicks_threshold: int) -> pd.DataFrame:
    if samples_df.empty or min_clicks_threshold <= 0:
        return samples_df
    filtered = samples_df[samples_df["before_clicks"] >= float(min_clicks_threshold)].copy()
    return filtered


def _apply_iqr_outlier_filter(samples_df: pd.DataFrame, outlier_iqr_k: float) -> Tuple[pd.DataFrame, int]:
    if samples_df.empty:
        return samples_df, 0

    numeric_cols = [
        "bid_change_pct",
        "before_acos",
        "after_acos",
        "before_clicks",
        "after_clicks",
        "before_cvr",
        "after_cvr",
    ]

    mask = pd.Series([True] * len(samples_df), index=samples_df.index)
    for col in numeric_cols:
        if col not in samples_df.columns:
            continue
        series = pd.to_numeric(samples_df[col], errors="coerce")
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr <= 0:
            continue
        lower = q1 - outlier_iqr_k * iqr
        upper = q3 + outlier_iqr_k * iqr
        mask = mask & (series >= lower) & (series <= upper)

    filtered = samples_df[mask].copy()
    removed = int(len(samples_df) - len(filtered))
    return filtered, removed


def _prepare_learning_df(
    samples_df: pd.DataFrame,
    min_clicks_threshold: int,
    enable_outlier_filter: bool,
    outlier_iqr_k: float,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if samples_df.empty:
        return samples_df, {"raw_samples": 0, "click_filtered_samples": 0, "outliers_removed": 0, "used_samples": 0}

    raw_count = len(samples_df)
    click_filtered_df = _apply_click_threshold_filter(samples_df, min_clicks_threshold=min_clicks_threshold)
    click_filtered_count = len(click_filtered_df)

    outliers_removed = 0
    final_df = click_filtered_df
    if enable_outlier_filter:
        final_df, outliers_removed = _apply_iqr_outlier_filter(
            click_filtered_df,
            outlier_iqr_k=outlier_iqr_k,
        )

    return final_df, {
        "raw_samples": raw_count,
        "click_filtered_samples": click_filtered_count,
        "outliers_removed": outliers_removed,
        "used_samples": len(final_df),
    }


def _label_score(performance_change: str) -> float:
    if performance_change == "better":
        return 1.0
    if performance_change == "neutral":
        return 0.5
    return 0.0


def _sample_weight(before_cvr: float, after_cvr: float, cvr_weight: float) -> float:
    baseline = max(before_cvr, 1e-6)
    cvr_delta_ratio = (after_cvr - before_cvr) / baseline
    # Keep weighting stable and avoid extreme CVR swings dominating rules.
    bounded = _clip(cvr_delta_ratio, -0.8, 1.5)
    return _clip(1.0 + cvr_weight * bounded, 0.2, 3.0)


def _weighted_win_rate(group_df: pd.DataFrame, cvr_weight: float) -> float:
    if group_df.empty:
        return 0.0

    weighted_success = 0.0
    total_weight = 0.0
    for _, row in group_df.iterrows():
        weight = _sample_weight(
            before_cvr=float(row.get("before_cvr", 0.0)),
            after_cvr=float(row.get("after_cvr", 0.0)),
            cvr_weight=cvr_weight,
        )
        score = _label_score(str(row.get("performance_change", "neutral")))
        weighted_success += score * weight
        total_weight += weight
    if total_weight <= 0:
        return 0.0
    return weighted_success / total_weight


def learn_rules_from_samples(
    store_id: str,
    samples_df: pd.DataFrame,
    min_samples: int,
    min_win_rate: float,
    max_rules: int,
    min_clicks_threshold: int = 10,
    cvr_weight: float = 0.35,
    outlier_iqr_k: float = 1.5,
    enable_outlier_filter: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    if samples_df.empty:
        return [], {
            "raw_samples": 0,
            "click_filtered_samples": 0,
            "outliers_removed": 0,
            "used_samples": 0,
        }

    working_df, learning_stats = _prepare_learning_df(
        samples_df=samples_df,
        min_clicks_threshold=min_clicks_threshold,
        enable_outlier_filter=enable_outlier_filter,
        outlier_iqr_k=outlier_iqr_k,
    )
    if working_df.empty:
        return [], learning_stats

    grouped = (
        working_df.groupby(
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
        return [], learning_stats

    rules: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        sample_size = int(row["sample_size"])
        if sample_size < min_samples:
            continue
        scenario_df = working_df[
            (working_df["action"] == row["action"])
            & (working_df["acos_level"] == row["acos_level"])
            & (working_df["traffic_level"] == row["traffic_level"])
            & (working_df["conversion_level"] == row["conversion_level"])
            & (working_df["upper_acos"] == row["upper_acos"])
            & (working_df["lower_acos"] == row["lower_acos"])
        ]
        win_rate = _weighted_win_rate(scenario_df, cvr_weight=cvr_weight)
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
        condition["min_clicks_threshold"] = int(min_clicks_threshold)
        condition["outlier_filter"] = (
            {"enabled": bool(enable_outlier_filter), "iqr_k": float(outlier_iqr_k)}
            if enable_outlier_filter
            else {"enabled": False}
        )
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
                "cvr_weight": float(cvr_weight),
            }
        )
        if len(rules) >= max_rules:
            break
    return rules, learning_stats


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

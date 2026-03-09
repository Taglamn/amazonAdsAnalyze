from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List

import pandas as pd


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 4)


def _safe_pct_change(before: float | None, after: float | None) -> float | None:
    if before is None or before == 0 or after is None:
        return None
    return round(((after - before) / before) * 100, 2)


def _window_summary(df: pd.DataFrame) -> Dict[str, float | None]:
    spend = float(df["spend"].sum())
    sales = float(df["sales"].sum())
    clicks = int(df["clicks"].sum())
    acos = round((spend / sales) * 100, 2) if sales else None
    roi = _safe_ratio(sales, spend)

    return {
        "spend": round(spend, 2),
        "sales": round(sales, 2),
        "clicks": clicks,
        "acos": acos,
        "roi": roi,
    }


def _assert_store_scope(store_id: str, history_df: pd.DataFrame, perf_df: pd.DataFrame) -> None:
    history_ids = set(history_df["store_id"].astype(str).unique())
    perf_ids = set(perf_df["store_id"].astype(str).unique())
    if history_ids != {store_id} or perf_ids != {store_id}:
        raise ValueError(
            f"Store scope mismatch. expected={store_id}, history={history_ids}, perf={perf_ids}"
        )


def analyze_impact(target_date: date, history_df: pd.DataFrame, perf_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Identify bid changes on target_date and compare 7-day performance before/after.
    Returns a JSON-like dict describing ROI impact.
    """
    bid_changes = history_df[
        (history_df["date"] == target_date)
        & (history_df["action_type"].str.contains("bid", case=False, na=False))
    ]

    if bid_changes.empty:
        return {
            "target_date": target_date.isoformat(),
            "has_bid_change": False,
            "message": "No bid changes were found on target_date.",
        }

    first_change = bid_changes.iloc[0]

    before_start = target_date - timedelta(days=7)
    before_end = target_date - timedelta(days=1)
    after_start = target_date + timedelta(days=1)
    after_end = target_date + timedelta(days=7)

    before_df = perf_df[(perf_df["date"] >= before_start) & (perf_df["date"] <= before_end)]
    after_df = perf_df[(perf_df["date"] >= after_start) & (perf_df["date"] <= after_end)]

    before_summary = _window_summary(before_df)
    after_summary = _window_summary(after_df)

    roi_change_pct = _safe_pct_change(before_summary["roi"], after_summary["roi"])
    acos_change_pct = _safe_pct_change(before_summary["acos"], after_summary["acos"])

    return {
        "target_date": target_date.isoformat(),
        "has_bid_change": True,
        "change_event": {
            "ad_group": str(first_change["ad_group"]),
            "action_type": str(first_change["action_type"]),
            "old_bid": float(first_change["old_bid"]),
            "new_bid": float(first_change["new_bid"]),
        },
        "before_window": {
            "start": before_start.isoformat(),
            "end": before_end.isoformat(),
            **before_summary,
        },
        "after_window": {
            "start": after_start.isoformat(),
            "end": after_end.isoformat(),
            **after_summary,
        },
        "impact": {
            "roi_change_pct": roi_change_pct,
            "acos_change_pct": acos_change_pct,
        },
    }


def build_optimization_cases(
    store_id: str, history_df: pd.DataFrame, perf_df: pd.DataFrame
) -> List[Dict[str, Any]]:
    _assert_store_scope(store_id, history_df, perf_df)

    bid_change_dates = (
        history_df[history_df["action_type"].str.contains("bid", case=False, na=False)]["date"]
        .dropna()
        .sort_values()
        .unique()
    )

    cases: List[Dict[str, Any]] = []
    for index, target_date in enumerate(bid_change_dates, start=1):
        case = analyze_impact(target_date, history_df, perf_df)
        case["case_id"] = f"case_{index}"
        cases.append(case)

    return cases


def build_bid_recommendations(
    store_id: str, history_df: pd.DataFrame, perf_df: pd.DataFrame
) -> List[Dict[str, Any]]:
    _assert_store_scope(store_id, history_df, perf_df)

    latest_date = perf_df["date"].max()
    latest_snapshot = perf_df[perf_df["date"] == latest_date].iloc[0]
    latest_acos = float(latest_snapshot["acos"])

    latest_by_ad_group = (
        history_df.sort_values(["date"]).groupby("ad_group", as_index=False).tail(1)
    )

    recommendations: List[Dict[str, Any]] = []
    for _, row in latest_by_ad_group.iterrows():
        current_bid = float(row["new_bid"])

        if latest_acos > 45:
            factor = 0.85
            reason = "ACoS above 45%, reduce bid aggressively."
        elif latest_acos > 30:
            factor = 0.92
            reason = "ACoS above 30%, reduce bid moderately."
        elif latest_acos < 20:
            factor = 1.10
            reason = "ACoS below 20%, room to scale traffic."
        else:
            factor = 1.00
            reason = "ACoS in target range, hold bid."

        recommendations.append(
            {
                "ad_group": str(row["ad_group"]),
                "current_bid": round(current_bid, 2),
                "suggested_bid": round(current_bid * factor, 2),
                "latest_acos": round(latest_acos, 2),
                "reason": reason,
            }
        )

    return recommendations

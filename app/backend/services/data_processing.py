from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd

from ..utils import parse_date, summarize_window


def _evaluate_performance_change(
    action: str,
    before_acos: float,
    after_acos: float,
    before_clicks: float,
    after_clicks: float,
    before_cvr: float,
    after_cvr: float,
) -> str:
    if action == "increase_bid":
        if after_clicks >= before_clicks * 1.05 and after_acos <= before_acos * 1.12:
            return "better"
        if after_acos >= before_acos * 1.15 and after_cvr <= before_cvr * 0.95:
            return "worse"
        return "neutral"

    if action == "decrease_bid":
        if after_acos <= before_acos * 0.92 and after_clicks >= before_clicks * 0.85:
            return "better"
        if after_clicks <= before_clicks * 0.75 and after_cvr <= before_cvr * 0.95:
            return "worse"
        return "neutral"

    if after_acos < before_acos or after_cvr > before_cvr:
        return "better"
    if after_acos > before_acos and after_cvr < before_cvr:
        return "worse"
    return "neutral"


def _window_df(
    perf_df: pd.DataFrame,
    ad_group_id: int,
    keyword_id: str,
    start_day: str,
    end_day: str,
) -> pd.DataFrame:
    scoped = perf_df[
        (perf_df["ad_group_id"] == ad_group_id)
        & (perf_df["date"] >= start_day)
        & (perf_df["date"] <= end_day)
    ]
    if scoped.empty:
        return scoped
    if keyword_id and keyword_id != "__all__" and "keyword_id" in scoped.columns:
        exact = scoped[scoped["keyword_id"] == keyword_id]
        if not exact.empty:
            return exact
    return scoped


def build_processed_samples(
    bid_changes_df: pd.DataFrame,
    perf_df: pd.DataFrame,
    window_days: int = 3,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if bid_changes_df.empty or perf_df.empty:
        empty_df = pd.DataFrame(
            columns=[
                "store_id",
                "ad_group_id",
                "keyword_id",
                "change_time",
                "action",
                "bid_change_pct",
                "before_acos",
                "after_acos",
                "before_clicks",
                "after_clicks",
                "before_cvr",
                "after_cvr",
                "performance_change",
            ]
        )
        return empty_df, {"matched_changes": 0, "dropped_changes": 0}

    bid_df = bid_changes_df.copy()
    perf = perf_df.copy()
    bid_df["change_day"] = bid_df["change_time"].astype(str).str.slice(0, 10)
    perf["date"] = perf["date"].astype(str)
    perf["ad_group_id"] = pd.to_numeric(perf["ad_group_id"], errors="coerce").fillna(-1).astype(int)

    rows: List[Dict[str, Any]] = []
    dropped = 0
    for _, action_row in bid_df.iterrows():
        ad_group_id = int(action_row["ad_group_id"])
        keyword_id = str(action_row.get("keyword_id") or "__all__")
        change_day = parse_date(str(action_row["change_day"]))
        before_start = (change_day - timedelta(days=window_days)).isoformat()
        before_end = (change_day - timedelta(days=1)).isoformat()
        after_start = (change_day + timedelta(days=1)).isoformat()
        after_end = (change_day + timedelta(days=window_days)).isoformat()

        before_df = _window_df(
            perf_df=perf,
            ad_group_id=ad_group_id,
            keyword_id=keyword_id,
            start_day=before_start,
            end_day=before_end,
        )
        after_df = _window_df(
            perf_df=perf,
            ad_group_id=ad_group_id,
            keyword_id=keyword_id,
            start_day=after_start,
            end_day=after_end,
        )
        if before_df.empty or after_df.empty:
            dropped += 1
            continue

        before_summary = summarize_window(before_df.to_dict(orient="records"))
        after_summary = summarize_window(after_df.to_dict(orient="records"))
        performance_change = _evaluate_performance_change(
            action=str(action_row["action_type"]),
            before_acos=float(before_summary["acos"]),
            after_acos=float(after_summary["acos"]),
            before_clicks=float(before_summary["clicks"]),
            after_clicks=float(after_summary["clicks"]),
            before_cvr=float(before_summary["cvr"]),
            after_cvr=float(after_summary["cvr"]),
        )
        rows.append(
            {
                "store_id": str(action_row["store_id"]),
                "ad_group_id": ad_group_id,
                "keyword_id": keyword_id,
                "change_time": str(action_row["change_time"]),
                "action": str(action_row["action_type"]),
                "bid_change_pct": float(action_row["bid_change_pct"]),
                "before_acos": round(float(before_summary["acos"]), 4),
                "after_acos": round(float(after_summary["acos"]), 4),
                "before_clicks": round(float(before_summary["clicks"]), 2),
                "after_clicks": round(float(after_summary["clicks"]), 2),
                "before_cvr": round(float(before_summary["cvr"]), 6),
                "after_cvr": round(float(after_summary["cvr"]), 6),
                "performance_change": performance_change,
            }
        )

    samples_df = pd.DataFrame(rows)
    summary = {
        "matched_changes": len(samples_df),
        "dropped_changes": dropped,
    }
    return samples_df, summary

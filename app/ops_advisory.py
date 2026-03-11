from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .ops_db import ops_data_store
from .ops_logger import get_ops_logger
from .ops_whitepaper import read_operational_whitepaper


logger = get_ops_logger()


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = text.replace(",", "").replace("%", "")
        return float(text)
    except (TypeError, ValueError):
        return None


def _load_latest_bid_by_ad_group(store_id: str, current_date: str) -> Dict[int, float]:
    start_date = (datetime.strptime(current_date, "%Y-%m-%d").date() - timedelta(days=365)).isoformat()
    change_df = ops_data_store.load_change_df(store_id, start_date=start_date, end_date=current_date)
    if change_df.empty:
        return {}

    bid_df = change_df[
        change_df["change_type"].isin(["targeting_bid", "targeting_bid_snapshot"])
    ].copy()
    if bid_df.empty:
        return {}
    bid_df["date"] = pd.to_datetime(bid_df["date"]).dt.date
    bid_df["new_value_num"] = bid_df["new_value"].map(_to_float)
    bid_df = bid_df[bid_df["new_value_num"].notna() & bid_df["ad_group_id"].notna()]
    if bid_df.empty:
        return {}

    bid_df["ad_group_id"] = bid_df["ad_group_id"].astype(float).astype(int)
    latest = (
        bid_df.sort_values(["ad_group_id", "date"])
        .groupby("ad_group_id", as_index=False)
        .tail(1)
    )
    return {int(row["ad_group_id"]): float(row["new_value_num"]) for _, row in latest.iterrows()}


def generate_periodic_advice(store_id: str) -> Dict[str, Any]:
    whitepaper_bundle = read_operational_whitepaper(store_id)
    whitepaper = whitepaper_bundle["whitepaper"]
    strategy = whitepaper.get("master_strategy", {})

    max_date_text = ops_data_store.get_max_performance_date(store_id)
    if not max_date_text:
        raise ValueError(f"No local performance data for {store_id}. Run incremental sync first.")

    current_day = datetime.strptime(max_date_text, "%Y-%m-%d").date()
    current_text = current_day.isoformat()
    lookback_start = (current_day - timedelta(days=364)).isoformat()

    perf_df = ops_data_store.load_performance_df(store_id, start_date=current_text, end_date=current_text)
    if perf_df.empty:
        raise ValueError("No current-day ad group performance found.")

    placement_df = ops_data_store.load_placement_df(store_id, start_date=current_text, end_date=current_text)
    query_df = ops_data_store.load_query_terms_df(store_id, start_date=lookback_start, end_date=current_text)
    inventory_df = ops_data_store.load_latest_inventory_df(store_id)
    latest_bid_map = _load_latest_bid_by_ad_group(store_id=store_id, current_date=current_text)

    perf_df["spend"] = pd.to_numeric(perf_df["spend"], errors="coerce").fillna(0.0)
    perf_df["sales"] = pd.to_numeric(perf_df["sales"], errors="coerce").fillna(0.0)
    perf_df["clicks"] = pd.to_numeric(perf_df["clicks"], errors="coerce").fillna(0).astype(int)
    perf_df["ad_group_id"] = pd.to_numeric(perf_df["ad_group_id"], errors="coerce").fillna(0).astype(int)
    perf_df["campaign_id"] = pd.to_numeric(perf_df["campaign_id"], errors="coerce").fillna(0).astype(int)
    perf_df["acos"] = perf_df.apply(
        lambda r: round((float(r["spend"]) / float(r["sales"])) * 100, 2) if float(r["sales"]) > 0 else 0.0,
        axis=1,
    )

    placement_benchmark_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in strategy.get("placement_roi_benchmarks", []):
        product_type = str(item.get("product_type") or "unknown")
        placement_type = str(item.get("placement_type") or "UNKNOWN")
        placement_benchmark_map[(product_type, placement_type)] = item

    campaign_placement_map: Dict[int, List[Dict[str, Any]]] = {}
    if not placement_df.empty:
        placement_df["campaign_id"] = pd.to_numeric(placement_df["campaign_id"], errors="coerce").fillna(0).astype(int)
        placement_df["spend"] = pd.to_numeric(placement_df["spend"], errors="coerce").fillna(0.0)
        placement_df["sales"] = pd.to_numeric(placement_df["sales"], errors="coerce").fillna(0.0)
        placement_df["clicks"] = pd.to_numeric(placement_df["clicks"], errors="coerce").fillna(0).astype(int)
        for _, row in placement_df.iterrows():
            roas = _safe_ratio(float(row["sales"]), float(row["spend"]))
            campaign_placement_map.setdefault(int(row["campaign_id"]), []).append(
                {
                    "placement_type": str(row.get("placement_type") or "UNKNOWN"),
                    "clicks": int(row["clicks"]),
                    "spend": round(float(row["spend"]), 2),
                    "sales": round(float(row["sales"]), 2),
                    "roas": roas,
                }
            )

    if not query_df.empty:
        query_df["ad_group_id"] = pd.to_numeric(query_df["ad_group_id"], errors="coerce").fillna(0).astype(int)
        query_df["clicks"] = pd.to_numeric(query_df["clicks"], errors="coerce").fillna(0).astype(int)
        query_df["sales"] = pd.to_numeric(query_df["sales"], errors="coerce").fillna(0.0)
        query_df["spend"] = pd.to_numeric(query_df["spend"], errors="coerce").fillna(0.0)
        grouped_query_df = query_df.groupby(["ad_group_id", "search_term"], as_index=False).agg(
            clicks=("clicks", "sum"),
            spend=("spend", "sum"),
            sales=("sales", "sum"),
        )
    else:
        grouped_query_df = pd.DataFrame(columns=["ad_group_id", "search_term", "clicks", "spend", "sales"])

    inventory_map: Dict[int, Dict[str, Any]] = {}
    if not inventory_df.empty:
        inventory_df["ad_group_id"] = pd.to_numeric(inventory_df["ad_group_id"], errors="coerce").fillna(0).astype(int)
        inventory_df["avg_price"] = pd.to_numeric(inventory_df["avg_price"], errors="coerce").fillna(0.0)
        inventory_df["avg_stock"] = pd.to_numeric(inventory_df["avg_stock"], errors="coerce").fillna(0.0)
        for _, row in inventory_df.iterrows():
            inventory_map[int(row["ad_group_id"])] = {
                "avg_price": float(row["avg_price"]),
                "avg_stock": float(row["avg_stock"]),
                "snapshot_date": str(row.get("snapshot_date") or ""),
            }

    assumptions = strategy.get("inventory_price_stock_assumptions", {})
    assumed_price = _to_float(assumptions.get("avg_price")) or 0.0
    assumed_stock = _to_float(assumptions.get("avg_stock")) or 0.0
    price_tol_pct = _to_float(assumptions.get("price_tolerance_pct")) or 20.0
    stock_tol_pct = _to_float(assumptions.get("stock_tolerance_pct")) or 30.0

    targeting_zone_map = strategy.get("targeting_bid_success_zone_by_product_type", {})
    negative_rule = strategy.get("negative_targeting_rule", {})
    min_clicks_without_sales = int(negative_rule.get("min_clicks_without_sales", 20) or 20)
    max_sales = float(negative_rule.get("max_sales", 0) or 0)

    ad_group_advice: List[Dict[str, Any]] = []
    high_sensitivity = False

    for _, row in perf_df.iterrows():
        ad_group_id = int(row["ad_group_id"])
        campaign_id = int(row["campaign_id"])
        product_type = str(row.get("product_type") or "unknown")
        current_acos = float(row["acos"])
        current_bid = latest_bid_map.get(ad_group_id)

        targeting_zone = targeting_zone_map.get(product_type) or targeting_zone_map.get("unknown")
        target_range = targeting_zone.get("value_range", {}) if isinstance(targeting_zone, dict) else {}
        target_min = _to_float(target_range.get("min"))
        target_max = _to_float(target_range.get("max"))
        target_mid = None
        if target_min is not None and target_max is not None:
            target_mid = round((target_min + target_max) / 2, 4)

        if current_bid is None or target_mid is None:
            targeting_action = "manual_check"
            suggested_bid = current_bid
            targeting_reason = "No reliable historical bid zone or current bid snapshot."
        elif current_acos > 35:
            suggested_bid = round(min(current_bid, target_mid) * 0.9, 4)
            targeting_action = "decrease"
            targeting_reason = "Current ACoS above target; move toward historical success bid zone."
        elif current_acos < 20:
            suggested_bid = round(max(current_bid, target_mid) * 1.08, 4)
            targeting_action = "increase"
            targeting_reason = "Current ACoS efficient; scale toward historical success bid zone."
        else:
            suggested_bid = round(target_mid, 4)
            targeting_action = "hold_or_align"
            targeting_reason = "Current ACoS in-range; align with historical success bid zone."

        placement_suggestions: List[Dict[str, Any]] = []
        for item in campaign_placement_map.get(campaign_id, []):
            placement_type = str(item["placement_type"])
            benchmark = placement_benchmark_map.get((product_type, placement_type))
            benchmark_roas = _to_float(benchmark.get("roas")) if benchmark else None
            current_roas = _to_float(item.get("roas"))
            action = "hold"
            reason = "No benchmark found."
            if benchmark_roas and current_roas is not None:
                if current_roas < benchmark_roas * 0.8:
                    action = "decrease_multiplier_10pct"
                    reason = "Current placement ROAS below historical benchmark."
                elif current_roas > benchmark_roas * 1.2:
                    action = "increase_multiplier_10pct"
                    reason = "Current placement ROAS above historical benchmark."
                else:
                    action = "hold"
                    reason = "Current placement ROAS close to historical benchmark."
            placement_suggestions.append(
                {
                    "placement_type": placement_type,
                    "current_roas": current_roas,
                    "benchmark_roas": benchmark_roas,
                    "suggestion": action,
                    "reason": reason,
                }
            )

        ad_group_terms = grouped_query_df[grouped_query_df["ad_group_id"] == ad_group_id]
        negative_candidates = ad_group_terms[
            (ad_group_terms["clicks"] >= min_clicks_without_sales)
            & (ad_group_terms["sales"] <= max_sales)
        ].sort_values(["spend", "clicks"], ascending=[False, False]).head(5)
        negative_suggestions = [
            {
                "search_term": str(r["search_term"]),
                "clicks": int(r["clicks"]),
                "spend": round(float(r["spend"]), 2),
                "sales": round(float(r["sales"]), 2),
                "reason": "Historical failure pattern: high click, zero sales.",
            }
            for _, r in negative_candidates.iterrows()
        ]

        inventory_current = inventory_map.get(ad_group_id, {"avg_price": 0.0, "avg_stock": 0.0, "snapshot_date": ""})
        current_price = float(inventory_current.get("avg_price") or 0.0)
        current_stock = float(inventory_current.get("avg_stock") or 0.0)
        price_dev_pct = (
            abs((current_price - assumed_price) / assumed_price) * 100
            if assumed_price > 0
            else 0.0
        )
        stock_dev_pct = (
            abs((current_stock - assumed_stock) / assumed_stock) * 100
            if assumed_stock > 0
            else 0.0
        )
        row_high_sensitivity = bool(
            (assumed_price > 0 and price_dev_pct > price_tol_pct)
            or (assumed_stock > 0 and stock_dev_pct > stock_tol_pct)
        )
        if row_high_sensitivity:
            high_sensitivity = True

        ad_group_advice.append(
            {
                "ad_group_id": ad_group_id,
                "ad_group_name": str(row.get("ad_group_name") or ""),
                "campaign_id": campaign_id,
                "campaign_name": str(row.get("campaign_name") or ""),
                "product_type": product_type,
                "current_metrics": {
                    "date": current_text,
                    "clicks": int(row["clicks"]),
                    "spend": round(float(row["spend"]), 2),
                    "sales": round(float(row["sales"]), 2),
                    "acos": round(current_acos, 2),
                    "current_bid": current_bid,
                },
                "targeting_bid_suggestion": {
                    "action": targeting_action,
                    "current_bid": current_bid,
                    "suggested_bid": suggested_bid,
                    "historical_success_zone": target_range or None,
                    "reason": targeting_reason,
                },
                "placement_bid_suggestion": placement_suggestions,
                "negative_targeting_suggestion": negative_suggestions,
                "sensitivity": {
                    "high_sensitivity": row_high_sensitivity,
                    "manual_review_required": row_high_sensitivity,
                    "reason": (
                        "Current price/stock deviates from whitepaper assumptions."
                        if row_high_sensitivity
                        else "Within whitepaper assumptions."
                    ),
                    "assumed_avg_price": assumed_price,
                    "assumed_avg_stock": assumed_stock,
                    "current_avg_price": current_price,
                    "current_avg_stock": current_stock,
                    "price_deviation_pct": round(price_dev_pct, 2),
                    "stock_deviation_pct": round(stock_dev_pct, 2),
                },
            }
        )

    result = {
        "store_id": store_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "advice_date": current_text,
        "high_sensitivity": high_sensitivity,
        "manual_review_required": high_sensitivity,
        "review_message": (
            "High Sensitivity detected: verify inventory/price assumptions before execution."
            if high_sensitivity
            else "No high-sensitivity deviations detected."
        ),
        "ad_group_advice": ad_group_advice,
    }
    logger.info(
        "periodic_advice_generated store_id=%s date=%s ad_groups=%s high_sensitivity=%s",
        store_id,
        current_text,
        len(ad_group_advice),
        high_sensitivity,
    )
    return result

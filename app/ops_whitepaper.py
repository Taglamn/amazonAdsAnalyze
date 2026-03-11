from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .data_access import DATA_DIR
from .ops_db import ops_data_store
from .ops_logger import get_ops_logger


logger = get_ops_logger()
WHITEPAPER_DIR = DATA_DIR / "ops_whitepapers"
WHITEPAPER_DIR.mkdir(parents=True, exist_ok=True)


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _safe_pct_change(before: float | None, after: float | None) -> float | None:
    if before is None or after is None or before == 0:
        return None
    return round(((after - before) / before) * 100, 2)


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = text.replace(",", "").replace("%", "")
        parsed = float(text)
        return parsed
    except (TypeError, ValueError):
        return None


def _window_summary(df: pd.DataFrame) -> Dict[str, Any]:
    spend = float(df["spend"].sum()) if not df.empty else 0.0
    sales = float(df["sales"].sum()) if not df.empty else 0.0
    clicks = int(df["clicks"].sum()) if not df.empty else 0
    acos = round((spend / sales) * 100, 2) if sales > 0 else None
    roi = _safe_ratio(sales, spend)
    return {
        "spend": round(spend, 2),
        "sales": round(sales, 2),
        "clicks": clicks,
        "acos": acos,
        "roi": roi,
    }


def _bucket_targeting_bid(value: float) -> Tuple[str, Dict[str, float]]:
    if value < 0.5:
        return "<0.5", {"min": 0.0, "max": 0.49}
    if value < 1.0:
        return "0.5-1.0", {"min": 0.5, "max": 0.99}
    if value < 1.5:
        return "1.0-1.5", {"min": 1.0, "max": 1.49}
    if value < 2.0:
        return "1.5-2.0", {"min": 1.5, "max": 1.99}
    return ">=2.0", {"min": 2.0, "max": 999.0}


def _normalize_placement_multiplier(value: float) -> float:
    if value <= 0:
        return 0.0
    if value <= 10:
        return round(value * 100, 2)
    return round(value, 2)


def _bucket_placement_multiplier(value_pct: float) -> Tuple[str, Dict[str, float]]:
    if value_pct < 20:
        return "<20%", {"min": 0.0, "max": 19.99}
    if value_pct < 60:
        return "20%-60%", {"min": 20.0, "max": 59.99}
    if value_pct < 120:
        return "60%-120%", {"min": 60.0, "max": 119.99}
    return ">=120%", {"min": 120.0, "max": 1000.0}


def _save_whitepaper(store_id: str, payload: Dict[str, Any], markdown: str) -> Dict[str, str]:
    json_path = WHITEPAPER_DIR / f"{store_id}_ops_whitepaper.json"
    md_path = WHITEPAPER_DIR / f"{store_id}_ops_whitepaper.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path)}


def _load_whitepaper(store_id: str) -> Dict[str, Any]:
    json_path = WHITEPAPER_DIR / f"{store_id}_ops_whitepaper.json"
    if not json_path.exists():
        raise FileNotFoundError(f"Operational whitepaper not found for {store_id}")
    return json.loads(json_path.read_text(encoding="utf-8"))


def read_operational_whitepaper(store_id: str) -> Dict[str, Any]:
    payload = _load_whitepaper(store_id)
    md_path = WHITEPAPER_DIR / f"{store_id}_ops_whitepaper.md"
    markdown = md_path.read_text(encoding="utf-8") if md_path.exists() else ""
    return {
        "store_id": store_id,
        "whitepaper": payload,
        "markdown": markdown,
        "json_path": str(WHITEPAPER_DIR / f"{store_id}_ops_whitepaper.json"),
        "markdown_path": str(md_path),
    }


def synthesize_operational_whitepaper(store_id: str) -> Dict[str, Any]:
    max_date_text = ops_data_store.get_max_performance_date(store_id)
    if not max_date_text:
        raise ValueError(
            f"No local performance data for store {store_id}. Run incremental sync first."
        )

    end_day = datetime.strptime(max_date_text, "%Y-%m-%d").date()
    start_day = end_day - timedelta(days=364)
    start_text = start_day.isoformat()
    end_text = end_day.isoformat()

    perf_df = ops_data_store.load_performance_df(store_id, start_text, end_text)
    change_df = ops_data_store.load_change_df(store_id, start_text, end_text)
    placement_df = ops_data_store.load_placement_df(store_id, start_text, end_text)
    query_df = ops_data_store.load_query_terms_df(store_id, start_text, end_text)
    inventory_df = ops_data_store.load_latest_inventory_df(store_id)

    if perf_df.empty:
        raise ValueError("No performance records in analysis window")

    perf_df["date"] = pd.to_datetime(perf_df["date"]).dt.date
    change_df["date"] = pd.to_datetime(change_df["date"]).dt.date if not change_df.empty else pd.Series(dtype="datetime64[ns]")

    for col in ("spend", "sales", "clicks", "acos"):
        if col in perf_df.columns:
            perf_df[col] = pd.to_numeric(perf_df[col], errors="coerce").fillna(0)

    causal_rows: List[Dict[str, Any]] = []
    relevant_changes = change_df[
        change_df["change_type"].isin(["targeting_bid", "placement_bid", "negative_targeting"])
    ] if not change_df.empty else pd.DataFrame()

    for _, row in relevant_changes.iterrows():
        event_day = row["date"]
        ad_group_id = row.get("ad_group_id")
        product_type = "unknown"

        scoped_perf = perf_df
        if pd.notna(ad_group_id):
            try:
                ad_group_id_int = int(float(ad_group_id))
                scoped_perf = perf_df[perf_df["ad_group_id"] == ad_group_id_int]
            except (TypeError, ValueError):
                scoped_perf = perf_df

        if not scoped_perf.empty:
            product_type = str(scoped_perf["product_type"].mode().iloc[0] or "unknown")

        before_df = scoped_perf[
            (scoped_perf["date"] >= event_day - timedelta(days=14))
            & (scoped_perf["date"] <= event_day - timedelta(days=1))
        ]
        after_df = scoped_perf[
            (scoped_perf["date"] >= event_day + timedelta(days=1))
            & (scoped_perf["date"] <= event_day + timedelta(days=14))
        ]
        before_summary = _window_summary(before_df)
        after_summary = _window_summary(after_df)
        roi_delta_pct = _safe_pct_change(before_summary.get("roi"), after_summary.get("roi"))
        acos_delta_pct = _safe_pct_change(before_summary.get("acos"), after_summary.get("acos"))

        success = bool(
            roi_delta_pct is not None
            and acos_delta_pct is not None
            and roi_delta_pct > 0
            and acos_delta_pct < 0
        )

        causal_rows.append(
            {
                "event_date": event_day.isoformat(),
                "change_type": str(row.get("change_type") or ""),
                "field_code": str(row.get("field_code") or ""),
                "ad_group_id": int(float(row["ad_group_id"])) if pd.notna(row.get("ad_group_id")) else None,
                "ad_group_name": str(row.get("ad_group_name") or ""),
                "product_type": product_type,
                "old_value": str(row.get("old_value") or ""),
                "new_value": str(row.get("new_value") or ""),
                "keyword_text": str(row.get("keyword_text") or ""),
                "before_14d": before_summary,
                "after_14d": after_summary,
                "impact": {
                    "roi_delta_pct": roi_delta_pct,
                    "acos_delta_pct": acos_delta_pct,
                    "success": success,
                },
            }
        )

    targeting_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}
    placement_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in causal_rows:
        change_type = row["change_type"]
        roi_delta = row["impact"]["roi_delta_pct"]
        success = bool(row["impact"]["success"])
        if roi_delta is None:
            continue
        product_type = str(row["product_type"] or "unknown")

        if change_type == "targeting_bid":
            value = _to_float(row.get("new_value"))
            if value is None:
                continue
            bucket, value_range = _bucket_targeting_bid(value)
            key = (product_type, bucket)
            item = targeting_stats.setdefault(
                key,
                {
                    "product_type": product_type,
                    "bucket": bucket,
                    "value_range": value_range,
                    "count": 0,
                    "success_count": 0,
                    "roi_delta_sum": 0.0,
                },
            )
            item["count"] += 1
            item["success_count"] += 1 if success else 0
            item["roi_delta_sum"] += float(roi_delta)
        elif change_type == "placement_bid":
            raw_value = _to_float(row.get("new_value"))
            if raw_value is None:
                continue
            value_pct = _normalize_placement_multiplier(raw_value)
            bucket, value_range = _bucket_placement_multiplier(value_pct)
            key = (product_type, bucket)
            item = placement_stats.setdefault(
                key,
                {
                    "product_type": product_type,
                    "bucket": bucket,
                    "value_range": value_range,
                    "count": 0,
                    "success_count": 0,
                    "roi_delta_sum": 0.0,
                },
            )
            item["count"] += 1
            item["success_count"] += 1 if success else 0
            item["roi_delta_sum"] += float(roi_delta)

    def _finalize_pattern_rows(stats: Dict[Tuple[str, str], Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for _, item in stats.items():
            count = int(item["count"])
            if count <= 0:
                continue
            success_rate = round(item["success_count"] / count, 4)
            avg_roi_delta_pct = round(item["roi_delta_sum"] / count, 2)
            rows.append(
                {
                    "product_type": item["product_type"],
                    "bucket": item["bucket"],
                    "value_range": item["value_range"],
                    "sample_count": count,
                    "success_rate": success_rate,
                    "avg_roi_delta_pct": avg_roi_delta_pct,
                }
            )
        rows.sort(
            key=lambda x: (
                x["product_type"],
                -float(x["success_rate"]),
                -float(x["avg_roi_delta_pct"]),
            )
        )
        return rows

    targeting_pattern_rows = _finalize_pattern_rows(targeting_stats)
    placement_pattern_rows = _finalize_pattern_rows(placement_stats)

    targeting_best: Dict[str, Dict[str, Any]] = {}
    for row in targeting_pattern_rows:
        product_type = row["product_type"]
        if product_type not in targeting_best:
            targeting_best[product_type] = row

    placement_best: Dict[str, Dict[str, Any]] = {}
    for row in placement_pattern_rows:
        product_type = row["product_type"]
        if product_type not in placement_best:
            placement_best[product_type] = row

    placement_benchmarks: List[Dict[str, Any]] = []
    if not placement_df.empty:
        placement_df["spend"] = pd.to_numeric(placement_df["spend"], errors="coerce").fillna(0.0)
        placement_df["sales"] = pd.to_numeric(placement_df["sales"], errors="coerce").fillna(0.0)
        campaign_product = (
            perf_df.groupby(["campaign_id", "product_type"], as_index=False)["spend"].sum()
            .sort_values(["campaign_id", "spend"], ascending=[True, False])
            .groupby("campaign_id", as_index=False)
            .head(1)[["campaign_id", "product_type"]]
        )
        merged = placement_df.merge(campaign_product, on="campaign_id", how="left")
        merged["product_type"] = merged["product_type"].fillna("unknown")
        grouped = merged.groupby(["product_type", "placement_type"], as_index=False).agg(
            clicks=("clicks", "sum"),
            spend=("spend", "sum"),
            sales=("sales", "sum"),
        )
        for _, row in grouped.iterrows():
            spend = float(row["spend"])
            sales = float(row["sales"])
            roas = _safe_ratio(sales, spend)
            acos = round((spend / sales) * 100, 2) if sales > 0 else None
            placement_benchmarks.append(
                {
                    "product_type": str(row["product_type"]),
                    "placement_type": str(row["placement_type"]),
                    "clicks": int(row["clicks"]),
                    "spend": round(spend, 2),
                    "sales": round(sales, 2),
                    "roas": roas,
                    "acos": acos,
                }
            )
        placement_benchmarks.sort(
            key=lambda x: (x["product_type"], x["placement_type"])
        )

    negative_failure_terms: List[Dict[str, Any]] = []
    if not query_df.empty:
        query_df["clicks"] = pd.to_numeric(query_df["clicks"], errors="coerce").fillna(0)
        query_df["sales"] = pd.to_numeric(query_df["sales"], errors="coerce").fillna(0.0)
        query_df["spend"] = pd.to_numeric(query_df["spend"], errors="coerce").fillna(0.0)
        term_group = query_df.groupby(["ad_group_id", "search_term"], as_index=False).agg(
            clicks=("clicks", "sum"),
            spend=("spend", "sum"),
            sales=("sales", "sum"),
            days=("date", "nunique"),
        )
        failure = term_group[(term_group["clicks"] >= 20) & (term_group["sales"] <= 0)]
        failure = failure.sort_values(["spend", "clicks"], ascending=[False, False]).head(100)
        for _, row in failure.iterrows():
            negative_failure_terms.append(
                {
                    "ad_group_id": int(row["ad_group_id"]),
                    "search_term": str(row["search_term"]),
                    "clicks": int(row["clicks"]),
                    "spend": round(float(row["spend"]), 2),
                    "sales": round(float(row["sales"]), 2),
                    "days": int(row["days"]),
                }
            )

    avg_price = 0.0
    avg_stock = 0.0
    inventory_date = ""
    if not inventory_df.empty:
        inventory_df["avg_price"] = pd.to_numeric(inventory_df["avg_price"], errors="coerce").fillna(0.0)
        inventory_df["avg_stock"] = pd.to_numeric(inventory_df["avg_stock"], errors="coerce").fillna(0.0)
        avg_price = round(float(inventory_df["avg_price"].mean()), 2)
        avg_stock = round(float(inventory_df["avg_stock"].mean()), 2)
        inventory_date = str(inventory_df["snapshot_date"].iloc[0])

    payload = {
        "store_id": store_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "analysis_window": {
            "start_date": start_text,
            "end_date": end_text,
            "days": 365,
        },
        "causal_mapping_14d": causal_rows,
        "success_failure_patterns": {
            "targeting_bid_patterns": targeting_pattern_rows,
            "placement_bid_patterns": placement_pattern_rows,
            "negative_targeting_failure_terms": negative_failure_terms,
        },
        "master_strategy": {
            "targeting_bid_success_zone_by_product_type": targeting_best,
            "placement_multiplier_success_zone_by_product_type": placement_best,
            "placement_roi_benchmarks": placement_benchmarks,
            "negative_targeting_rule": {
                "min_clicks_without_sales": 20,
                "max_sales": 0,
            },
            "inventory_price_stock_assumptions": {
                "snapshot_date": inventory_date,
                "avg_price": avg_price,
                "avg_stock": avg_stock,
                "price_tolerance_pct": 20,
                "stock_tolerance_pct": 30,
            },
        },
    }

    markdown_lines = [
        f"# Ad Operations Whitepaper - {store_id}",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Analysis window: {start_text} to {end_text} (365 days)",
        "",
        "## Master Strategy Snapshot",
        "",
        "### Targeting Bid Success Zones",
    ]
    if targeting_best:
        for product_type, zone in targeting_best.items():
            markdown_lines.append(
                f"- {product_type}: {zone['bucket']} | success_rate={zone['success_rate']:.2%} | avg_roi_delta={zone['avg_roi_delta_pct']}%"
            )
    else:
        markdown_lines.append("- No stable targeting bid success zone identified yet.")

    markdown_lines.extend(["", "### Placement Multiplier Success Zones"])
    if placement_best:
        for product_type, zone in placement_best.items():
            markdown_lines.append(
                f"- {product_type}: {zone['bucket']} | success_rate={zone['success_rate']:.2%} | avg_roi_delta={zone['avg_roi_delta_pct']}%"
            )
    else:
        markdown_lines.append("- No stable placement multiplier success zone identified yet.")

    markdown_lines.extend(
        [
            "",
            "### Negative Targeting Failure Pattern",
            f"- Terms with clicks >= 20 and sales <= 0: {len(negative_failure_terms)}",
            "",
            "### Inventory Assumptions",
            f"- Avg Price: {avg_price}",
            f"- Avg Stock: {avg_stock}",
            f"- Snapshot Date: {inventory_date or 'N/A'}",
        ]
    )
    markdown = "\n".join(markdown_lines)

    output_paths = _save_whitepaper(store_id=store_id, payload=payload, markdown=markdown)
    logger.info(
        "whitepaper_synthesized store_id=%s window=%s..%s causal_events=%s",
        store_id,
        start_text,
        end_text,
        len(causal_rows),
    )
    return {
        "store_id": store_id,
        "whitepaper": payload,
        "markdown": markdown,
        **output_paths,
    }

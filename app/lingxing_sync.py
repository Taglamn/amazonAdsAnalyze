from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from .analysis import build_bid_recommendations, build_optimization_cases
from .data_access import DATA_DIR, HISTORY_DIR, PERFORMANCE_DIR, PLAYBOOK_DIR
from .lingxing_client import LingxingClient, LingxingCredentials


@dataclass
class SyncWindow:
    start_date: date
    end_date: date


def _parse_date_or_none(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def resolve_sync_window(
    report_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> SyncWindow:
    report_day = _parse_date_or_none(report_date)
    if report_day:
        return SyncWindow(start_date=report_day, end_date=report_day)

    parsed_end = _parse_date_or_none(end_date)
    parsed_start = _parse_date_or_none(start_date)

    end_day = parsed_end or (date.today() - timedelta(days=1))
    start_day = parsed_start or (end_day - timedelta(days=364))

    if start_day > end_day:
        raise ValueError("start_date must be <= end_date")

    return SyncWindow(start_date=start_day, end_date=end_day)


def _date_iter(start_day: date, end_day: date) -> List[date]:
    days: List[date] = []
    current = start_day
    while current <= end_day:
        days.append(current)
        current += timedelta(days=1)
    return days


def _chunk_date_ranges(
    start_day: date,
    end_day: date,
    max_span_days: int = 30,
) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    current = start_day
    while current <= end_day:
        chunk_end = min(current + timedelta(days=max_span_days), end_day)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _sort_unique_dates(date_texts: List[str]) -> List[str]:
    unique = sorted({str(item) for item in date_texts if str(item).strip()})
    return unique


def _missing_days(requested_days: List[str], covered_days: Set[str]) -> List[str]:
    return [day for day in requested_days if day not in covered_days]


def _build_missing_date_ranges(missing_days: List[str], max_span_days: int = 30) -> List[Tuple[date, date]]:
    if not missing_days:
        return []
    parsed = sorted({datetime.strptime(day, "%Y-%m-%d").date() for day in missing_days})
    ranges: List[Tuple[date, date]] = []

    seg_start = parsed[0]
    seg_prev = parsed[0]
    for day in parsed[1:]:
        if (day - seg_prev).days == 1:
            seg_prev = day
            continue
        ranges.extend(_chunk_date_ranges(seg_start, seg_prev, max_span_days=max_span_days))
        seg_start = day
        seg_prev = day
    ranges.extend(_chunk_date_ranges(seg_start, seg_prev, max_span_days=max_span_days))
    return ranges


def _filter_rows_by_window(rows: List[Dict[str, Any]], start_day: date, end_day: date) -> List[Dict[str, Any]]:
    start_text = start_day.isoformat()
    end_text = end_day.isoformat()
    result = [
        row
        for row in rows
        if start_text <= str(row.get("date", "")) <= end_text
    ]
    return result


def _upsert_rows_by_key(
    existing_rows: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    key_fields: List[str],
) -> List[Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    for row in existing_rows:
        key = "|".join(str(row.get(field, "")) for field in key_fields)
        mapping[key] = row
    for row in new_rows:
        key = "|".join(str(row.get(field, "")) for field in key_fields)
        mapping[key] = row

    merged = list(mapping.values())
    merged.sort(
        key=lambda x: tuple(str(x.get(field, "")) for field in key_fields)
    )
    return merged


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group())


def _aggregate_daily_metrics(rows: List[Dict[str, Any]], target_date: date, store_id: str) -> Dict[str, Any]:
    clicks = sum(int(float(item.get("clicks", 0) or 0)) for item in rows)
    spend = sum(
        float(
            item.get("cost", item.get("total_cost", item.get("spend", 0)))
            or 0
        )
        for item in rows
    )
    sales = sum(float(item.get("sales", item.get("same_sales", 0)) or 0) for item in rows)

    acos = round((spend / sales) * 100, 2) if sales else 0.0

    return {
        "store_id": store_id,
        "date": target_date.isoformat(),
        "clicks": clicks,
        "spend": round(spend, 2),
        "acos": acos,
        "sales": round(sales, 2),
    }


def _pick_first_str(item: Dict[str, Any], keys: List[str], fallback: str) -> str:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return fallback


def _pick_first_int(item: Dict[str, Any], keys: List[str]) -> int | None:
    for key in keys:
        value = item.get(key)
        if value is None or value == "":
            continue
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return None


def _pick_first_float(item: Dict[str, Any], keys: List[str]) -> float | None:
    for key in keys:
        value = _to_float(item.get(key))
        if value is not None:
            return value
    return None


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _normalize_pct(value: float) -> float:
    # Some APIs return ratio (0.1234), some return percentage (12.34)
    if value <= 1.5:
        return value * 100
    return value


def _extract_common_metrics(item: Dict[str, Any]) -> Dict[str, float]:
    impressions = float(
        _pick_first_float(item, ["impressions", "impression", "show", "shows", "views", "view"]) or 0
    )
    clicks = float(_pick_first_float(item, ["clicks", "click"]) or 0)
    total_cost = float(_pick_first_float(item, ["total_cost", "cost", "spend"]) or 0)
    purchase = float(
        _pick_first_float(item, ["purchase", "purchases", "orders", "same_orders", "attributed_orders"]) or 0
    )
    sales = float(
        _pick_first_float(item, ["sales", "same_sales", "attributed_sales", "total_sales"]) or 0
    )

    raw_ctr = _pick_first_float(item, ["ctr", "click_rate"])
    raw_cpc = _pick_first_float(item, ["cpc", "avg_cpc"])
    raw_acos = _pick_first_float(item, ["acos", "a_cos", "cost_sale_ratio"])
    raw_purchase_rate = _pick_first_float(item, ["purchase_rate", "conversion_rate", "cvr"])

    ctr = _normalize_pct(raw_ctr) if raw_ctr is not None else (_safe_div(clicks, impressions) * 100)
    cpc = raw_cpc if raw_cpc is not None else _safe_div(total_cost, clicks)
    acos = _normalize_pct(raw_acos) if raw_acos is not None else (_safe_div(total_cost, sales) * 100)
    purchase_rate = (
        _normalize_pct(raw_purchase_rate)
        if raw_purchase_rate is not None
        else (_safe_div(purchase, clicks) * 100)
    )

    return {
        "impression": round(impressions, 2),
        "clicks": round(clicks, 2),
        "total_cost": round(total_cost, 2),
        "cpc": round(cpc, 4),
        "purchase": round(purchase, 2),
        "sales": round(sales, 2),
        "acos": round(acos, 4),
        "purchase_rate": round(purchase_rate, 4),
        "ctr": round(ctr, 4),
    }


def _extract_day_text(item: Dict[str, Any], fallback_day: str) -> str:
    value = _pick_first_str(item, ["report_date", "date", "day"], fallback_day)
    if len(value) >= 10:
        return value[:10]
    return fallback_day


def _normalize_ad_group_report_rows(
    report_rows: List[Dict[str, Any]],
    fallback_day: str,
    campaign_name_map: Dict[Tuple[str, int], str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in report_rows:
        sponsored_type = _pick_first_str(item, ["sponsored_type"], "").lower()
        campaign_id = _pick_first_int(item, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        if not sponsored_type or campaign_id is None or ad_group_id is None:
            continue

        metrics = _extract_common_metrics(item)
        day_text = _extract_day_text(item, fallback_day)
        campaign_name = campaign_name_map.get((sponsored_type, campaign_id), "").strip() or _pick_first_str(
            item,
            ["campaign_name", "campaignName", "campaign"],
            f"campaign_{campaign_id}",
        )
        ad_group_name = _pick_first_str(
            item,
            ["ad_group_name", "adGroupName", "ad_group", "adgroup_name", "name"],
            f"adgroup_{ad_group_id}",
        )

        rows.append(
            {
                "date": day_text,
                "sponsored_type": sponsored_type,
                "ad_combo": sponsored_type.upper(),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                **metrics,
            }
        )
    return rows


def _normalize_targeting_rows(
    report_rows: List[Dict[str, Any]],
    fallback_day: str,
    campaign_name_map: Dict[Tuple[str, int], str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in report_rows:
        sponsored_type = _pick_first_str(item, ["sponsored_type"], "").lower()
        campaign_id = _pick_first_int(item, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        if not sponsored_type or campaign_id is None or ad_group_id is None:
            continue

        metrics = _extract_common_metrics(item)
        day_text = _extract_day_text(item, fallback_day)
        campaign_name = campaign_name_map.get((sponsored_type, campaign_id), "").strip() or _pick_first_str(
            item,
            ["campaign_name", "campaignName", "campaign"],
            f"campaign_{campaign_id}",
        )
        ad_group_name = _pick_first_str(
            item,
            ["ad_group_name", "adGroupName", "ad_group", "adgroup_name", "name"],
            f"adgroup_{ad_group_id}",
        )
        keyword = _pick_first_str(
            item,
            ["keyword", "keywords", "target", "targeting_text", "query_word", "queryWord"],
            "",
        )
        target_match_type = _pick_first_str(
            item,
            ["match_type", "target_match_type", "matchType", "targeting_match_type"],
            "",
        )
        top_of_search = _pick_first_float(
            item,
            ["top_of_search", "top_of_search_is", "top_of_search_impression_share"],
        )

        rows.append(
            {
                "date": day_text,
                "ad_combo": sponsored_type.upper(),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                "keyword": keyword,
                "target_match_type": target_match_type,
                "top_of_search": round(float(top_of_search), 4) if top_of_search is not None else 0.0,
                **metrics,
            }
        )
    return rows


def _normalize_negative_targeting_rows(
    report_rows: List[Dict[str, Any]],
    fallback_day: str,
    campaign_name_map: Dict[Tuple[str, int], str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in report_rows:
        sponsored_type = _pick_first_str(item, ["sponsored_type"], "").lower()
        campaign_id = _pick_first_int(item, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        if not sponsored_type or campaign_id is None or ad_group_id is None:
            continue

        metrics = _extract_common_metrics(item)
        day_text = _extract_day_text(item, fallback_day)
        campaign_name = campaign_name_map.get((sponsored_type, campaign_id), "").strip() or _pick_first_str(
            item,
            ["campaign_name", "campaignName", "campaign"],
            f"campaign_{campaign_id}",
        )
        ad_group_name = _pick_first_str(
            item,
            ["ad_group_name", "adGroupName", "ad_group", "adgroup_name", "name"],
            f"adgroup_{ad_group_id}",
        )
        matches_product = _pick_first_str(
            item,
            ["matches_product", "matched_product", "matched_asin", "targeting_expression"],
            "",
        )
        keywords = _pick_first_str(
            item,
            ["keyword", "keywords", "negative_keyword", "target", "negative_target"],
            "",
        )

        rows.append(
            {
                "date": day_text,
                "ad_combo": sponsored_type.upper(),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                "matches_product": matches_product,
                "keywords": keywords,
                **metrics,
            }
        )
    return rows


def _normalize_ads_rows(
    report_rows: List[Dict[str, Any]],
    fallback_day: str,
    campaign_name_map: Dict[Tuple[str, int], str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in report_rows:
        sponsored_type = _pick_first_str(item, ["sponsored_type"], "").lower()
        campaign_id = _pick_first_int(item, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        if not sponsored_type or campaign_id is None or ad_group_id is None:
            continue

        metrics = _extract_common_metrics(item)
        day_text = _extract_day_text(item, fallback_day)
        campaign_name = campaign_name_map.get((sponsored_type, campaign_id), "").strip() or _pick_first_str(
            item,
            ["campaign_name", "campaignName", "campaign"],
            f"campaign_{campaign_id}",
        )
        ad_group_name = _pick_first_str(
            item,
            ["ad_group_name", "adGroupName", "ad_group", "adgroup_name", "name"],
            f"adgroup_{ad_group_id}",
        )
        asin = _pick_first_str(item, ["asin", "advertised_asin", "sku_asin", "product_asin"], "")

        rows.append(
            {
                "date": day_text,
                "ad_combo": sponsored_type.upper(),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                "asin": asin,
                **metrics,
            }
        )
    return rows


def _build_campaign_daily_rows(
    report_rows: List[Dict[str, Any]],
    fallback_day: str,
    campaign_name_map: Dict[Tuple[str, int], str],
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, int, str], Dict[str, Any]] = {}
    for item in report_rows:
        sponsored_type = _pick_first_str(item, ["sponsored_type"], "").lower()
        campaign_id = _pick_first_int(item, ["campaign_id", "campaignId", "campaign"])
        if not sponsored_type or campaign_id is None:
            continue

        day_text = _extract_day_text(item, fallback_day)
        key = (sponsored_type, campaign_id, day_text)
        if key not in grouped:
            grouped[key] = {
                "date": day_text,
                "ad_combo": sponsored_type.upper(),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name_map.get((sponsored_type, campaign_id), "").strip()
                or _pick_first_str(item, ["campaign_name", "campaignName", "campaign"], f"campaign_{campaign_id}"),
                "impression": 0.0,
                "clicks": 0.0,
                "total_cost": 0.0,
                "purchase": 0.0,
                "sales": 0.0,
            }

        metrics = _extract_common_metrics(item)
        grouped[key]["impression"] += metrics["impression"]
        grouped[key]["clicks"] += metrics["clicks"]
        grouped[key]["total_cost"] += metrics["total_cost"]
        grouped[key]["purchase"] += metrics["purchase"]
        grouped[key]["sales"] += metrics["sales"]

    rows: List[Dict[str, Any]] = []
    for _, item in grouped.items():
        impressions = float(item["impression"])
        clicks = float(item["clicks"])
        cost = float(item["total_cost"])
        purchase = float(item["purchase"])
        sales = float(item["sales"])
        rows.append(
            {
                "date": item["date"],
                "ad_combo": item["ad_combo"],
                "campaign_id": item["campaign_id"],
                "campaign_name": item["campaign_name"],
                "impression": round(impressions, 2),
                "clicks": round(clicks, 2),
                "total_cost": round(cost, 2),
                "cpc": round(_safe_div(cost, clicks), 4),
                "purchase": round(purchase, 2),
                "sales": round(sales, 2),
                "acos": round(_safe_div(cost, sales) * 100, 4),
                "purchase_rate": round(_safe_div(purchase, clicks) * 100, 4),
                "ctr": round(_safe_div(clicks, impressions) * 100, 4),
            }
        )

    rows.sort(key=lambda x: (x["ad_combo"], x["campaign_name"], x["date"]))
    return rows


def _build_full_change_records(store_id: str, op_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for log in op_logs:
        operate_type = str(log.get("operate_type") or "").strip()
        if operate_type not in {"adGroups", "campaigns"}:
            continue

        rows.append(
            {
                "store_id": store_id,
                "date": _extract_operate_day(log, fallback_day=date.today()).isoformat(),
                "operate_time": str(log.get("operate_time") or ""),
                "ad_combo": str(log.get("sponsored_type") or "").upper(),
                "operate_type": operate_type,
                "object_id": str(log.get("object_id") or ""),
                "object_name": str(log.get("object_name") or ""),
                "operate_before": log.get("operate_before") or [],
                "operate_after": log.get("operate_after") or [],
            }
        )
    rows.sort(key=lambda x: (x["date"], x["ad_combo"], x["operate_type"], x["object_name"]))
    return rows


def _persist_full_dataset_payload(
    store_id: str,
    window: SyncWindow,
    payload: Dict[str, Any],
) -> str:
    output_dir = DATA_DIR / "lingxing_sync"
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = (
        f"{store_id}_dataset_{window.start_date.isoformat()}_{window.end_date.isoformat()}.json"
    )
    path = output_dir / filename
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _latest_dataset_cache_path(store_id: str) -> Path:
    output_dir = DATA_DIR / "lingxing_sync"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"{store_id}_latest.json"


def _empty_local_dataset(store_id: str, store_name: str) -> Dict[str, Any]:
    return {
        "store_id": store_id,
        "store_name": store_name,
        "updated_at": "",
        "coverage": {
            "ad_group_reports": [],
            "targeting": [],
            "negative_targeting": [],
            "ads": [],
            "change_logs": [],
        },
        "ad_group_reports_by_day_ad_group": [],
        "targeting_by_day_ad_group": [],
        "negative_targeting_by_day_ad_group": [],
        "ads_by_day_ad_group": [],
        "change_history_by_ad_group_campaign": [],
    }


def _load_local_dataset(store_id: str, store_name: str) -> Dict[str, Any]:
    path = _latest_dataset_cache_path(store_id)
    if not path.exists():
        return _empty_local_dataset(store_id=store_id, store_name=store_name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return _empty_local_dataset(store_id=store_id, store_name=store_name)
    except (OSError, json.JSONDecodeError):
        return _empty_local_dataset(store_id=store_id, store_name=store_name)

    payload.setdefault("store_id", store_id)
    payload.setdefault("store_name", store_name)
    payload.setdefault("updated_at", "")
    payload.setdefault("coverage", {})
    payload["coverage"].setdefault("ad_group_reports", [])
    payload["coverage"].setdefault("targeting", [])
    payload["coverage"].setdefault("negative_targeting", [])
    payload["coverage"].setdefault("ads", [])
    payload["coverage"].setdefault("change_logs", [])
    payload.setdefault("ad_group_reports_by_day_ad_group", [])
    payload.setdefault("targeting_by_day_ad_group", [])
    payload.setdefault("negative_targeting_by_day_ad_group", [])
    payload.setdefault("ads_by_day_ad_group", [])
    payload.setdefault("change_history_by_ad_group_campaign", [])
    return payload


def _save_local_dataset(local_dataset: Dict[str, Any]) -> None:
    store_id = str(local_dataset.get("store_id") or "").strip()
    if not store_id:
        return
    local_dataset["updated_at"] = datetime.utcnow().isoformat()
    path = _latest_dataset_cache_path(store_id)
    path.write_text(json.dumps(local_dataset, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_daily_rows_from_ad_group_rows(
    store_id: str,
    requested_days: List[str],
    ad_group_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_day: Dict[str, Dict[str, float]] = {}
    for row in ad_group_rows:
        day = str(row.get("date") or "").strip()
        if not day:
            continue
        bucket = by_day.setdefault(day, {"clicks": 0.0, "cost": 0.0, "sales": 0.0})
        bucket["clicks"] += float(row.get("clicks", 0) or 0)
        bucket["cost"] += float(row.get("total_cost", row.get("cost", row.get("spend", 0))) or 0)
        bucket["sales"] += float(row.get("sales", 0) or 0)

    rows: List[Dict[str, Any]] = []
    for day in requested_days:
        bucket = by_day.get(day, {"clicks": 0.0, "cost": 0.0, "sales": 0.0})
        spend = float(bucket["cost"])
        sales = float(bucket["sales"])
        rows.append(
            {
                "store_id": store_id,
                "date": day,
                "clicks": int(round(float(bucket["clicks"]))),
                "spend": round(spend, 2),
                "acos": round((spend / sales) * 100, 2) if sales > 0 else 0.0,
                "sales": round(sales, 2),
            }
        )
    return rows


def _build_log_like_rows(change_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in change_records:
        rows.append(
            {
                "operate_time": row.get("operate_time"),
                "operate_before": row.get("operate_before") or [],
                "operate_after": row.get("operate_after") or [],
                "object_name": row.get("object_name"),
                "object_id": row.get("object_id"),
                "sponsored_type": str(row.get("ad_combo") or "").lower(),
            }
        )
    return rows


def _suggest_bid(current_bid: float | None, acos: float) -> float | None:
    if current_bid is None:
        return None
    if acos > 45:
        factor = 0.85
    elif acos > 30:
        factor = 0.92
    elif acos < 20:
        factor = 1.10
    else:
        factor = 1.0
    return round(current_bid * factor, 2)


def _build_lingxing_output_rows(
    report_rows: List[Dict[str, Any]],
    bid_snapshots: List[Dict[str, Any]],
    campaign_name_map: Dict[Tuple[str, int], str],
) -> List[Dict[str, Any]]:
    snapshot_by_key: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for snap in bid_snapshots:
        sponsored_type = str(snap.get("sponsored_type") or "").strip().lower()
        ad_group_id = _pick_first_int(snap, ["ad_group_id", "adGroupId"])
        if not sponsored_type or ad_group_id is None:
            continue
        snapshot_by_key[(sponsored_type, ad_group_id)] = snap

    grouped: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for item in report_rows:
        ad_combo = str(item.get("sponsored_type") or "").strip().upper() or "UNKNOWN"
        sponsored_type = ad_combo.lower()

        campaign_id = _pick_first_int(item, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        if campaign_id is None or ad_group_id is None:
            continue

        snapshot = snapshot_by_key.get((sponsored_type, ad_group_id), {})

        campaign = campaign_name_map.get((sponsored_type, campaign_id), "").strip()
        if not campaign:
            campaign = _pick_first_str(
                item,
                [
                    "campaign_name",
                    "campaignName",
                    "campaign",
                    "campaign_name_cn",
                    "campaign_name_en",
                ],
                _pick_first_str(
                    snapshot,
                    ["campaign_name", "campaignName", "campaign"],
                    f"campaign_{campaign_id}",
                ),
            )
        ad_group = _pick_first_str(
            item,
            [
                "ad_group_name",
                "adGroupName",
                "ad_group",
                "adgroup_name",
                "name",
            ],
            _pick_first_str(
                snapshot,
                ["ad_group", "ad_group_name", "adGroupName", "name"],
                f"adgroup_{ad_group_id}",
            ),
        )
        key = (sponsored_type, campaign_id, ad_group_id)

        clicks = int(float(item.get("clicks", item.get("click", 0)) or 0))
        spend = float(item.get("cost", item.get("total_cost", item.get("spend", 0))) or 0)
        sales = float(
            item.get(
                "sales",
                item.get(
                    "same_sales",
                    item.get("attributed_sales", 0),
                ),
            )
            or 0
        )

        if key not in grouped:
            grouped[key] = {
                "ad_combo": ad_combo,
                "sponsored_type": sponsored_type,
                "campaign_id": campaign_id,
                "campaign": campaign,
                "ad_group_id": ad_group_id,
                "ad_group": ad_group,
                "clicks": 0,
                "spend": 0.0,
                "sales": 0.0,
            }

        grouped[key]["clicks"] += clicks
        grouped[key]["spend"] += spend
        grouped[key]["sales"] += sales

    rows: List[Dict[str, Any]] = []
    for (_, _, _), row in grouped.items():
        spend = float(row["spend"])
        if spend <= 0:
            continue

        sales = float(row["sales"])
        acos = round((spend / sales) * 100, 2) if sales > 0 else 0.0

        snapshot = snapshot_by_key.get((row["sponsored_type"], row["ad_group_id"]), {})
        current_bid = _to_float(snapshot.get("current_bid"))
        suggested_bid = _suggest_bid(current_bid=current_bid, acos=acos)

        rows.append(
            {
                "ad_combo": row["ad_combo"],
                "campaign_id": row["campaign_id"],
                "campaign_name": str(row["campaign"]),
                "campaign": str(row["campaign"]),
                "ad_group_id": row["ad_group_id"],
                "ad_group": row["ad_group"],
                "current_bid": round(current_bid, 2) if current_bid is not None else None,
                "suggested_bid": round(suggested_bid, 2) if suggested_bid is not None else None,
                "clicks": int(row["clicks"]),
                "spend": round(spend, 2),
                "sales": round(sales, 2),
                "acos": acos,
            }
        )

    rows.sort(key=lambda x: (x["ad_combo"], x["campaign"], x["ad_group"]))
    return rows


def _extract_bid_change(log_row: Dict[str, Any]) -> Tuple[float, float, str] | None:
    before_items = log_row.get("operate_before") or []
    after_items = log_row.get("operate_after") or []

    before = {
        str(item.get("code")): item.get("value")
        for item in before_items
        if isinstance(item, dict) and item.get("code")
    }
    after = {
        str(item.get("code")): item.get("value")
        for item in after_items
        if isinstance(item, dict) and item.get("code")
    }

    keys = sorted(set(before.keys()) | set(after.keys()))
    for key in keys:
        upper = key.upper()
        if "BID" not in upper and "CPC" not in upper:
            continue

        old_bid = _to_float(before.get(key))
        new_bid = _to_float(after.get(key))
        if old_bid is None or new_bid is None:
            continue
        if abs(old_bid - new_bid) < 1e-9:
            continue

        return old_bid, new_bid, key

    return None


def _extract_operate_day(log_row: Dict[str, Any], fallback_day: date) -> date:
    operate_time = str(log_row.get("operate_time") or "").strip()
    if len(operate_time) >= 10:
        try:
            return datetime.strptime(operate_time[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    return fallback_day


def _ensure_default_playbook(store_id: str, store_name: str) -> None:
    PLAYBOOK_DIR.mkdir(parents=True, exist_ok=True)
    playbook_path = PLAYBOOK_DIR / f"store_playbook_{store_id}.json"

    if playbook_path.exists():
        try:
            existing = json.loads(playbook_path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                existing_name = str(existing.get("store_name") or "").strip()
                if existing_name != store_name:
                    existing["store_name"] = store_name
                    playbook_path.write_text(
                        json.dumps(existing, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
        except (OSError, json.JSONDecodeError):
            pass
        return

    default_playbook = {
        "store_id": store_id,
        "store_name": store_name,
        "rules": {
            "negative_search_terms": "Conservative Negative Search Terms",
            "acos_limit": 45,
            "bid_step_up_pct": 8,
            "bid_step_down_pct": 12,
            "focus": f"Prioritize efficient scaling for {store_name}",
        },
    }

    with playbook_path.open("w", encoding="utf-8") as f:
        json.dump(default_playbook, f, ensure_ascii=False, indent=2)


def _persist_store_frames(store_id: str, perf_df: pd.DataFrame, history_df: pd.DataFrame) -> None:
    PERFORMANCE_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    perf_out = perf_df.copy()
    history_out = history_df.copy()

    perf_out["date"] = perf_out["date"].astype(str)
    history_out["date"] = history_out["date"].astype(str)

    perf_out.to_csv(PERFORMANCE_DIR / f"{store_id}.csv", index=False)
    history_out.to_csv(HISTORY_DIR / f"{store_id}.csv", index=False)


def _build_store_history_rows(
    store_id: str,
    end_day: date,
    op_logs: List[Dict[str, Any]],
    snapshots: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for log_row in op_logs:
        bid_change = _extract_bid_change(log_row)
        if not bid_change:
            continue

        old_bid, new_bid, bid_code = bid_change
        operate_day = _extract_operate_day(log_row, fallback_day=end_day)
        ad_group = (
            str(log_row.get("ad_group_name") or "").strip()
            or str(log_row.get("object_name") or "").strip()
            or f"{log_row.get('sponsored_type', 'ad')}_{log_row.get('object_id', 'unknown')}"
        )

        rows.append(
            {
                "store_id": store_id,
                "date": operate_day.isoformat(),
                "ad_group": ad_group,
                "action_type": f"bid_change_{bid_code.lower()}",
                "old_bid": round(old_bid, 4),
                "new_bid": round(new_bid, 4),
            }
        )

    snapshot_day = end_day.isoformat()
    for item in snapshots:
        current_bid = _to_float(item.get("current_bid"))
        if current_bid is None:
            continue

        rows.append(
            {
                "store_id": store_id,
                "date": snapshot_day,
                "ad_group": str(item.get("ad_group") or "UNKNOWN_AD_GROUP"),
                "action_type": "snapshot",
                "old_bid": round(current_bid, 4),
                "new_bid": round(current_bid, 4),
            }
        )

    if not rows:
        rows.append(
            {
                "store_id": store_id,
                "date": snapshot_day,
                "ad_group": "UNKNOWN_AD_GROUP",
                "action_type": "snapshot",
                "old_bid": 1.0,
                "new_bid": 1.0,
            }
        )

    return rows


def sync_lingxing_data(
    store_id: str | None = None,
    report_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    persist: bool = True,
) -> Dict[str, Any]:
    window = resolve_sync_window(report_date=report_date, start_date=start_date, end_date=end_date)

    credentials = LingxingCredentials.from_env()
    client = LingxingClient(credentials=credentials)

    access_token = client.generate_access_token()
    sellers = client.list_sellers(access_token=access_token)

    valid_sellers = [
        item
        for item in sellers
        if int(item.get("status", 0) or 0) == 1 and int(item.get("has_ads_setting", 0) or 0) == 1
    ]
    target_store_id = (store_id or "").strip()
    if target_store_id:
        if not target_store_id.startswith("lingxing_"):
            raise ValueError(
                "Lingxing sync supports Lingxing stores only. "
                "Please use a store_id like lingxing_<sid>."
            )

        valid_sellers = [
            item for item in valid_sellers if f"lingxing_{int(item['sid'])}" == target_store_id
        ]
        if not valid_sellers:
            raise ValueError(
                f"No Lingxing ads-enabled store matched store_id={target_store_id}. "
                "Please confirm current store is linked in Lingxing and ads are enabled."
            )

    store_results: List[Dict[str, Any]] = []

    for seller in valid_sellers:
        sid = int(seller["sid"])
        store_id = f"lingxing_{sid}"
        store_name = str(seller.get("name") or store_id)
        campaign_name_map = client.fetch_campaign_names(access_token=access_token, sid=sid)
        bid_snapshots = client.fetch_bid_snapshots(access_token=access_token, sid=sid)
        requested_days = [day.isoformat() for day in _date_iter(window.start_date, window.end_date)]

        local_dataset = _load_local_dataset(store_id=store_id, store_name=store_name)
        coverage = local_dataset.get("coverage", {})
        ad_covered = set(str(x) for x in coverage.get("ad_group_reports", []))
        targeting_covered = set(str(x) for x in coverage.get("targeting", []))
        negative_covered = set(str(x) for x in coverage.get("negative_targeting", []))
        ads_covered = set(str(x) for x in coverage.get("ads", []))
        change_covered = set(str(x) for x in coverage.get("change_logs", []))

        missing_ad_days = _missing_days(requested_days, ad_covered)
        missing_targeting_days = _missing_days(requested_days, targeting_covered)
        missing_negative_days = _missing_days(requested_days, negative_covered)
        missing_ads_days = _missing_days(requested_days, ads_covered)
        missing_change_days = _missing_days(requested_days, change_covered)

        new_ad_group_rows: List[Dict[str, Any]] = []
        new_targeting_rows: List[Dict[str, Any]] = []
        new_negative_rows: List[Dict[str, Any]] = []
        new_ads_rows: List[Dict[str, Any]] = []
        for day_text in requested_days:
            if day_text in missing_ad_days:
                report_rows = client.fetch_ad_reports_for_day(
                    access_token=access_token,
                    sid=sid,
                    report_date=day_text,
                )
                new_ad_group_rows.extend(
                    _normalize_ad_group_report_rows(
                        report_rows=report_rows,
                        fallback_day=day_text,
                        campaign_name_map=campaign_name_map,
                    )
                )

            if day_text in missing_targeting_days:
                targeting_raw_rows = client.fetch_targeting_reports_for_day(
                    access_token=access_token,
                    sid=sid,
                    report_date=day_text,
                )
                new_targeting_rows.extend(
                    _normalize_targeting_rows(
                        report_rows=targeting_raw_rows,
                        fallback_day=day_text,
                        campaign_name_map=campaign_name_map,
                    )
                )

            if day_text in missing_negative_days:
                negative_raw_rows = client.fetch_negative_targeting_reports_for_day(
                    access_token=access_token,
                    sid=sid,
                    report_date=day_text,
                )
                new_negative_rows.extend(
                    _normalize_negative_targeting_rows(
                        report_rows=negative_raw_rows,
                        fallback_day=day_text,
                        campaign_name_map=campaign_name_map,
                    )
                )

            if day_text in missing_ads_days:
                ads_raw_rows = client.fetch_ads_reports_for_day(
                    access_token=access_token,
                    sid=sid,
                    report_date=day_text,
                )
                new_ads_rows.extend(
                    _normalize_ads_rows(
                        report_rows=ads_raw_rows,
                        fallback_day=day_text,
                        campaign_name_map=campaign_name_map,
                    )
                )

        op_logs: List[Dict[str, Any]] = []
        new_full_change_records: List[Dict[str, Any]] = []
        for chunk_start, chunk_end in _build_missing_date_ranges(missing_change_days, max_span_days=30):
            chunk_logs = client.fetch_operation_logs(
                access_token=access_token,
                sid=sid,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
            )
            op_logs.extend(chunk_logs)
            new_full_change_records.extend(
                _build_full_change_records(
                    store_id=store_id,
                    op_logs=chunk_logs,
                )
            )

        local_dataset["store_name"] = store_name
        local_dataset["ad_group_reports_by_day_ad_group"] = _upsert_rows_by_key(
            existing_rows=list(local_dataset.get("ad_group_reports_by_day_ad_group", [])),
            new_rows=new_ad_group_rows,
            key_fields=["date", "ad_combo", "campaign_id", "ad_group_id"],
        )
        local_dataset["targeting_by_day_ad_group"] = _upsert_rows_by_key(
            existing_rows=list(local_dataset.get("targeting_by_day_ad_group", [])),
            new_rows=new_targeting_rows,
            key_fields=["date", "ad_combo", "campaign_id", "ad_group_id", "keyword", "target_match_type"],
        )
        local_dataset["negative_targeting_by_day_ad_group"] = _upsert_rows_by_key(
            existing_rows=list(local_dataset.get("negative_targeting_by_day_ad_group", [])),
            new_rows=new_negative_rows,
            key_fields=["date", "ad_combo", "campaign_id", "ad_group_id", "matches_product", "keywords"],
        )
        local_dataset["ads_by_day_ad_group"] = _upsert_rows_by_key(
            existing_rows=list(local_dataset.get("ads_by_day_ad_group", [])),
            new_rows=new_ads_rows,
            key_fields=["date", "ad_combo", "campaign_id", "ad_group_id", "asin"],
        )
        local_dataset["change_history_by_ad_group_campaign"] = _upsert_rows_by_key(
            existing_rows=list(local_dataset.get("change_history_by_ad_group_campaign", [])),
            new_rows=new_full_change_records,
            key_fields=["date", "operate_time", "ad_combo", "operate_type", "object_id", "object_name"],
        )

        coverage = local_dataset.setdefault("coverage", {})
        coverage["ad_group_reports"] = _sort_unique_dates(
            list(coverage.get("ad_group_reports", [])) + missing_ad_days
        )
        coverage["targeting"] = _sort_unique_dates(
            list(coverage.get("targeting", [])) + missing_targeting_days
        )
        coverage["negative_targeting"] = _sort_unique_dates(
            list(coverage.get("negative_targeting", [])) + missing_negative_days
        )
        coverage["ads"] = _sort_unique_dates(
            list(coverage.get("ads", [])) + missing_ads_days
        )
        coverage["change_logs"] = _sort_unique_dates(
            list(coverage.get("change_logs", [])) + missing_change_days
        )

        if persist:
            _save_local_dataset(local_dataset)

        ad_group_rows_window = _filter_rows_by_window(
            list(local_dataset.get("ad_group_reports_by_day_ad_group", [])),
            start_day=window.start_date,
            end_day=window.end_date,
        )
        targeting_rows_all = _filter_rows_by_window(
            list(local_dataset.get("targeting_by_day_ad_group", [])),
            start_day=window.start_date,
            end_day=window.end_date,
        )
        negative_targeting_rows_all = _filter_rows_by_window(
            list(local_dataset.get("negative_targeting_by_day_ad_group", [])),
            start_day=window.start_date,
            end_day=window.end_date,
        )
        ads_rows_all = _filter_rows_by_window(
            list(local_dataset.get("ads_by_day_ad_group", [])),
            start_day=window.start_date,
            end_day=window.end_date,
        )
        full_change_records = _filter_rows_by_window(
            list(local_dataset.get("change_history_by_ad_group_campaign", [])),
            start_day=window.start_date,
            end_day=window.end_date,
        )
        campaign_daily_rows_all = _build_campaign_daily_rows(
            report_rows=ad_group_rows_window,
            fallback_day=window.end_date.isoformat(),
            campaign_name_map=campaign_name_map,
        )

        daily_rows = _build_daily_rows_from_ad_group_rows(
            store_id=store_id,
            requested_days=requested_days,
            ad_group_rows=ad_group_rows_window,
        )
        history_rows = _build_store_history_rows(
            store_id=store_id,
            end_day=window.end_date,
            op_logs=_build_log_like_rows(full_change_records),
            snapshots=bid_snapshots,
        )

        perf_df = pd.DataFrame(daily_rows)
        history_df = pd.DataFrame(history_rows)
        perf_df["date"] = pd.to_datetime(perf_df["date"]).dt.date
        history_df["date"] = pd.to_datetime(history_df["date"]).dt.date

        cases = build_optimization_cases(
            store_id=store_id,
            history_df=history_df,
            perf_df=perf_df,
        )
        recommendations = build_bid_recommendations(
            store_id=store_id,
            history_df=history_df,
            perf_df=perf_df,
        )
        lingxing_output_rows = _build_lingxing_output_rows(
            report_rows=ad_group_rows_window,
            bid_snapshots=bid_snapshots,
            campaign_name_map=campaign_name_map,
        )

        full_dataset_payload = {
            "store_id": store_id,
            "store_name": store_name,
            "window": {
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
            },
            "targeting_by_day_ad_group": targeting_rows_all,
            "negative_targeting_by_day_ad_group": negative_targeting_rows_all,
            "ads_by_day_ad_group": ads_rows_all,
            "ad_groups_by_day_campaign": campaign_daily_rows_all,
            "change_history_by_ad_group_campaign": full_change_records,
        }

        full_dataset_path = ""
        if persist:
            _persist_store_frames(store_id=store_id, perf_df=perf_df, history_df=history_df)
            _ensure_default_playbook(store_id=store_id, store_name=store_name)
            full_dataset_path = _persist_full_dataset_payload(
                store_id=store_id,
                window=window,
                payload=full_dataset_payload,
            )

        latest_perf = perf_df.sort_values("date").iloc[-1].to_dict()
        latest_perf["date"] = latest_perf["date"].isoformat()

        store_results.append(
            {
                "store_id": store_id,
                "sid": sid,
                "store_name": store_name,
                "country": seller.get("country"),
                "daily_points": len(daily_rows),
                "operation_logs": len(op_logs),
                "bid_snapshots": len(bid_snapshots),
                "history_rows": len(history_rows),
                "history_change_records_count": len(full_change_records),
                "history_change_records_sample": full_change_records[:20],
                "targeting_rows": len(targeting_rows_all),
                "negative_targeting_rows": len(negative_targeting_rows_all),
                "ads_rows": len(ads_rows_all),
                "campaign_daily_rows": len(campaign_daily_rows_all),
                "local_cache_hit_days": {
                    "ad_group_reports": len(requested_days) - len(missing_ad_days),
                    "targeting": len(requested_days) - len(missing_targeting_days),
                    "negative_targeting": len(requested_days) - len(missing_negative_days),
                    "ads": len(requested_days) - len(missing_ads_days),
                    "change_logs": len(requested_days) - len(missing_change_days),
                },
                "fetched_missing_days": {
                    "ad_group_reports": len(missing_ad_days),
                    "targeting": len(missing_targeting_days),
                    "negative_targeting": len(missing_negative_days),
                    "ads": len(missing_ads_days),
                    "change_logs": len(missing_change_days),
                },
                "full_dataset_path": full_dataset_path,
                "lingxing_output_rows": lingxing_output_rows,
                "latest_performance": latest_perf,
                "recommendations": recommendations,
                "optimization_cases": cases,
            }
        )

    return {
        "target_store_id": target_store_id or None,
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
        },
        "stores_total": len(sellers),
        "stores_ads_enabled": len(valid_sellers),
        "stores_synced": len(store_results),
        "stores": store_results,
    }

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .lingxing_client import LingxingClient, LingxingCredentials


ProgressCallback = Callable[[str, int, str], None]


@dataclass
class ContextWindow:
    start_date: date
    end_date: date


def _parse_date_or_none(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def resolve_context_window(
    start_date: str | None = None,
    end_date: str | None = None,
    days: int = 365,
) -> ContextWindow:
    start_day = _parse_date_or_none(start_date)
    end_day = _parse_date_or_none(end_date)

    if (start_day is None) != (end_day is None):
        raise ValueError("start_date and end_date must be provided together")

    if start_day and end_day:
        if start_day > end_day:
            raise ValueError("start_date must be <= end_date")
        return ContextWindow(start_date=start_day, end_date=end_day)

    if days < 1:
        raise ValueError("days must be >= 1")
    if days > 1095:
        raise ValueError("days must be <= 1095")

    resolved_end = date.today() - timedelta(days=1)
    resolved_start = resolved_end - timedelta(days=days - 1)
    return ContextWindow(start_date=resolved_start, end_date=resolved_end)


def _date_iter(start_day: date, end_day: date) -> List[date]:
    days: List[date] = []
    current = start_day
    while current <= end_day:
        days.append(current)
        current += timedelta(days=1)
    return days


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        cleaned = text.replace(",", "").replace("%", "")
        try:
            return float(cleaned)
        except ValueError:
            match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
            if not match:
                return None
            return float(match.group())


def _to_int(value: Any) -> int | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _pick_first_int(item: Dict[str, Any], keys: List[str]) -> int | None:
    for key in keys:
        parsed = _to_int(item.get(key))
        if parsed is not None:
            return parsed
    return None


def _pick_first_float(item: Dict[str, Any], keys: List[str]) -> float | None:
    for key in keys:
        parsed = _to_float(item.get(key))
        if parsed is not None:
            return parsed
    return None


def _pick_first_str(item: Dict[str, Any], keys: List[str], fallback: str = "") -> str:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return fallback


def _normalize_currency(value: float | None) -> float:
    if value is None:
        return 0.0
    return round(float(value), 2)


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


def _stock_status(avg_stock: float) -> str:
    if avg_stock <= 0:
        return "out_of_stock"
    if avg_stock < 10:
        return "low"
    return "healthy"


def _remove_nulls(payload: Any) -> Any:
    if isinstance(payload, dict):
        cleaned: Dict[str, Any] = {}
        for key, value in payload.items():
            value_clean = _remove_nulls(value)
            if value_clean is None:
                continue
            cleaned[key] = value_clean
        return cleaned
    if isinstance(payload, list):
        return [_remove_nulls(item) for item in payload]
    return payload


def _emit_progress(
    progress_cb: Optional[ProgressCallback],
    stage: str,
    pct: int,
    msg: str,
) -> None:
    if progress_cb is None:
        return
    try:
        progress_cb(stage, max(0, min(100, int(pct))), msg)
    except Exception:
        # Progress callbacks should not break package generation.
        return


def _resolve_target_seller(
    store_id: str,
    sellers: List[Dict[str, Any]],
) -> Tuple[int, str, str]:
    if not store_id.strip():
        raise ValueError("store_id is required")

    target = store_id.strip()
    if target.startswith("lingxing_"):
        sid_text = target.split("lingxing_", 1)[1]
        sid = int(sid_text)
        for seller in sellers:
            if int(seller.get("sid", 0) or 0) == sid:
                name = str(seller.get("name") or target)
                resolved_store_id = f"lingxing_{sid}"
                return sid, resolved_store_id, name

        raise ValueError(f"store_id not found in Lingxing sellers: {target}")

    for seller in sellers:
        name = str(seller.get("name") or "").strip()
        if name and name == target:
            sid = int(seller["sid"])
            resolved_store_id = f"lingxing_{sid}"
            return sid, resolved_store_id, name

    raise ValueError(
        "store_id must be lingxing_<sid> or exact seller name from Lingxing."
    )


def build_lingxing_context_package(
    store_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    days: int = 365,
    progress_cb: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    _emit_progress(progress_cb, "init", 1, "Preparing context package request")
    window = resolve_context_window(start_date=start_date, end_date=end_date, days=days)

    _emit_progress(progress_cb, "auth", 4, "Generating Lingxing access token")
    credentials = LingxingCredentials.from_env()
    client = LingxingClient(credentials=credentials)
    access_token = client.generate_access_token()

    _emit_progress(progress_cb, "load_sellers", 8, "Loading Lingxing sellers")
    sellers = client.list_sellers(access_token=access_token)
    sellers = [
        item
        for item in sellers
        if int(item.get("status", 0) or 0) == 1 and int(item.get("has_ads_setting", 0) or 0) == 1
    ]

    _emit_progress(progress_cb, "resolve_store", 10, "Resolving selected store")
    sid, resolved_store_id, store_name = _resolve_target_seller(store_id=store_id, sellers=sellers)

    campaign_name_map = client.fetch_campaign_names(access_token=access_token, sid=sid)
    bid_snapshots = client.fetch_bid_snapshots(access_token=access_token, sid=sid)
    query_rows_all: List[Dict[str, Any]] = []
    report_rows_all: List[Dict[str, Any]] = []
    placement_rows_all: List[Dict[str, Any]] = []
    days_in_window = _date_iter(window.start_date, window.end_date)
    days_count = max(1, len(days_in_window))

    _emit_progress(progress_cb, "daily_reports", 12, "Fetching ad group and search-term reports")
    for idx, day in enumerate(days_in_window):
        day_text = day.isoformat()
        report_rows_all.extend(
            client.fetch_ad_reports_for_day(
                access_token=access_token,
                sid=sid,
                report_date=day_text,
            )
        )
        pct = 12 + int(((idx + 1) / days_count) * 36)
        _emit_progress(
            progress_cb,
            "daily_reports",
            pct,
            f"Fetched daily reports {idx + 1}/{days_count}",
        )
        query_rows_all.extend(
            client.fetch_query_word_reports_for_day(
                access_token=access_token,
                sid=sid,
                report_date=day_text,
            )
        )

    active_campaign_keys: Set[Tuple[str, int]] = set()
    for row in report_rows_all:
        sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
        campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
        if sponsored_type and campaign_id is not None:
            active_campaign_keys.add((sponsored_type, campaign_id))

    _emit_progress(progress_cb, "placement_reports", 50, "Fetching placement reports")
    for idx, day in enumerate(days_in_window):
        day_text = day.isoformat()
        day_placements = client.fetch_campaign_placement_reports_for_day(
            access_token=access_token,
            sid=sid,
            report_date=day_text,
        )
        for row in day_placements:
            sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
            campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
            if not sponsored_type or campaign_id is None:
                continue
            if (sponsored_type, campaign_id) not in active_campaign_keys:
                continue
            placement_rows_all.append(row)
        pct = 50 + int(((idx + 1) / days_count) * 20)
        _emit_progress(
            progress_cb,
            "placement_reports",
            pct,
            f"Fetched placement reports {idx + 1}/{days_count}",
        )

    _emit_progress(progress_cb, "product_links", 72, "Fetching ad group product links")
    ad_product_links = client.fetch_ad_group_product_links(access_token=access_token, sid=sid)

    # Map ad_group -> ASINs
    ad_group_asins: Dict[Tuple[str, int], Set[str]] = {}
    for item in ad_product_links:
        sponsored_type = str(item.get("sponsored_type") or "").strip().lower()
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        if not sponsored_type or ad_group_id is None:
            continue

        asin = _pick_first_str(
            item,
            ["asin", "advertised_asin", "ad_asin", "same_asin", "sku_asin"],
        ).upper()
        if not asin:
            continue

        key = (sponsored_type, ad_group_id)
        ad_group_asins.setdefault(key, set()).add(asin)

    all_asins = sorted({asin for values in ad_group_asins.values() for asin in values})
    _emit_progress(progress_cb, "product_listings", 76, "Fetching product listing metrics")
    product_listings = client.fetch_product_listings(
        access_token=access_token,
        sid=sid,
        asins=all_asins if all_asins else None,
    )

    asin_metrics_raw: Dict[str, Dict[str, List[float]]] = {}
    for row in product_listings:
        asin = _pick_first_str(
            row,
            ["asin", "amazon_asin", "market_asin", "listing_asin"],
        ).upper()
        if not asin:
            continue

        price = _pick_first_float(
            row,
            [
                "selling_price",
                "price",
                "sale_price",
                "listing_price",
                "current_price",
            ],
        )
        stock = _pick_first_float(
            row,
            [
                "stock_level",
                "stock",
                "available_stock",
                "inventory",
                "sellable_qty",
            ],
        )

        bucket = asin_metrics_raw.setdefault(asin, {"prices": [], "stocks": []})
        if price is not None:
            bucket["prices"].append(float(price))
        if stock is not None:
            bucket["stocks"].append(float(stock))

    asin_metrics: Dict[str, Dict[str, float]] = {}
    for asin, values in asin_metrics_raw.items():
        prices = values["prices"]
        stocks = values["stocks"]
        asin_metrics[asin] = {
            "selling_price": round(sum(prices) / len(prices), 2) if prices else 0.0,
            "stock_level": round(sum(stocks) / len(stocks), 2) if stocks else 0.0,
        }

    perf_by_day_group: Dict[Tuple[str, int, int, str], Dict[str, Any]] = {}
    ad_group_meta: Dict[Tuple[str, int, int], Dict[str, Any]] = {}

    for row in report_rows_all:
        sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
        campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId", "adgroup_id"])
        report_date = _pick_first_str(row, ["report_date", "date"])

        if not sponsored_type or campaign_id is None or ad_group_id is None or not report_date:
            continue

        meta_key = (sponsored_type, campaign_id, ad_group_id)
        campaign_name = campaign_name_map.get((sponsored_type, campaign_id), "").strip()
        if not campaign_name:
            campaign_name = _pick_first_str(
                row,
                ["campaign_name", "campaignName", "campaign"],
                f"campaign_{campaign_id}",
            )

        ad_group_name = _pick_first_str(
            row,
            ["ad_group_name", "adGroupName", "ad_group", "adgroup_name", "name"],
            f"adgroup_{ad_group_id}",
        )

        ad_group_meta.setdefault(
            meta_key,
            {
                "ad_combo": sponsored_type.upper(),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
            },
        )

        day_key = (sponsored_type, campaign_id, ad_group_id, report_date)
        record = perf_by_day_group.setdefault(
            day_key,
            {
                "clicks": 0,
                "spend": 0.0,
                "sales": 0.0,
            },
        )
        record["clicks"] += int(float(row.get("clicks", row.get("click", 0)) or 0))
        record["spend"] += float(row.get("cost", row.get("spend", 0)) or 0)
        record["sales"] += float(
            row.get(
                "sales",
                row.get(
                    "same_sales",
                    row.get("attributed_sales", 0),
                ),
            )
            or 0
        )

    bid_by_group: Dict[Tuple[str, int], float] = {}
    for row in bid_snapshots:
        sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
        ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId", "adgroup_id"])
        current_bid = _to_float(row.get("current_bid"))
        if not sponsored_type or ad_group_id is None or current_bid is None:
            continue
        bid_by_group[(sponsored_type, ad_group_id)] = float(current_bid)

    query_term_map: Dict[Tuple[str, int, str], Dict[str, Dict[str, Any]]] = {}
    for row in query_rows_all:
        sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
        ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId", "adgroup_id"])
        report_date = _pick_first_str(row, ["report_date", "date"])
        term = _pick_first_str(
            row,
            ["query_word", "queryWord", "search_term", "query", "customer_search_term"],
        )

        if not sponsored_type or ad_group_id is None or not report_date or not term:
            continue

        key = (sponsored_type, ad_group_id, report_date)
        term_bucket = query_term_map.setdefault(key, {})
        entry = term_bucket.setdefault(
            term,
            {
                "term": term,
                "clicks": 0,
                "spend": 0.0,
                "sales": 0.0,
                "orders": 0,
            },
        )
        entry["clicks"] += int(float(row.get("clicks", row.get("click", 0)) or 0))
        entry["spend"] += float(row.get("cost", row.get("spend", 0)) or 0)
        entry["sales"] += float(row.get("sales", row.get("same_sales", 0)) or 0)
        entry["orders"] += int(float(row.get("orders", row.get("same_orders", 0)) or 0))

    campaign_day_groups: Dict[Tuple[str, int, str], List[Tuple[int, Dict[str, float]]]] = {}
    for (sponsored_type, campaign_id, ad_group_id, report_date), values in perf_by_day_group.items():
        key = (sponsored_type, campaign_id, report_date)
        campaign_day_groups.setdefault(key, []).append(
            (
                ad_group_id,
                {
                    "clicks": float(values["clicks"]),
                    "spend": float(values["spend"]),
                    "sales": float(values["sales"]),
                },
            )
        )

    placement_by_group: Dict[Tuple[str, int, int, str], Dict[str, float | str]] = {}
    for row in placement_rows_all:
        sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
        campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
        report_date = _pick_first_str(row, ["report_date", "date"])
        placement_type = _pick_first_str(
            row,
            ["placement_type", "placement", "placement_name", "placementName"],
            "UNKNOWN_PLACEMENT",
        )
        top_of_search_is = _pick_first_float(
            row,
            [
                "top_of_search_is",
                "top_of_search_impression_share",
                "top_of_search_is_percent",
                "top_of_search_impression_share_percent",
                "top_of_search_is_rate",
            ],
        )

        if not sponsored_type or campaign_id is None or not report_date:
            continue

        placement_clicks = float(row.get("clicks", row.get("click", 0)) or 0)
        placement_spend = float(row.get("cost", row.get("spend", 0)) or 0)
        placement_sales = float(
            row.get(
                "sales",
                row.get(
                    "same_sales",
                    row.get("attributed_sales", 0),
                ),
            )
            or 0
        )

        explicit_ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId", "adgroup_id"])
        allocations: List[Tuple[int, float]]
        if explicit_ad_group_id is not None:
            allocations = [(explicit_ad_group_id, 1.0)]
        else:
            day_groups = campaign_day_groups.get((sponsored_type, campaign_id, report_date), [])
            if not day_groups:
                continue

            spend_total = sum(item[1]["spend"] for item in day_groups)
            clicks_total = sum(item[1]["clicks"] for item in day_groups)
            sales_total = sum(item[1]["sales"] for item in day_groups)

            allocations = []
            for ad_group_id, group_values in day_groups:
                if placement_spend > 0 and spend_total > 0:
                    share = group_values["spend"] / spend_total
                elif placement_clicks > 0 and clicks_total > 0:
                    share = group_values["clicks"] / clicks_total
                elif placement_sales > 0 and sales_total > 0:
                    share = group_values["sales"] / sales_total
                else:
                    share = 1.0 / len(day_groups)
                allocations.append((ad_group_id, share))

        for ad_group_id, share in allocations:
            allocation_key = (sponsored_type, campaign_id, ad_group_id, placement_type)
            placement_bucket = placement_by_group.setdefault(
                allocation_key,
                {
                    "placement_type": placement_type,
                    "clicks": 0.0,
                    "spend": 0.0,
                    "sales": 0.0,
                    "top_of_search_is_weighted_sum": 0.0,
                    "top_of_search_is_weight": 0.0,
                },
            )

            allocated_clicks = placement_clicks * share
            allocated_spend = placement_spend * share
            allocated_sales = placement_sales * share
            placement_bucket["clicks"] = float(placement_bucket["clicks"]) + allocated_clicks
            placement_bucket["spend"] = float(placement_bucket["spend"]) + allocated_spend
            placement_bucket["sales"] = float(placement_bucket["sales"]) + allocated_sales

            if top_of_search_is is not None:
                top_weight = allocated_spend if allocated_spend > 0 else allocated_clicks
                if top_weight <= 0:
                    top_weight = 1.0
                placement_bucket["top_of_search_is_weighted_sum"] = float(
                    placement_bucket["top_of_search_is_weighted_sum"]
                ) + (top_of_search_is * top_weight)
                placement_bucket["top_of_search_is_weight"] = float(
                    placement_bucket["top_of_search_is_weight"]
                ) + top_weight

    _emit_progress(progress_cb, "aggregate", 82, "Aggregating context package data")
    ad_groups: List[Dict[str, Any]] = []
    for (sponsored_type, campaign_id, ad_group_id), meta in sorted(
        ad_group_meta.items(),
        key=lambda x: (
            x[1]["ad_combo"],
            x[1]["campaign_name"],
            x[1]["ad_group_name"],
        ),
    ):
        day_records = [
            (day, values)
            for (stype, camp_id, group_id, day), values in perf_by_day_group.items()
            if stype == sponsored_type and camp_id == campaign_id and group_id == ad_group_id
        ]
        if not day_records:
            continue

        day_records.sort(key=lambda x: x[0])
        asins = sorted(ad_group_asins.get((sponsored_type, ad_group_id), set()))
        metrics = [asin_metrics[asin] for asin in asins if asin in asin_metrics]

        if metrics:
            avg_price = round(sum(item["selling_price"] for item in metrics) / len(metrics), 2)
            avg_stock = round(sum(item["stock_level"] for item in metrics) / len(metrics), 2)
            stock_status = _stock_status(avg_stock)
        else:
            avg_price = 0.0
            avg_stock = 0.0
            stock_status = "unknown"

        current_bid = bid_by_group.get((sponsored_type, ad_group_id))
        total_spend = sum(float(v["spend"]) for _, v in day_records)
        total_sales = sum(float(v["sales"]) for _, v in day_records)
        avg_acos = round((total_spend / total_sales) * 100, 2) if total_sales > 0 else 0.0
        suggested_bid = _suggest_bid(current_bid=current_bid, acos=avg_acos)

        timeline: List[Dict[str, Any]] = []
        for day, values in day_records:
            spend = _normalize_currency(float(values["spend"]))
            sales = _normalize_currency(float(values["sales"]))
            acos = round((spend / sales) * 100, 2) if sales > 0 else 0.0
            query_terms_raw = query_term_map.get((sponsored_type, ad_group_id, day), {})
            query_terms = sorted(
                query_terms_raw.values(),
                key=lambda x: (-int(x["clicks"]), -float(x["spend"]), str(x["term"])),
            )[:20]
            query_terms = [
                {
                    "term": item["term"],
                    "clicks": int(item["clicks"]),
                    "spend": _normalize_currency(item["spend"]),
                    "sales": _normalize_currency(item["sales"]),
                    "orders": int(item["orders"]),
                }
                for item in query_terms
            ]

            timeline.append(
                {
                    "date": day,
                    "clicks": int(values["clicks"]),
                    "spend": spend,
                    "sales": sales,
                    "acos": acos,
                    "currency": "USD",
                    "avg_selling_price": avg_price,
                    "avg_stock_level": avg_stock,
                    "stock_status": stock_status,
                    "keyword_search_terms": query_terms,
                }
            )

        placement_metrics: List[Dict[str, Any]] = []
        for (stype, camp_id, group_id, placement_type), values in placement_by_group.items():
            if stype != sponsored_type or camp_id != campaign_id or group_id != ad_group_id:
                continue

            spend = _normalize_currency(float(values["spend"]))
            sales = _normalize_currency(float(values["sales"]))
            acos = round((spend / sales) * 100, 2) if sales > 0 else 0.0
            top_weight = float(values["top_of_search_is_weight"])
            top_of_search_is = None
            if top_weight > 0:
                top_of_search_is = round(float(values["top_of_search_is_weighted_sum"]) / top_weight, 4)

            placement_metrics.append(
                {
                    "placement_type": str(placement_type),
                    "top_of_search_is": top_of_search_is,
                    "clicks": int(round(float(values["clicks"]))),
                    "spend": spend,
                    "sales": sales,
                    "acos": acos,
                    "currency": "USD",
                }
            )

        placement_metrics.sort(
            key=lambda x: (
                -float(x["spend"]),
                -int(x["clicks"]),
                str(x["placement_type"]),
            )
        )

        ad_groups.append(
            {
                "ad_combo": meta["ad_combo"],
                "campaign_id": campaign_id,
                "campaign_name": meta["campaign_name"],
                "ad_group_id": ad_group_id,
                "ad_group_name": meta["ad_group_name"],
                "current_bid": round(current_bid, 2) if current_bid is not None else None,
                "suggested_bid": suggested_bid,
                "asin_count": len(asins),
                "asins": asins,
                "placement_metrics": placement_metrics,
                "timeline": timeline,
            }
        )

    _emit_progress(progress_cb, "finalize", 94, "Finalizing context package")
    package = {
        "store_id": resolved_store_id,
        "store_name": store_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "days": (window.end_date - window.start_date).days + 1,
        },
        "granularity": "date_ad_group",
        "context_ready_for": "gemini_api",
        "ad_groups": ad_groups,
        "meta": {
            "data_sources": [
                "ad_group_performance",
                "query_word_reports",
                "campaign_placement_reports",
                "ad_group_product_links",
                "product_listings",
                "campaigns_base_data",
                "ad_group_base_data",
            ],
            "currency": "USD",
            "ad_group_count": len(ad_groups),
        },
    }

    cleaned = _remove_nulls(package)
    _emit_progress(progress_cb, "done", 100, "Context package build completed")
    return cleaned

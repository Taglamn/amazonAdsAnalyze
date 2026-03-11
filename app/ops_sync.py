from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .lingxing_client import LingxingClient, LingxingCredentials
from .lingxing_sync import _ensure_default_playbook
from .ops_db import ops_data_store
from .ops_logger import get_ops_logger


logger = get_ops_logger()


@dataclass
class SyncWindow:
    start_date: date
    end_date: date


def _parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def _date_iter(start_day: date, end_day: date) -> List[date]:
    result: List[date] = []
    current = start_day
    while current <= end_day:
        result.append(current)
        current += timedelta(days=1)
    return result


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        return float(match.group())


def _to_int(value: Any) -> int | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _pick_first_int(item: Dict[str, Any], keys: Sequence[str]) -> int | None:
    for key in keys:
        value = _to_int(item.get(key))
        if value is not None:
            return value
    return None


def _pick_first_float(item: Dict[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        value = _to_float(item.get(key))
        if value is not None:
            return value
    return None


def _pick_first_str(item: Dict[str, Any], keys: Sequence[str], fallback: str = "") -> str:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return fallback


def _normalize_product_type(campaign_name: str, ad_group_name: str) -> str:
    source = (ad_group_name or campaign_name or "").strip()
    if not source:
        return "unknown"
    source = source.lower()
    parts = re.split(r"[|/_\-]+", source)
    for part in parts:
        part = part.strip()
        if part:
            return part[:60]
    return source[:60]


def _resolve_target_seller(
    store_id: str,
    sellers: List[Dict[str, Any]],
) -> Tuple[int, str, str]:
    normalized = (store_id or "").strip()
    if not normalized:
        raise ValueError("store_id is required")

    if normalized.startswith("lingxing_"):
        sid_text = normalized.split("lingxing_", 1)[1]
        sid = int(sid_text)
        for seller in sellers:
            if int(seller.get("sid", 0) or 0) == sid:
                return sid, f"lingxing_{sid}", str(seller.get("name") or f"lingxing_{sid}")
        raise ValueError(f"Store not found in Lingxing sellers: {normalized}")

    for seller in sellers:
        name = str(seller.get("name") or "").strip()
        if name == normalized:
            sid = int(seller["sid"])
            return sid, f"lingxing_{sid}", name

    raise ValueError("store_id must be lingxing_<sid> or exact Lingxing seller name")


def _sha1_hash(payload: str) -> str:
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _chunk_date_ranges(start_day: date, end_day: date, max_span_days: int = 30) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    current = start_day
    while current <= end_day:
        chunk_end = min(current + timedelta(days=max_span_days), end_day)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _extract_change_events(
    store_id: str,
    log_row: Dict[str, Any],
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    sponsored_type = str(log_row.get("sponsored_type") or "").strip().lower()
    target_level = str(log_row.get("operate_type") or "").strip().lower()
    campaign_id = _pick_first_int(log_row, ["campaign_id", "campaignId", "campaign"])
    ad_group_id = _pick_first_int(log_row, ["ad_group_id", "adGroupId", "object_id"])
    ad_group_name = _pick_first_str(
        log_row,
        ["ad_group_name", "adGroupName", "object_name"],
        "UNKNOWN_AD_GROUP",
    )

    event_time = str(log_row.get("operate_time") or "").strip()
    if len(event_time) >= 10:
        event_date = event_time[:10]
    else:
        event_date = date.today().isoformat()

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

    raw_json = json.dumps(log_row, ensure_ascii=False, sort_keys=True)
    seen_negative = False
    for code in sorted(set(before.keys()) | set(after.keys())):
        old_value = before.get(code)
        new_value = after.get(code)
        if str(old_value) == str(new_value):
            continue

        code_upper = code.upper()
        change_type: Optional[str] = None
        keyword_text = ""

        if "BID" in code_upper or "CPC" in code_upper:
            if any(k in code_upper for k in ("PLACEMENT", "TOP", "PRODUCT_PAGE", "REST")):
                change_type = "placement_bid"
            else:
                change_type = "targeting_bid"
        elif "NEGATIVE" in code_upper:
            change_type = "negative_targeting"
            keyword_text = str(new_value or "")
            seen_negative = True

        if not change_type:
            continue

        event_key = "|".join(
            [
                store_id,
                event_time,
                event_date,
                sponsored_type,
                target_level,
                str(campaign_id or ""),
                str(ad_group_id or ""),
                change_type,
                code,
                str(old_value or ""),
                str(new_value or ""),
            ]
        )
        events.append(
            {
                "event_hash": _sha1_hash(event_key),
                "store_id": store_id,
                "date": event_date,
                "event_time": event_time,
                "sponsored_type": sponsored_type,
                "target_level": target_level,
                "campaign_id": campaign_id,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                "change_type": change_type,
                "field_code": code,
                "old_value": "" if old_value is None else str(old_value),
                "new_value": "" if new_value is None else str(new_value),
                "keyword_text": keyword_text,
                "raw_json": raw_json,
            }
        )

    raw_lower = raw_json.lower()
    if (not seen_negative) and target_level in {"keywords", "targets"} and "negative" in raw_lower:
        fallback_keyword = _pick_first_str(log_row, ["object_name", "keyword", "target"], "")
        event_key = "|".join(
            [
                store_id,
                event_time,
                event_date,
                sponsored_type,
                target_level,
                str(campaign_id or ""),
                str(ad_group_id or ""),
                "negative_targeting",
                fallback_keyword,
            ]
        )
        events.append(
            {
                "event_hash": _sha1_hash(event_key),
                "store_id": store_id,
                "date": event_date,
                "event_time": event_time,
                "sponsored_type": sponsored_type,
                "target_level": target_level,
                "campaign_id": campaign_id,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                "change_type": "negative_targeting",
                "field_code": "negative_fallback",
                "old_value": "",
                "new_value": fallback_keyword,
                "keyword_text": fallback_keyword,
                "raw_json": raw_json,
            }
        )

    return events


def _extract_snapshot_events(
    store_id: str,
    snapshot_date: str,
    snapshots: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for row in snapshots:
        sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
        campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
        ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId"])
        ad_group_name = _pick_first_str(
            row,
            ["ad_group", "ad_group_name", "adGroupName", "name"],
            "UNKNOWN_AD_GROUP",
        )
        current_bid = _to_float(row.get("current_bid"))
        if current_bid is None:
            continue

        event_time = f"{snapshot_date}T00:00:00"
        event_key = "|".join(
            [
                store_id,
                snapshot_date,
                sponsored_type,
                str(campaign_id or ""),
                str(ad_group_id or ""),
                "targeting_bid_snapshot",
                f"{current_bid:.4f}",
            ]
        )
        events.append(
            {
                "event_hash": _sha1_hash(event_key),
                "store_id": store_id,
                "date": snapshot_date,
                "event_time": event_time,
                "sponsored_type": sponsored_type,
                "target_level": "adgroup",
                "campaign_id": campaign_id,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
                "change_type": "targeting_bid_snapshot",
                "field_code": "snapshot_bid",
                "old_value": f"{current_bid:.4f}",
                "new_value": f"{current_bid:.4f}",
                "keyword_text": "",
                "raw_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
            }
        )
    return events


def _build_inventory_rows(
    store_id: str,
    snapshot_date: str,
    ad_product_links: List[Dict[str, Any]],
    product_listings: List[Dict[str, Any]],
    bid_snapshots: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    ad_group_asins: Dict[Tuple[str, int], Set[str]] = {}
    for item in ad_product_links:
        sponsored_type = str(item.get("sponsored_type") or "").strip().lower()
        ad_group_id = _pick_first_int(item, ["ad_group_id", "adGroupId", "adgroup_id"])
        asin = _pick_first_str(item, ["asin", "advertised_asin", "ad_asin", "same_asin"], "").upper()
        if not sponsored_type or ad_group_id is None or not asin:
            continue
        ad_group_asins.setdefault((sponsored_type, ad_group_id), set()).add(asin)

    asin_metrics: Dict[str, Dict[str, float]] = {}
    for row in product_listings:
        asin = _pick_first_str(row, ["asin", "amazon_asin", "market_asin", "listing_asin"], "").upper()
        if not asin:
            continue
        price = _pick_first_float(
            row,
            ["selling_price", "price", "sale_price", "listing_price", "current_price"],
        )
        stock = _pick_first_float(
            row,
            ["stock_level", "stock", "available_stock", "inventory", "sellable_qty"],
        )
        if asin not in asin_metrics:
            asin_metrics[asin] = {"price_sum": 0.0, "price_cnt": 0.0, "stock_sum": 0.0, "stock_cnt": 0.0}
        if price is not None:
            asin_metrics[asin]["price_sum"] += float(price)
            asin_metrics[asin]["price_cnt"] += 1
        if stock is not None:
            asin_metrics[asin]["stock_sum"] += float(stock)
            asin_metrics[asin]["stock_cnt"] += 1

    ad_group_names: Dict[Tuple[str, int], str] = {}
    for row in bid_snapshots:
        stype = str(row.get("sponsored_type") or "").strip().lower()
        gid = _pick_first_int(row, ["ad_group_id", "adGroupId"])
        gname = _pick_first_str(row, ["ad_group", "ad_group_name", "adGroupName", "name"], "")
        if stype and gid is not None and gname:
            ad_group_names[(stype, gid)] = gname

    inventory_rows: List[Dict[str, Any]] = []
    for (sponsored_type, ad_group_id), asins in ad_group_asins.items():
        prices: List[float] = []
        stocks: List[float] = []
        for asin in sorted(asins):
            metrics = asin_metrics.get(asin)
            if not metrics:
                continue
            if metrics["price_cnt"] > 0:
                prices.append(metrics["price_sum"] / metrics["price_cnt"])
            if metrics["stock_cnt"] > 0:
                stocks.append(metrics["stock_sum"] / metrics["stock_cnt"])
        avg_price = sum(prices) / len(prices) if prices else 0.0
        avg_stock = sum(stocks) / len(stocks) if stocks else 0.0
        inventory_rows.append(
            {
                "store_id": store_id,
                "snapshot_date": snapshot_date,
                "sponsored_type": sponsored_type,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_names.get((sponsored_type, ad_group_id), f"adgroup_{ad_group_id}"),
                "avg_price": round(avg_price, 4),
                "avg_stock": round(avg_stock, 4),
                "asin_count": len(asins),
            }
        )

    return inventory_rows


def incremental_sync_store(
    store_id: str,
    persist_csv: bool = True,
) -> Dict[str, Any]:
    logger.info("incremental_sync_start store_id=%s", store_id)

    credentials = LingxingCredentials.from_env()
    client = LingxingClient(credentials=credentials)
    access_token = client.generate_access_token()

    sellers = client.list_sellers(access_token=access_token)
    sellers = [
        row
        for row in sellers
        if int(row.get("status", 0) or 0) == 1 and int(row.get("has_ads_setting", 0) or 0) == 1
    ]
    sid, resolved_store_id, store_name = _resolve_target_seller(store_id, sellers)

    end_day = date.today() - timedelta(days=1)
    start_day = end_day - timedelta(days=364)
    window = SyncWindow(start_date=start_day, end_date=end_day)

    synced_dates = ops_data_store.get_synced_dates(
        store_id=resolved_store_id,
        start_date=window.start_date.isoformat(),
        end_date=window.end_date.isoformat(),
    )
    if not synced_dates:
        synced_dates = ops_data_store.get_existing_performance_dates(
            store_id=resolved_store_id,
            start_date=window.start_date.isoformat(),
            end_date=window.end_date.isoformat(),
        )
    missing_days = [
        day
        for day in _date_iter(window.start_date, window.end_date)
        if day.isoformat() not in synced_dates
    ]
    fetched_days: List[str] = []

    performance_rows: List[Dict[str, Any]] = []
    placement_rows: List[Dict[str, Any]] = []
    query_rows: List[Dict[str, Any]] = []

    for day in missing_days:
        day_text = day.isoformat()
        fetched_days.append(day_text)
        report_rows = client.fetch_ad_reports_for_day(
            access_token=access_token,
            sid=sid,
            report_date=day_text,
        )
        active_campaign_keys: Set[Tuple[str, int]] = set()
        for row in report_rows:
            sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
            campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
            ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId", "adgroup_id"])
            if not sponsored_type or campaign_id is None or ad_group_id is None:
                continue
            campaign_name = _pick_first_str(row, ["campaign_name", "campaignName", "campaign"], f"campaign_{campaign_id}")
            ad_group_name = _pick_first_str(row, ["ad_group_name", "adGroupName", "ad_group", "name"], f"adgroup_{ad_group_id}")
            spend = float(row.get("cost", row.get("spend", 0)) or 0)
            sales = float(
                row.get("sales", row.get("same_sales", row.get("attributed_sales", 0))) or 0
            )
            clicks = int(float(row.get("clicks", row.get("click", 0)) or 0))
            product_type = _normalize_product_type(campaign_name=campaign_name, ad_group_name=ad_group_name)
            performance_rows.append(
                {
                    "store_id": resolved_store_id,
                    "date": day_text,
                    "sponsored_type": sponsored_type,
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "ad_group_id": ad_group_id,
                    "ad_group_name": ad_group_name,
                    "product_type": product_type,
                    "clicks": clicks,
                    "spend": round(spend, 2),
                    "sales": round(sales, 2),
                }
            )
            active_campaign_keys.add((sponsored_type, campaign_id))

        day_query_rows = client.fetch_query_word_reports_for_day(
            access_token=access_token,
            sid=sid,
            report_date=day_text,
        )
        for row in day_query_rows:
            sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
            campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
            ad_group_id = _pick_first_int(row, ["ad_group_id", "adGroupId", "adgroup_id"])
            term = _pick_first_str(
                row,
                ["query_word", "queryWord", "search_term", "query", "customer_search_term"],
                "",
            )
            if not sponsored_type or campaign_id is None or ad_group_id is None or not term:
                continue
            query_rows.append(
                {
                    "store_id": resolved_store_id,
                    "date": day_text,
                    "sponsored_type": sponsored_type,
                    "campaign_id": campaign_id,
                    "ad_group_id": ad_group_id,
                    "search_term": term,
                    "clicks": int(float(row.get("clicks", row.get("click", 0)) or 0)),
                    "spend": float(row.get("cost", row.get("spend", 0)) or 0),
                    "sales": float(row.get("sales", row.get("same_sales", 0)) or 0),
                    "orders": int(float(row.get("orders", row.get("same_orders", 0)) or 0)),
                }
            )

        day_placement_rows = client.fetch_campaign_placement_reports_for_day(
            access_token=access_token,
            sid=sid,
            report_date=day_text,
        )
        for row in day_placement_rows:
            sponsored_type = str(row.get("sponsored_type") or "").strip().lower()
            campaign_id = _pick_first_int(row, ["campaign_id", "campaignId", "campaign"])
            if not sponsored_type or campaign_id is None:
                continue
            if (sponsored_type, campaign_id) not in active_campaign_keys:
                continue
            placement_type = _pick_first_str(
                row,
                ["placement_type", "placement", "placement_name", "placementName"],
                "UNKNOWN_PLACEMENT",
            )
            placement_rows.append(
                {
                    "store_id": resolved_store_id,
                    "date": day_text,
                    "sponsored_type": sponsored_type,
                    "campaign_id": campaign_id,
                    "placement_type": placement_type,
                    "top_of_search_is": _pick_first_float(
                        row,
                        [
                            "top_of_search_is",
                            "top_of_search_impression_share",
                            "top_of_search_is_percent",
                            "top_of_search_is_rate",
                        ],
                    ),
                    "clicks": int(float(row.get("clicks", row.get("click", 0)) or 0)),
                    "spend": float(row.get("cost", row.get("spend", 0)) or 0),
                    "sales": float(row.get("sales", row.get("same_sales", 0)) or 0),
                }
            )

    max_change_date = ops_data_store.get_max_change_date(resolved_store_id)
    change_start = window.start_date
    if max_change_date:
        change_start = max(change_start, _parse_date(max_change_date) + timedelta(days=1))

    operation_logs: List[Dict[str, Any]] = []
    if change_start <= window.end_date:
        for chunk_start, chunk_end in _chunk_date_ranges(change_start, window.end_date, max_span_days=30):
            logs = client.fetch_operation_logs(
                access_token=access_token,
                sid=sid,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
            )
            operation_logs.extend(logs)

    change_rows: List[Dict[str, Any]] = []
    for row in operation_logs:
        change_rows.extend(_extract_change_events(store_id=resolved_store_id, log_row=row))

    bid_snapshots = client.fetch_bid_snapshots(access_token=access_token, sid=sid)
    change_rows.extend(
        _extract_snapshot_events(
            store_id=resolved_store_id,
            snapshot_date=window.end_date.isoformat(),
            snapshots=bid_snapshots,
        )
    )

    ad_product_links = client.fetch_ad_group_product_links(access_token=access_token, sid=sid)
    all_asins = sorted(
        {
            _pick_first_str(item, ["asin", "advertised_asin", "ad_asin", "same_asin"], "").upper()
            for item in ad_product_links
            if _pick_first_str(item, ["asin", "advertised_asin", "ad_asin", "same_asin"], "")
        }
    )
    product_listings = client.fetch_product_listings(
        access_token=access_token,
        sid=sid,
        asins=all_asins if all_asins else None,
    )
    inventory_rows = _build_inventory_rows(
        store_id=resolved_store_id,
        snapshot_date=window.end_date.isoformat(),
        ad_product_links=ad_product_links,
        product_listings=product_listings,
        bid_snapshots=bid_snapshots,
    )

    inserted_perf = ops_data_store.upsert_performance_rows(performance_rows)
    inserted_place = ops_data_store.upsert_placement_rows(placement_rows)
    inserted_query = ops_data_store.upsert_query_term_rows(query_rows)
    inserted_change = ops_data_store.upsert_change_rows(change_rows)
    inserted_inventory = ops_data_store.upsert_inventory_rows(inventory_rows)
    synced_day_records = ops_data_store.upsert_synced_dates(
        store_id=resolved_store_id,
        dates=fetched_days,
        scope="daily",
    )
    ops_data_store.cleanup_old_rows(resolved_store_id, keep_days=400)

    csv_result: Dict[str, Any] = {}
    if persist_csv:
        csv_result = ops_data_store.export_store_csv(resolved_store_id)
        _ensure_default_playbook(store_id=resolved_store_id, store_name=store_name)

    summary = {
        "store_id": resolved_store_id,
        "store_name": store_name,
        "sid": sid,
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
        },
        "local_date_coverage": {
            "synced_days_before_sync": len(synced_dates),
            "missing_days_fetched": len(missing_days),
            "synced_day_records_upserted": synced_day_records,
        },
        "records_upserted": {
            "performance_rows": inserted_perf,
            "placement_rows": inserted_place,
            "query_term_rows": inserted_query,
            "change_rows": inserted_change,
            "inventory_rows": inserted_inventory,
        },
        "operation_logs_fetched": len(operation_logs),
        "persist_csv": persist_csv,
        "csv_result": csv_result,
    }
    logger.info("incremental_sync_done store_id=%s summary=%s", resolved_store_id, json.dumps(summary, ensure_ascii=False))
    return summary

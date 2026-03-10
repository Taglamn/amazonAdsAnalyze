from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from .analysis import build_bid_recommendations, build_optimization_cases
from .data_access import HISTORY_DIR, PERFORMANCE_DIR, PLAYBOOK_DIR
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
    start_day = parsed_start or (end_day - timedelta(days=13))

    if start_day > end_day:
        raise ValueError("start_date must be <= end_date")

    if (end_day - start_day).days > 30:
        raise ValueError("Lingxing operation-log API only allows up to 31 days range")

    return SyncWindow(start_date=start_day, end_date=end_day)


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

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group())


def _aggregate_daily_metrics(rows: List[Dict[str, Any]], target_date: date, store_id: str) -> Dict[str, Any]:
    clicks = sum(int(float(item.get("clicks", 0) or 0)) for item in rows)
    spend = sum(float(item.get("cost", 0) or 0) for item in rows)
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
        return

    default_playbook = {
        "store_id": store_id,
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

    store_results: List[Dict[str, Any]] = []

    for seller in valid_sellers:
        sid = int(seller["sid"])
        store_id = f"lingxing_{sid}"
        store_name = str(seller.get("name") or store_id)

        daily_rows: List[Dict[str, Any]] = []
        for day in _date_iter(window.start_date, window.end_date):
            report_rows = client.fetch_ad_reports_for_day(
                access_token=access_token,
                sid=sid,
                report_date=day.isoformat(),
            )
            daily_rows.append(_aggregate_daily_metrics(report_rows, day, store_id))

        op_logs = client.fetch_operation_logs(
            access_token=access_token,
            sid=sid,
            start_date=window.start_date.isoformat(),
            end_date=window.end_date.isoformat(),
        )
        bid_snapshots = client.fetch_bid_snapshots(access_token=access_token, sid=sid)

        history_rows = _build_store_history_rows(
            store_id=store_id,
            end_day=window.end_date,
            op_logs=op_logs,
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

        if persist:
            _persist_store_frames(store_id=store_id, perf_df=perf_df, history_df=history_df)
            _ensure_default_playbook(store_id=store_id, store_name=store_name)

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
                "latest_performance": latest_perf,
                "recommendations": recommendations,
                "optimization_cases": cases,
            }
        )

    return {
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
        },
        "stores_total": len(sellers),
        "stores_ads_enabled": len(valid_sellers),
        "stores_synced": len(store_results),
        "stores": store_results,
    }

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List


def parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def date_range(start_day: date, end_day: date) -> List[date]:
    days: List[date] = []
    cursor = start_day
    while cursor <= end_day:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def to_float(value: Any) -> float | None:
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


def safe_float(value: Any, default: float = 0.0) -> float:
    parsed = to_float(value)
    if parsed is None:
        return default
    return float(parsed)


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def action_from_change_pct(bid_change_pct: float) -> str:
    if bid_change_pct > 0:
        return "increase_bid"
    if bid_change_pct < 0:
        return "decrease_bid"
    return "keep_bid"


def summarize_window(rows: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    total_cost = 0.0
    total_sales = 0.0
    total_clicks = 0.0
    total_orders = 0.0
    total_impressions = 0.0
    for row in rows:
        total_cost += safe_float(row.get("cost"))
        total_sales += safe_float(row.get("sales"))
        total_clicks += safe_float(row.get("clicks"))
        total_orders += safe_float(row.get("orders"))
        total_impressions += safe_float(row.get("impressions"))
    acos = safe_ratio(total_cost, total_sales) * 100 if total_sales > 0 else 0.0
    cvr = safe_ratio(total_orders, total_clicks) if total_clicks > 0 else 0.0
    return {
        "impressions": round(total_impressions, 2),
        "clicks": round(total_clicks, 2),
        "cost": round(total_cost, 4),
        "sales": round(total_sales, 4),
        "orders": round(total_orders, 2),
        "acos": round(acos, 4),
        "cvr": round(cvr, 6),
    }

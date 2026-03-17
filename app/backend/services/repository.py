from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from ...data_access import DATA_DIR, HISTORY_DIR, PERFORMANCE_DIR
from ...ops_db import ops_data_store
from ..utils import action_from_change_pct, safe_float, to_float


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_iso_day(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10:
        return text[:10]
    return text


class AdsRuleEngineRepository:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        default_path = DATA_DIR / "ops_data.db"
        raw_path = os.getenv("ADS_RULE_ENGINE_DB_PATH", "").strip()
        self.db_path = Path(raw_path or str(db_path or default_path))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS strategy_config (
                    store_id TEXT NOT NULL,
                    ad_group_id INTEGER NOT NULL,
                    target_acos REAL NOT NULL,
                    upper_acos REAL NOT NULL,
                    lower_acos REAL NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, ad_group_id)
                );

                CREATE TABLE IF NOT EXISTS bid_changes (
                    change_id TEXT PRIMARY KEY,
                    store_id TEXT NOT NULL,
                    ad_group_id INTEGER NOT NULL,
                    keyword_id TEXT NOT NULL,
                    old_bid REAL NOT NULL,
                    new_bid REAL NOT NULL,
                    bid_change_pct REAL NOT NULL,
                    action_type TEXT NOT NULL,
                    change_time TEXT NOT NULL,
                    source_event_hash TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_bid_changes_store_time
                ON bid_changes (store_id, change_time);

                CREATE TABLE IF NOT EXISTS processed_samples (
                    sample_id TEXT PRIMARY KEY,
                    store_id TEXT NOT NULL,
                    ad_group_id INTEGER NOT NULL,
                    keyword_id TEXT NOT NULL,
                    change_time TEXT NOT NULL,
                    action TEXT NOT NULL,
                    bid_change_pct REAL NOT NULL,
                    before_acos REAL NOT NULL,
                    after_acos REAL NOT NULL,
                    before_clicks REAL NOT NULL,
                    after_clicks REAL NOT NULL,
                    before_cvr REAL NOT NULL,
                    after_cvr REAL NOT NULL,
                    performance_change TEXT NOT NULL,
                    target_acos REAL NOT NULL,
                    upper_acos REAL NOT NULL,
                    lower_acos REAL NOT NULL,
                    acos_level TEXT NOT NULL,
                    traffic_level TEXT NOT NULL,
                    conversion_level TEXT NOT NULL,
                    feature_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_samples_store_time
                ON processed_samples (store_id, change_time);

                CREATE TABLE IF NOT EXISTS rules (
                    rule_id TEXT PRIMARY KEY,
                    store_id TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    condition_json TEXT NOT NULL,
                    action_json TEXT NOT NULL,
                    lingxing_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    win_rate REAL NOT NULL,
                    sample_size INTEGER NOT NULL,
                    bid_adjustment_pct REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_rules_store_status
                ON rules (store_id, status, updated_at DESC);

                CREATE TABLE IF NOT EXISTS rule_results (
                    result_id TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    store_id TEXT NOT NULL,
                    before_acos REAL NOT NULL,
                    after_acos REAL NOT NULL,
                    before_clicks REAL NOT NULL,
                    after_clicks REAL NOT NULL,
                    before_cvr REAL NOT NULL,
                    after_cvr REAL NOT NULL,
                    effect TEXT NOT NULL,
                    suggestion TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    evaluated_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_rule_results_store_time
                ON rule_results (store_id, evaluated_at DESC);
                """
            )

    @staticmethod
    def _hash_key(*parts: str) -> str:
        joined = "|".join(parts)
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()

    def upsert_strategy_configs(
        self,
        store_id: str,
        default_config: Dict[str, float],
        overrides: Sequence[Dict[str, Any]],
    ) -> None:
        now_iso = _utc_now_iso()
        payload: List[tuple[Any, ...]] = [
            (
                store_id,
                -1,
                float(default_config["target_acos"]),
                float(default_config["upper_acos"]),
                float(default_config["lower_acos"]),
                now_iso,
            )
        ]
        for row in overrides:
            payload.append(
                (
                    store_id,
                    _to_int(row.get("ad_group_id"), -1),
                    float(row.get("target_acos", default_config["target_acos"])),
                    float(row.get("upper_acos", default_config["upper_acos"])),
                    float(row.get("lower_acos", default_config["lower_acos"])),
                    now_iso,
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO strategy_config (
                    store_id, ad_group_id, target_acos, upper_acos, lower_acos, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_id, ad_group_id)
                DO UPDATE SET
                    target_acos=excluded.target_acos,
                    upper_acos=excluded.upper_acos,
                    lower_acos=excluded.lower_acos,
                    updated_at=excluded.updated_at
                """,
                payload,
            )

    def load_strategy_map(self, store_id: str) -> Dict[int, Dict[str, float]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ad_group_id, target_acos, upper_acos, lower_acos
                FROM strategy_config
                WHERE store_id = ?
                """,
                (store_id,),
            ).fetchall()
        result: Dict[int, Dict[str, float]] = {}
        for row in rows:
            result[int(row["ad_group_id"])] = {
                "target_acos": float(row["target_acos"]),
                "upper_acos": float(row["upper_acos"]),
                "lower_acos": float(row["lower_acos"]),
            }
        return result

    def _bootstrap_bid_changes_from_ops_data(
        self,
        store_id: str,
        start_date: str,
        end_date: str,
    ) -> int:
        change_df = ops_data_store.load_change_df(store_id, start_date=start_date, end_date=end_date)
        if change_df.empty:
            return 0
        candidates = change_df[
            change_df["change_type"].astype(str).str.contains("bid", case=False, na=False)
        ].copy()
        if candidates.empty:
            return 0

        rows: List[Dict[str, Any]] = []
        for _, row in candidates.iterrows():
            old_bid = to_float(row.get("old_value"))
            new_bid = to_float(row.get("new_value"))
            if old_bid is None or new_bid is None:
                continue
            change_time = str(row.get("event_time") or row.get("date") or "").strip()
            if not change_time:
                continue
            ad_group_id = _to_int(row.get("ad_group_id"), -1)
            if ad_group_id <= 0:
                continue
            keyword_text = str(row.get("keyword_text") or "").strip()
            keyword_id = keyword_text if keyword_text else "__all__"
            bid_change_pct = ((new_bid - old_bid) / old_bid * 100) if old_bid != 0 else 0.0
            source_event_hash = str(row.get("event_hash") or "")
            rows.append(
                {
                    "store_id": store_id,
                    "ad_group_id": ad_group_id,
                    "keyword_id": keyword_id,
                    "old_bid": old_bid,
                    "new_bid": new_bid,
                    "bid_change_pct": bid_change_pct,
                    "action_type": action_from_change_pct(bid_change_pct),
                    "change_time": change_time,
                    "source_event_hash": source_event_hash,
                }
            )
        return self.upsert_bid_changes(rows)

    def _bootstrap_bid_changes_from_csv(self, store_id: str) -> int:
        history_path = HISTORY_DIR / f"{store_id}.csv"
        if not history_path.exists():
            return 0
        history_df = pd.read_csv(history_path)
        if history_df.empty:
            return 0

        rows: List[Dict[str, Any]] = []
        for _, row in history_df.iterrows():
            old_bid = to_float(row.get("old_bid", row.get("old_value")))
            new_bid = to_float(row.get("new_bid", row.get("new_value")))
            if old_bid is None or new_bid is None:
                continue
            ad_group_raw = str(row.get("ad_group") or row.get("ad_group_name") or "").strip()
            ad_group_id = _to_int(row.get("ad_group_id"), -1)
            if ad_group_id <= 0 and ad_group_raw.startswith("adgroup_"):
                ad_group_id = _to_int(ad_group_raw.split("adgroup_", 1)[1], -1)
            if ad_group_id <= 0:
                continue
            day = _to_iso_day(row.get("date"))
            if not day:
                continue
            bid_change_pct = ((new_bid - old_bid) / old_bid * 100) if old_bid != 0 else 0.0
            rows.append(
                {
                    "store_id": store_id,
                    "ad_group_id": ad_group_id,
                    "keyword_id": "__all__",
                    "old_bid": old_bid,
                    "new_bid": new_bid,
                    "bid_change_pct": bid_change_pct,
                    "action_type": action_from_change_pct(bid_change_pct),
                    "change_time": f"{day}T00:00:00",
                    "source_event_hash": "",
                }
            )
        return self.upsert_bid_changes(rows)

    def ensure_bid_changes(
        self,
        store_id: str,
        start_date: str,
        end_date: str,
    ) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(1) AS cnt
                FROM bid_changes
                WHERE store_id = ? AND substr(change_time, 1, 10) BETWEEN ? AND ?
                """,
                (store_id, start_date, end_date),
            ).fetchone()
        if row and int(row["cnt"]) > 0:
            return 0
        inserted = self._bootstrap_bid_changes_from_ops_data(
            store_id=store_id,
            start_date=start_date,
            end_date=end_date,
        )
        if inserted > 0:
            return inserted
        return self._bootstrap_bid_changes_from_csv(store_id=store_id)

    def upsert_bid_changes(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload: List[tuple[Any, ...]] = []
        now_iso = _utc_now_iso()
        for row in rows:
            change_time = str(row.get("change_time") or "").strip()
            if not change_time:
                continue
            store_id = str(row.get("store_id") or "").strip()
            ad_group_id = _to_int(row.get("ad_group_id"), -1)
            keyword_id = str(row.get("keyword_id") or "__all__")
            old_bid = safe_float(row.get("old_bid"))
            new_bid = safe_float(row.get("new_bid"))
            change_id = self._hash_key(store_id, str(ad_group_id), keyword_id, change_time, f"{new_bid:.6f}")
            payload.append(
                (
                    change_id,
                    store_id,
                    ad_group_id,
                    keyword_id,
                    old_bid,
                    new_bid,
                    safe_float(row.get("bid_change_pct")),
                    str(row.get("action_type") or action_from_change_pct(safe_float(row.get("bid_change_pct")))),
                    change_time,
                    str(row.get("source_event_hash") or ""),
                    now_iso,
                )
            )
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO bid_changes (
                    change_id, store_id, ad_group_id, keyword_id, old_bid, new_bid,
                    bid_change_pct, action_type, change_time, source_event_hash, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (change_id) DO UPDATE SET
                    old_bid=excluded.old_bid,
                    new_bid=excluded.new_bid,
                    bid_change_pct=excluded.bid_change_pct,
                    action_type=excluded.action_type
                """,
                payload,
            )
        return len(payload)

    def load_bid_changes_df(self, store_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        self.ensure_bid_changes(store_id=store_id, start_date=start_date, end_date=end_date)
        with self._connect() as conn:
            return pd.read_sql_query(
                """
                SELECT store_id, ad_group_id, keyword_id, old_bid, new_bid, bid_change_pct, action_type, change_time
                FROM bid_changes
                WHERE store_id = ? AND substr(change_time, 1, 10) BETWEEN ? AND ?
                ORDER BY change_time ASC
                """,
                conn,
                params=(store_id, start_date, end_date),
            )

    def load_performance_df(self, store_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        perf_df = ops_data_store.load_performance_df(store_id, start_date=start_date, end_date=end_date)
        if perf_df.empty:
            perf_path = PERFORMANCE_DIR / f"{store_id}.csv"
            if not perf_path.exists():
                return pd.DataFrame(
                    columns=[
                        "store_id",
                        "ad_group_id",
                        "keyword_id",
                        "date",
                        "impressions",
                        "clicks",
                        "cost",
                        "sales",
                        "orders",
                        "acos",
                        "cvr",
                    ]
                )
            raw_df = pd.read_csv(perf_path)
            if raw_df.empty:
                return raw_df
            raw_df["date"] = raw_df["date"].astype(str)
            raw_df = raw_df[(raw_df["date"] >= start_date) & (raw_df["date"] <= end_date)].copy()
            if raw_df.empty:
                return raw_df
            raw_df["ad_group_id"] = -1
            raw_df["keyword_id"] = "__all__"
            raw_df["impressions"] = 0
            raw_df["cost"] = pd.to_numeric(raw_df.get("spend"), errors="coerce").fillna(0.0)
            raw_df["orders"] = 0
            raw_df["cvr"] = 0.0
            raw_df["acos"] = pd.to_numeric(raw_df.get("acos"), errors="coerce").fillna(0.0)
            raw_df["clicks"] = pd.to_numeric(raw_df.get("clicks"), errors="coerce").fillna(0.0)
            raw_df["sales"] = pd.to_numeric(raw_df.get("sales"), errors="coerce").fillna(0.0)
            raw_df["store_id"] = store_id
            return raw_df[
                [
                    "store_id",
                    "ad_group_id",
                    "keyword_id",
                    "date",
                    "impressions",
                    "clicks",
                    "cost",
                    "sales",
                    "orders",
                    "acos",
                    "cvr",
                ]
            ]

        perf_df["date"] = perf_df["date"].astype(str)
        perf_df["ad_group_id"] = pd.to_numeric(perf_df["ad_group_id"], errors="coerce").fillna(-1).astype(int)
        perf_df["clicks"] = pd.to_numeric(perf_df["clicks"], errors="coerce").fillna(0.0)
        perf_df["spend"] = pd.to_numeric(perf_df["spend"], errors="coerce").fillna(0.0)
        perf_df["sales"] = pd.to_numeric(perf_df["sales"], errors="coerce").fillna(0.0)

        query_df = ops_data_store.load_query_terms_df(store_id, start_date=start_date, end_date=end_date)
        if query_df.empty:
            orders_by_day = pd.DataFrame(columns=["date", "ad_group_id", "orders"])
        else:
            query_df["date"] = query_df["date"].astype(str)
            query_df["ad_group_id"] = pd.to_numeric(query_df["ad_group_id"], errors="coerce").fillna(-1).astype(int)
            query_df["orders"] = pd.to_numeric(query_df["orders"], errors="coerce").fillna(0.0)
            orders_by_day = (
                query_df.groupby(["date", "ad_group_id"], as_index=False)
                .agg(orders=("orders", "sum"))
            )

        merged = perf_df.merge(orders_by_day, on=["date", "ad_group_id"], how="left")
        merged["orders"] = merged["orders"].fillna(0.0)
        merged["cvr"] = merged.apply(
            lambda r: (float(r["orders"]) / float(r["clicks"])) if float(r["clicks"]) > 0 else 0.0,
            axis=1,
        )
        merged["acos"] = merged.apply(
            lambda r: (float(r["spend"]) / float(r["sales"]) * 100.0) if float(r["sales"]) > 0 else 0.0,
            axis=1,
        )
        merged["keyword_id"] = "__all__"
        merged["impressions"] = 0
        merged = merged.rename(columns={"spend": "cost"})
        merged["store_id"] = store_id
        return merged[
            [
                "store_id",
                "ad_group_id",
                "keyword_id",
                "date",
                "impressions",
                "clicks",
                "cost",
                "sales",
                "orders",
                "acos",
                "cvr",
            ]
        ]

    def upsert_processed_samples(self, rows: Iterable[Dict[str, Any]]) -> int:
        now_iso = _utc_now_iso()
        payload: List[tuple[Any, ...]] = []
        for row in rows:
            store_id = str(row["store_id"])
            ad_group_id = _to_int(row["ad_group_id"], -1)
            keyword_id = str(row.get("keyword_id") or "__all__")
            change_time = str(row["change_time"])
            action = str(row["action"])
            sample_id = self._hash_key(store_id, str(ad_group_id), keyword_id, change_time, action)
            feature_json = json.dumps(
                {
                    "acos_level": row.get("acos_level"),
                    "traffic_level": row.get("traffic_level"),
                    "conversion_level": row.get("conversion_level"),
                },
                ensure_ascii=False,
            )
            payload.append(
                (
                    sample_id,
                    store_id,
                    ad_group_id,
                    keyword_id,
                    change_time,
                    action,
                    safe_float(row.get("bid_change_pct")),
                    safe_float(row.get("before_acos")),
                    safe_float(row.get("after_acos")),
                    safe_float(row.get("before_clicks")),
                    safe_float(row.get("after_clicks")),
                    safe_float(row.get("before_cvr")),
                    safe_float(row.get("after_cvr")),
                    str(row.get("performance_change") or "neutral"),
                    safe_float(row.get("target_acos")),
                    safe_float(row.get("upper_acos")),
                    safe_float(row.get("lower_acos")),
                    str(row.get("acos_level") or "normal"),
                    str(row.get("traffic_level") or "low_clicks"),
                    str(row.get("conversion_level") or "low_cvr"),
                    feature_json,
                    now_iso,
                )
            )
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO processed_samples (
                    sample_id, store_id, ad_group_id, keyword_id, change_time, action,
                    bid_change_pct, before_acos, after_acos, before_clicks, after_clicks,
                    before_cvr, after_cvr, performance_change, target_acos, upper_acos, lower_acos,
                    acos_level, traffic_level, conversion_level, feature_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (sample_id)
                DO UPDATE SET
                    bid_change_pct=excluded.bid_change_pct,
                    before_acos=excluded.before_acos,
                    after_acos=excluded.after_acos,
                    before_clicks=excluded.before_clicks,
                    after_clicks=excluded.after_clicks,
                    before_cvr=excluded.before_cvr,
                    after_cvr=excluded.after_cvr,
                    performance_change=excluded.performance_change,
                    target_acos=excluded.target_acos,
                    upper_acos=excluded.upper_acos,
                    lower_acos=excluded.lower_acos,
                    acos_level=excluded.acos_level,
                    traffic_level=excluded.traffic_level,
                    conversion_level=excluded.conversion_level,
                    feature_json=excluded.feature_json,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def load_processed_samples_df(self, store_id: str, limit: int = 50000) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                """
                SELECT *
                FROM processed_samples
                WHERE store_id = ?
                ORDER BY change_time DESC
                LIMIT ?
                """,
                conn,
                params=(store_id, int(limit)),
            )

    def save_rules(self, rows: Iterable[Dict[str, Any]]) -> int:
        now_iso = _utc_now_iso()
        payload: List[tuple[Any, ...]] = []
        for row in rows:
            store_id = str(row["store_id"])
            condition_json = json.dumps(row.get("condition", {}), ensure_ascii=False)
            action_json = json.dumps(row.get("action", {}), ensure_ascii=False)
            lingxing_json = json.dumps(row.get("lingxing_rule", {}), ensure_ascii=False)
            raw_rule_id = str(row.get("rule_id") or "").strip()
            rule_id = raw_rule_id or self._hash_key(
                store_id,
                str(row.get("rule_name") or ""),
                str(row.get("source") or ""),
                condition_json,
                action_json,
            )
            payload.append(
                (
                    rule_id,
                    store_id,
                    str(row.get("rule_name") or rule_id),
                    condition_json,
                    action_json,
                    lingxing_json,
                    str(row.get("source") or "learned"),
                    str(row.get("status") or "active"),
                    safe_float(row.get("confidence"), 0.0),
                    safe_float(row.get("win_rate"), 0.0),
                    _to_int(row.get("sample_size"), 0),
                    safe_float(row.get("bid_adjustment_pct"), 0.0),
                    now_iso,
                    now_iso,
                )
            )
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO rules (
                    rule_id, store_id, rule_name, condition_json, action_json, lingxing_json,
                    source, status, confidence, win_rate, sample_size, bid_adjustment_pct,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (rule_id)
                DO UPDATE SET
                    rule_name=excluded.rule_name,
                    condition_json=excluded.condition_json,
                    action_json=excluded.action_json,
                    lingxing_json=excluded.lingxing_json,
                    source=excluded.source,
                    status=excluded.status,
                    confidence=excluded.confidence,
                    win_rate=excluded.win_rate,
                    sample_size=excluded.sample_size,
                    bid_adjustment_pct=excluded.bid_adjustment_pct,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def list_rules(
        self,
        store_id: str,
        active_only: bool = True,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        where = "store_id = ?"
        params: List[Any] = [store_id]
        if active_only:
            where += " AND status = 'active'"
        params.append(int(limit))
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM rules
                WHERE {where}
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "rule_id": str(row["rule_id"]),
                    "store_id": str(row["store_id"]),
                    "rule_name": str(row["rule_name"]),
                    "condition": json.loads(row["condition_json"] or "{}"),
                    "action": json.loads(row["action_json"] or "{}"),
                    "lingxing_rule": json.loads(row["lingxing_json"] or "{}"),
                    "source": str(row["source"]),
                    "status": str(row["status"]),
                    "confidence": float(row["confidence"]),
                    "win_rate": float(row["win_rate"]),
                    "sample_size": int(row["sample_size"]),
                    "bid_adjustment_pct": float(row["bid_adjustment_pct"]),
                    "updated_at": str(row["updated_at"]),
                }
            )
        return result

    def save_rule_results(self, rows: Iterable[Dict[str, Any]]) -> int:
        now_iso = _utc_now_iso()
        payload: List[tuple[Any, ...]] = []
        for row in rows:
            result_id = self._hash_key(
                str(row["store_id"]),
                str(row["rule_id"]),
                str(row.get("evaluated_at") or now_iso),
                str(row.get("effect") or ""),
            )
            payload.append(
                (
                    result_id,
                    str(row["rule_id"]),
                    str(row["store_id"]),
                    safe_float(row.get("before_acos")),
                    safe_float(row.get("after_acos")),
                    safe_float(row.get("before_clicks")),
                    safe_float(row.get("after_clicks")),
                    safe_float(row.get("before_cvr")),
                    safe_float(row.get("after_cvr")),
                    str(row.get("effect") or "neutral"),
                    str(row.get("suggestion") or ""),
                    json.dumps(row.get("detail", {}), ensure_ascii=False),
                    str(row.get("evaluated_at") or now_iso),
                    now_iso,
                )
            )
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO rule_results (
                    result_id, rule_id, store_id, before_acos, after_acos, before_clicks, after_clicks,
                    before_cvr, after_cvr, effect, suggestion, detail_json, evaluated_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (result_id) DO NOTHING
                """,
                payload,
            )
        return len(payload)

    def patch_rule(
        self,
        rule_id: str,
        action_patch: Dict[str, Any] | None = None,
        condition_patch: Dict[str, Any] | None = None,
        status: str | None = None,
    ) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT action_json, condition_json, status FROM rules WHERE rule_id = ?",
                (rule_id,),
            ).fetchone()
            if not row:
                return
            action_obj = json.loads(row["action_json"] or "{}")
            condition_obj = json.loads(row["condition_json"] or "{}")
            if action_patch:
                action_obj.update(action_patch)
            if condition_patch:
                condition_obj.update(condition_patch)
            next_status = status or str(row["status"])
            conn.execute(
                """
                UPDATE rules
                SET action_json = ?, condition_json = ?, status = ?, updated_at = ?
                WHERE rule_id = ?
                """,
                (
                    json.dumps(action_obj, ensure_ascii=False),
                    json.dumps(condition_obj, ensure_ascii=False),
                    next_status,
                    _utc_now_iso(),
                    rule_id,
                ),
            )

    def get_recent_rule_results(
        self,
        store_id: str,
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        end_day = date.today()
        start_day = end_day - timedelta(days=max(1, days))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT rule_id, effect, suggestion, evaluated_at
                FROM rule_results
                WHERE store_id = ? AND substr(evaluated_at, 1, 10) BETWEEN ? AND ?
                ORDER BY evaluated_at DESC
                """,
                (store_id, start_day.isoformat(), end_day.isoformat()),
            ).fetchall()
        return [dict(item) for item in rows]


rule_engine_repo = AdsRuleEngineRepository()

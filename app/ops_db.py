from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

from .data_access import DATA_DIR, HISTORY_DIR, PERFORMANCE_DIR


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _float_or_zero(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


class OpsDataStore:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        default_path = DATA_DIR / "ops_data.db"
        self.db_path = Path(os.getenv("OPS_DB_PATH", str(db_path or default_path)))
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
                CREATE TABLE IF NOT EXISTS performance_daily (
                    store_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    sponsored_type TEXT NOT NULL,
                    campaign_id INTEGER NOT NULL,
                    campaign_name TEXT,
                    ad_group_id INTEGER NOT NULL,
                    ad_group_name TEXT,
                    product_type TEXT,
                    clicks INTEGER NOT NULL,
                    spend REAL NOT NULL,
                    sales REAL NOT NULL,
                    acos REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, date, sponsored_type, campaign_id, ad_group_id)
                );

                CREATE INDEX IF NOT EXISTS idx_perf_store_date
                ON performance_daily (store_id, date);

                CREATE TABLE IF NOT EXISTS change_history (
                    event_hash TEXT PRIMARY KEY,
                    store_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    event_time TEXT,
                    sponsored_type TEXT,
                    target_level TEXT,
                    campaign_id INTEGER,
                    ad_group_id INTEGER,
                    ad_group_name TEXT,
                    change_type TEXT NOT NULL,
                    field_code TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    keyword_text TEXT,
                    raw_json TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_change_store_date
                ON change_history (store_id, date);

                CREATE INDEX IF NOT EXISTS idx_change_store_type
                ON change_history (store_id, change_type);

                CREATE TABLE IF NOT EXISTS placement_daily (
                    store_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    sponsored_type TEXT NOT NULL,
                    campaign_id INTEGER NOT NULL,
                    placement_type TEXT NOT NULL,
                    top_of_search_is REAL,
                    clicks INTEGER NOT NULL,
                    spend REAL NOT NULL,
                    sales REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, date, sponsored_type, campaign_id, placement_type)
                );

                CREATE INDEX IF NOT EXISTS idx_place_store_date
                ON placement_daily (store_id, date);

                CREATE TABLE IF NOT EXISTS query_term_daily (
                    store_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    sponsored_type TEXT NOT NULL,
                    campaign_id INTEGER NOT NULL,
                    ad_group_id INTEGER NOT NULL,
                    search_term TEXT NOT NULL,
                    clicks INTEGER NOT NULL,
                    spend REAL NOT NULL,
                    sales REAL NOT NULL,
                    orders INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, date, sponsored_type, campaign_id, ad_group_id, search_term)
                );

                CREATE INDEX IF NOT EXISTS idx_query_store_date
                ON query_term_daily (store_id, date);

                CREATE TABLE IF NOT EXISTS inventory_snapshot (
                    store_id TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    sponsored_type TEXT NOT NULL,
                    ad_group_id INTEGER NOT NULL,
                    ad_group_name TEXT,
                    avg_price REAL NOT NULL,
                    avg_stock REAL NOT NULL,
                    asin_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, snapshot_date, sponsored_type, ad_group_id)
                );

                CREATE INDEX IF NOT EXISTS idx_inventory_store_date
                ON inventory_snapshot (store_id, snapshot_date);

                CREATE TABLE IF NOT EXISTS sync_coverage (
                    store_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    synced_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, date, scope)
                );

                CREATE INDEX IF NOT EXISTS idx_sync_store_date
                ON sync_coverage (store_id, date);
                """
            )

    def get_existing_performance_dates(
        self,
        store_id: str,
        start_date: str,
        end_date: str,
    ) -> Set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT date
                FROM performance_daily
                WHERE store_id = ?
                  AND date BETWEEN ? AND ?
                """,
                (store_id, start_date, end_date),
            ).fetchall()
        return {str(row["date"]) for row in rows}

    def get_synced_dates(
        self,
        store_id: str,
        start_date: str,
        end_date: str,
        scope: str = "daily",
    ) -> Set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT date
                FROM sync_coverage
                WHERE store_id = ?
                  AND scope = ?
                  AND date BETWEEN ? AND ?
                """,
                (store_id, scope, start_date, end_date),
            ).fetchall()
        return {str(row["date"]) for row in rows}

    def upsert_synced_dates(
        self,
        store_id: str,
        dates: Sequence[str],
        scope: str = "daily",
    ) -> int:
        if not dates:
            return 0
        now_iso = _utc_now_iso()
        payload = [
            (
                str(store_id),
                str(day),
                str(scope),
                now_iso,
            )
            for day in dates
        ]
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO sync_coverage (store_id, date, scope, synced_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (store_id, date, scope)
                DO UPDATE SET synced_at=excluded.synced_at
                """,
                payload,
            )
        return len(payload)

    def get_max_change_date(self, store_id: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(date) AS max_date FROM change_history WHERE store_id = ?",
                (store_id,),
            ).fetchone()
        if not row:
            return None
        return str(row["max_date"]) if row["max_date"] else None

    def get_max_performance_date(self, store_id: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(date) AS max_date FROM performance_daily WHERE store_id = ?",
                (store_id,),
            ).fetchone()
        if not row:
            return None
        return str(row["max_date"]) if row["max_date"] else None

    def upsert_performance_rows(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        now_iso = _utc_now_iso()
        payload: List[Tuple[Any, ...]] = []
        for row in rows:
            spend = _float_or_zero(row.get("spend"))
            sales = _float_or_zero(row.get("sales"))
            acos = (spend / sales) * 100 if sales > 0 else 0.0
            payload.append(
                (
                    str(row["store_id"]),
                    str(row["date"]),
                    str(row["sponsored_type"]),
                    _int_or_zero(row["campaign_id"]),
                    str(row.get("campaign_name") or ""),
                    _int_or_zero(row["ad_group_id"]),
                    str(row.get("ad_group_name") or ""),
                    str(row.get("product_type") or "unknown"),
                    _int_or_zero(row.get("clicks")),
                    round(spend, 2),
                    round(sales, 2),
                    round(acos, 2),
                    now_iso,
                    now_iso,
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO performance_daily (
                    store_id, date, sponsored_type, campaign_id, campaign_name,
                    ad_group_id, ad_group_name, product_type,
                    clicks, spend, sales, acos, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_id, date, sponsored_type, campaign_id, ad_group_id)
                DO UPDATE SET
                    campaign_name=excluded.campaign_name,
                    ad_group_name=excluded.ad_group_name,
                    product_type=excluded.product_type,
                    clicks=excluded.clicks,
                    spend=excluded.spend,
                    sales=excluded.sales,
                    acos=excluded.acos,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def upsert_change_rows(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        now_iso = _utc_now_iso()
        payload: List[Tuple[Any, ...]] = []
        for row in rows:
            payload.append(
                (
                    str(row["event_hash"]),
                    str(row["store_id"]),
                    str(row["date"]),
                    str(row.get("event_time") or ""),
                    str(row.get("sponsored_type") or ""),
                    str(row.get("target_level") or ""),
                    row.get("campaign_id"),
                    row.get("ad_group_id"),
                    str(row.get("ad_group_name") or ""),
                    str(row.get("change_type") or ""),
                    str(row.get("field_code") or ""),
                    str(row.get("old_value") or ""),
                    str(row.get("new_value") or ""),
                    str(row.get("keyword_text") or ""),
                    str(row.get("raw_json") or ""),
                    now_iso,
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO change_history (
                    event_hash, store_id, date, event_time, sponsored_type, target_level,
                    campaign_id, ad_group_id, ad_group_name, change_type, field_code,
                    old_value, new_value, keyword_text, raw_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_hash) DO NOTHING
                """,
                payload,
            )
        return len(payload)

    def upsert_placement_rows(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        now_iso = _utc_now_iso()
        payload: List[Tuple[Any, ...]] = []
        for row in rows:
            payload.append(
                (
                    str(row["store_id"]),
                    str(row["date"]),
                    str(row["sponsored_type"]),
                    _int_or_zero(row["campaign_id"]),
                    str(row["placement_type"]),
                    row.get("top_of_search_is"),
                    _int_or_zero(row.get("clicks")),
                    round(_float_or_zero(row.get("spend")), 2),
                    round(_float_or_zero(row.get("sales")), 2),
                    now_iso,
                    now_iso,
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO placement_daily (
                    store_id, date, sponsored_type, campaign_id, placement_type, top_of_search_is,
                    clicks, spend, sales, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_id, date, sponsored_type, campaign_id, placement_type)
                DO UPDATE SET
                    top_of_search_is=excluded.top_of_search_is,
                    clicks=excluded.clicks,
                    spend=excluded.spend,
                    sales=excluded.sales,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def upsert_query_term_rows(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        now_iso = _utc_now_iso()
        payload: List[Tuple[Any, ...]] = []
        for row in rows:
            payload.append(
                (
                    str(row["store_id"]),
                    str(row["date"]),
                    str(row["sponsored_type"]),
                    _int_or_zero(row["campaign_id"]),
                    _int_or_zero(row["ad_group_id"]),
                    str(row["search_term"]),
                    _int_or_zero(row.get("clicks")),
                    round(_float_or_zero(row.get("spend")), 2),
                    round(_float_or_zero(row.get("sales")), 2),
                    _int_or_zero(row.get("orders")),
                    now_iso,
                    now_iso,
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO query_term_daily (
                    store_id, date, sponsored_type, campaign_id, ad_group_id, search_term,
                    clicks, spend, sales, orders, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_id, date, sponsored_type, campaign_id, ad_group_id, search_term)
                DO UPDATE SET
                    clicks=excluded.clicks,
                    spend=excluded.spend,
                    sales=excluded.sales,
                    orders=excluded.orders,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def upsert_inventory_rows(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        now_iso = _utc_now_iso()
        payload: List[Tuple[Any, ...]] = []
        for row in rows:
            payload.append(
                (
                    str(row["store_id"]),
                    str(row["snapshot_date"]),
                    str(row["sponsored_type"]),
                    _int_or_zero(row["ad_group_id"]),
                    str(row.get("ad_group_name") or ""),
                    round(_float_or_zero(row.get("avg_price")), 4),
                    round(_float_or_zero(row.get("avg_stock")), 4),
                    _int_or_zero(row.get("asin_count")),
                    now_iso,
                    now_iso,
                )
            )
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO inventory_snapshot (
                    store_id, snapshot_date, sponsored_type, ad_group_id, ad_group_name,
                    avg_price, avg_stock, asin_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_id, snapshot_date, sponsored_type, ad_group_id)
                DO UPDATE SET
                    ad_group_name=excluded.ad_group_name,
                    avg_price=excluded.avg_price,
                    avg_stock=excluded.avg_stock,
                    asin_count=excluded.asin_count,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def load_dataframe(self, sql: str, params: Sequence[Any]) -> pd.DataFrame:
        conn = self._connect()
        try:
            return pd.read_sql_query(sql, conn, params=params)
        finally:
            conn.close()

    def load_performance_df(self, store_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.load_dataframe(
            """
            SELECT *
            FROM performance_daily
            WHERE store_id = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
            """,
            (store_id, start_date, end_date),
        )

    def load_change_df(self, store_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.load_dataframe(
            """
            SELECT *
            FROM change_history
            WHERE store_id = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
            """,
            (store_id, start_date, end_date),
        )

    def load_placement_df(self, store_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.load_dataframe(
            """
            SELECT *
            FROM placement_daily
            WHERE store_id = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
            """,
            (store_id, start_date, end_date),
        )

    def load_query_terms_df(self, store_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.load_dataframe(
            """
            SELECT *
            FROM query_term_daily
            WHERE store_id = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
            """,
            (store_id, start_date, end_date),
        )

    def load_latest_inventory_df(self, store_id: str) -> pd.DataFrame:
        return self.load_dataframe(
            """
            SELECT i.*
            FROM inventory_snapshot i
            WHERE i.store_id = ?
              AND i.snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM inventory_snapshot
                WHERE store_id = ?
              )
            ORDER BY i.sponsored_type, i.ad_group_id
            """,
            (store_id, store_id),
        )

    def cleanup_old_rows(self, store_id: str, keep_days: int = 400) -> None:
        cutoff = (date.today() - timedelta(days=max(30, keep_days))).isoformat()
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM performance_daily WHERE store_id = ? AND date < ?",
                (store_id, cutoff),
            )
            conn.execute(
                "DELETE FROM placement_daily WHERE store_id = ? AND date < ?",
                (store_id, cutoff),
            )
            conn.execute(
                "DELETE FROM query_term_daily WHERE store_id = ? AND date < ?",
                (store_id, cutoff),
            )
            conn.execute(
                "DELETE FROM change_history WHERE store_id = ? AND date < ?",
                (store_id, cutoff),
            )
            conn.execute(
                "DELETE FROM inventory_snapshot WHERE store_id = ? AND snapshot_date < ?",
                (store_id, cutoff),
            )
            conn.execute(
                "DELETE FROM sync_coverage WHERE store_id = ? AND date < ?",
                (store_id, cutoff),
            )

    def export_store_csv(self, store_id: str) -> Dict[str, Any]:
        PERFORMANCE_DIR.mkdir(parents=True, exist_ok=True)
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)

        perf_df = self.load_dataframe(
            """
            SELECT
                store_id,
                date,
                SUM(clicks) AS clicks,
                ROUND(SUM(spend), 2) AS spend,
                ROUND(SUM(sales), 2) AS sales
            FROM performance_daily
            WHERE store_id = ?
            GROUP BY store_id, date
            ORDER BY date ASC
            """,
            (store_id,),
        )
        if not perf_df.empty:
            perf_df["acos"] = perf_df.apply(
                lambda r: round((float(r["spend"]) / float(r["sales"])) * 100, 2)
                if float(r["sales"]) > 0
                else 0.0,
                axis=1,
            )
            perf_df = perf_df[["store_id", "date", "clicks", "spend", "acos", "sales"]]
            perf_df.to_csv(PERFORMANCE_DIR / f"{store_id}.csv", index=False)

        history_df = self.load_dataframe(
            """
            SELECT
                store_id,
                date,
                COALESCE(ad_group_name, 'UNKNOWN_AD_GROUP') AS ad_group,
                change_type AS action_type,
                old_value,
                new_value
            FROM change_history
            WHERE store_id = ?
              AND (
                change_type LIKE '%bid%'
                OR change_type LIKE '%snapshot%'
              )
            ORDER BY date ASC
            """,
            (store_id,),
        )

        if history_df.empty:
            latest_date = self.get_max_performance_date(store_id) or date.today().isoformat()
            history_df = pd.DataFrame(
                [
                    {
                        "store_id": store_id,
                        "date": latest_date,
                        "ad_group": "UNKNOWN_AD_GROUP",
                        "action_type": "snapshot",
                        "old_bid": 1.0,
                        "new_bid": 1.0,
                    }
                ]
            )
        else:
            history_df["old_bid"] = history_df["old_value"].map(_float_or_zero).round(4)
            history_df["new_bid"] = history_df["new_value"].map(_float_or_zero).round(4)
            history_df = history_df[
                ["store_id", "date", "ad_group", "action_type", "old_bid", "new_bid"]
            ]

        history_df.to_csv(HISTORY_DIR / f"{store_id}.csv", index=False)

        return {
            "performance_rows": int(len(perf_df)),
            "history_rows": int(len(history_df)),
            "performance_path": str(PERFORMANCE_DIR / f"{store_id}.csv"),
            "history_path": str(HISTORY_DIR / f"{store_id}.csv"),
        }


ops_data_store = OpsDataStore()

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PERFORMANCE_DIR = DATA_DIR / "performance"
HISTORY_DIR = DATA_DIR / "history"
PLAYBOOK_DIR = DATA_DIR / "playbooks"


@dataclass
class Store:
    """Multi-tenant container to keep each store's data isolated."""

    store_id: str
    performance_data: pd.DataFrame
    change_history: pd.DataFrame


class StoreRepository:
    def __init__(self) -> None:
        self._cache: Dict[str, Store] = {}

    def list_store_ids(self) -> List[str]:
        return sorted(path.stem for path in PERFORMANCE_DIR.glob("*.csv"))

    def get_store(self, store_id: str) -> Store:
        if store_id in self._cache:
            return self._cache[store_id]

        performance_path = PERFORMANCE_DIR / f"{store_id}.csv"
        history_path = HISTORY_DIR / f"{store_id}.csv"

        if not performance_path.exists() or not history_path.exists():
            raise FileNotFoundError(f"Store data not found for {store_id}")

        perf_df = pd.read_csv(performance_path)
        history_df = pd.read_csv(history_path)

        self._validate_store(perf_df, history_df, store_id)

        perf_df["date"] = pd.to_datetime(perf_df["date"]).dt.date
        history_df["date"] = pd.to_datetime(history_df["date"]).dt.date

        store = Store(
            store_id=store_id,
            performance_data=perf_df,
            change_history=history_df,
        )
        self._cache[store_id] = store
        return store

    def invalidate(self, store_id: str | None = None) -> None:
        if store_id is None:
            self._cache.clear()
            return
        self._cache.pop(store_id, None)

    @staticmethod
    def _validate_store(perf_df: pd.DataFrame, history_df: pd.DataFrame, store_id: str) -> None:
        perf_required = {"store_id", "date", "clicks", "spend", "acos", "sales"}
        history_required = {
            "store_id",
            "date",
            "ad_group",
            "action_type",
            "old_bid",
            "new_bid",
        }

        missing_perf = perf_required - set(perf_df.columns)
        missing_history = history_required - set(history_df.columns)

        if missing_perf:
            raise ValueError(f"Missing performance columns: {sorted(missing_perf)}")
        if missing_history:
            raise ValueError(f"Missing history columns: {sorted(missing_history)}")

        perf_store_ids = set(perf_df["store_id"].astype(str).unique())
        history_store_ids = set(history_df["store_id"].astype(str).unique())

        if perf_store_ids != {store_id}:
            raise ValueError(f"Performance store_id mismatch for {store_id}: {perf_store_ids}")
        if history_store_ids != {store_id}:
            raise ValueError(f"History store_id mismatch for {store_id}: {history_store_ids}")


store_repo = StoreRepository()

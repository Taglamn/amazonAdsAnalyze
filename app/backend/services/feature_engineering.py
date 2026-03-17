from __future__ import annotations

from typing import Dict

import pandas as pd


def _acos_level(acos: float, lower: float, upper: float) -> str:
    if acos > upper:
        return "high"
    if acos < lower:
        return "low"
    return "normal"


def _traffic_level(clicks: float, threshold: int) -> str:
    return "high_clicks" if clicks >= threshold else "low_clicks"


def _conversion_level(cvr: float, threshold: float) -> str:
    return "high_cvr" if cvr >= threshold else "low_cvr"


def build_features(
    samples_df: pd.DataFrame,
    strategy_map: Dict[int, Dict[str, float]],
    default_strategy: Dict[str, float],
    traffic_click_threshold: int,
    conversion_cvr_threshold: float,
) -> pd.DataFrame:
    if samples_df.empty:
        return samples_df

    default_cfg = strategy_map.get(-1, default_strategy)
    rows = []
    for _, row in samples_df.iterrows():
        ad_group_id = int(row["ad_group_id"])
        cfg = strategy_map.get(ad_group_id, default_cfg)
        target_acos = float(cfg["target_acos"])
        upper_acos = float(cfg["upper_acos"])
        lower_acos = float(cfg["lower_acos"])
        before_acos = float(row["before_acos"])
        before_clicks = float(row["before_clicks"])
        before_cvr = float(row["before_cvr"])
        rows.append(
            {
                **row.to_dict(),
                "target_acos": target_acos,
                "upper_acos": upper_acos,
                "lower_acos": lower_acos,
                "acos_level": _acos_level(before_acos, lower=lower_acos, upper=upper_acos),
                "traffic_level": _traffic_level(before_clicks, threshold=traffic_click_threshold),
                "conversion_level": _conversion_level(before_cvr, threshold=conversion_cvr_threshold),
            }
        )

    return pd.DataFrame(rows)

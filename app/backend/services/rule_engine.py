from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List

from .data_processing import build_processed_samples
from .feature_engineering import build_features
from .feedback_learning import evaluate_observations, merge_action_patch
from .repository import rule_engine_repo
from .rule_generation import build_lingxing_rules
from .rule_learning import build_strategy_rules, learn_rules_from_samples


def _resolve_window(start_date: str | None, end_date: str | None) -> Dict[str, str]:
    if start_date and end_date:
        return {"start_date": start_date, "end_date": end_date}
    end_day = date.today() - timedelta(days=1)
    start_day = end_day - timedelta(days=364)
    return {"start_date": start_day.isoformat(), "end_date": end_day.isoformat()}


def process_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    store_id = str(payload["store_id"])
    window = _resolve_window(payload.get("start_date"), payload.get("end_date"))
    default_strategy = {
        "target_acos": float(payload["target_acos"]),
        "upper_acos": float(payload["upper_acos"]),
        "lower_acos": float(payload["lower_acos"]),
    }
    rule_engine_repo.upsert_strategy_configs(
        store_id=store_id,
        default_config=default_strategy,
        overrides=payload.get("strategy_configs", []),
    )
    strategy_map = rule_engine_repo.load_strategy_map(store_id)

    bid_changes_df = rule_engine_repo.load_bid_changes_df(
        store_id=store_id,
        start_date=window["start_date"],
        end_date=window["end_date"],
    )
    perf_df = rule_engine_repo.load_performance_df(
        store_id=store_id,
        start_date=window["start_date"],
        end_date=window["end_date"],
    )
    samples_df, process_summary = build_processed_samples(
        bid_changes_df=bid_changes_df,
        perf_df=perf_df,
        window_days=int(payload["window_days"]),
    )
    featured_df = build_features(
        samples_df=samples_df,
        strategy_map=strategy_map,
        default_strategy=default_strategy,
        traffic_click_threshold=int(payload["traffic_click_threshold"]),
        conversion_cvr_threshold=float(payload["conversion_cvr_threshold"]),
    )

    inserted = 0
    if bool(payload.get("persist", True)) and not featured_df.empty:
        inserted = rule_engine_repo.upsert_processed_samples(featured_df.to_dict(orient="records"))

    preview = featured_df.head(20).to_dict(orient="records")
    return {
        "store_id": store_id,
        "window": window,
        "stats": {
            "bid_changes": len(bid_changes_df),
            "performance_rows": len(perf_df),
            "processed_samples": len(featured_df),
            "persisted_samples": inserted,
            **process_summary,
        },
        "preview": preview,
    }


def learn_rules(payload: Dict[str, Any]) -> Dict[str, Any]:
    store_id = str(payload["store_id"])
    samples_df = rule_engine_repo.load_processed_samples_df(store_id=store_id)
    if samples_df.empty:
        return {
            "store_id": store_id,
            "learned_rules": [],
            "saved_rules": 0,
            "message": "No processed samples found. Run /process-data first.",
        }
    learned = learn_rules_from_samples(
        store_id=store_id,
        samples_df=samples_df,
        min_samples=int(payload["min_samples"]),
        min_win_rate=float(payload["min_win_rate"]),
        max_rules=int(payload["max_rules"]),
    )
    if bool(payload.get("include_strategy_baseline", True)):
        strategy_map = rule_engine_repo.load_strategy_map(store_id)
        default_strategy = strategy_map.get(
            -1,
            {"target_acos": 30.0, "upper_acos": 40.0, "lower_acos": 20.0},
        )
        learned.extend(build_strategy_rules(store_id=store_id, default_strategy=default_strategy))

    saved = rule_engine_repo.save_rules(learned)
    return {
        "store_id": store_id,
        "learned_rules": learned,
        "saved_rules": saved,
    }


def generate_lingxing_rules(payload: Dict[str, Any]) -> Dict[str, Any]:
    store_id = str(payload["store_id"])
    raw_rules = rule_engine_repo.list_rules(
        store_id=store_id,
        active_only=bool(payload.get("active_only", True)),
        limit=int(payload["max_rules"]),
    )
    lingxing_rules = build_lingxing_rules(raw_rules, max_rules=int(payload["max_rules"]))
    if lingxing_rules:
        rule_engine_repo.save_rules(lingxing_rules)
    return {
        "store_id": store_id,
        "count": len(lingxing_rules),
        "rules": lingxing_rules,
    }


def evaluate_rules(payload: Dict[str, Any]) -> Dict[str, Any]:
    store_id = str(payload["store_id"])
    observations = payload.get("observations") or []
    if not observations:
        recent_samples = rule_engine_repo.load_processed_samples_df(store_id=store_id, limit=20)
        fallback_rules = rule_engine_repo.list_rules(store_id=store_id, active_only=True, limit=20)
        observations = []
        for idx, row in enumerate(recent_samples.to_dict(orient="records")):
            if idx >= len(fallback_rules):
                break
            observations.append(
                {
                    "rule_id": fallback_rules[idx]["rule_id"],
                    "before_acos": row.get("before_acos", 0),
                    "after_acos": row.get("after_acos", 0),
                    "before_clicks": row.get("before_clicks", 0),
                    "after_clicks": row.get("after_clicks", 0),
                    "before_cvr": row.get("before_cvr", 0),
                    "after_cvr": row.get("after_cvr", 0),
                }
            )

    evaluated = evaluate_observations(observations)
    result_rows: List[Dict[str, Any]] = []
    for item in evaluated:
        row = dict(item)
        row["store_id"] = store_id
        result_rows.append(row)
    saved = rule_engine_repo.save_rule_results(result_rows)

    if bool(payload.get("update_rules", True)):
        existing_rules = {item["rule_id"]: item for item in rule_engine_repo.list_rules(store_id, active_only=False)}
        for item in evaluated:
            rule = existing_rules.get(item["rule_id"])
            if not rule:
                continue
            action_patch = merge_action_patch(rule.get("action", {}), item.get("action_patch", {}))
            condition_patch = item.get("condition_patch", {})
            next_status = "active"
            if item["effect"] == "harmful" and abs(float(action_patch.get("bid_adjustment_pct", 0.0))) < 1:
                next_status = "disabled"
            rule_engine_repo.patch_rule(
                rule_id=item["rule_id"],
                action_patch=action_patch,
                condition_patch=condition_patch,
                status=next_status,
            )

    return {
        "store_id": store_id,
        "evaluated_count": len(evaluated),
        "saved_results": saved,
        "results": evaluated,
    }


def list_rules(store_id: str, active_only: bool = True, limit: int = 200) -> Dict[str, Any]:
    rules = rule_engine_repo.list_rules(store_id=store_id, active_only=active_only, limit=limit)
    return {
        "store_id": store_id,
        "count": len(rules),
        "rules": rules,
    }

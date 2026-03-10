from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Dict

from app.data_access import store_repo
from app.gemini_bridge import (
    build_advice_prompt,
    call_gemini,
    load_playbook,
    normalize_language,
    validate_metrics_store,
)
from app.whitepaper_store import load_whitepaper


def pick_metrics(store_id: str, target_date: str | None) -> Dict[str, Any]:
    store = store_repo.get_store(store_id)
    perf_df = store.performance_data.copy()
    perf_df["date"] = perf_df["date"].astype(str)

    if target_date:
        metrics_rows = perf_df[perf_df["date"] == target_date]
        if metrics_rows.empty:
            raise ValueError(f"No metrics found for {target_date}")
        row = metrics_rows.iloc[0].to_dict()
    else:
        row = perf_df.sort_values("date").iloc[-1].to_dict()

    return {
        "store_id": row["store_id"],
        "date": row["date"],
        "clicks": int(row["clicks"]),
        "spend": float(row["spend"]),
        "acos": float(row["acos"]),
        "sales": float(row["sales"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Gemini advice from store playbook.")
    parser.add_argument("--store-id", required=True, help="Store ID, e.g., store_a")
    parser.add_argument(
        "--date",
        required=False,
        help="Metrics date in YYYY-MM-DD. Defaults to latest available date.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Gemini model name. Defaults to GEMINI_MODEL (or gemini-2.5-flash).",
    )
    parser.add_argument("--lang", default="zh", help="Output language: zh or en")
    args = parser.parse_args()

    playbook = load_playbook(args.store_id)
    metrics = pick_metrics(store_id=args.store_id, target_date=args.date)
    lang = normalize_language(args.lang)

    # Strict tenant isolation check before Gemini call.
    validate_metrics_store(metrics, args.store_id)
    whitepaper = load_whitepaper(args.store_id)
    if not whitepaper:
        raise ValueError(
            f"No stored whitepaper for {args.store_id}. Generate or import whitepaper before requesting advice."
        )

    prompt = build_advice_prompt(
        store_id=args.store_id,
        rules=playbook.get("rules", {}),
        metrics=metrics,
        whitepaper_context=whitepaper,
        language=lang,
    )

    advice = call_gemini(prompt=prompt, model=args.model)

    print(
        json.dumps(
            {
                "store_id": args.store_id,
                "language": lang,
                "metrics_date": metrics["date"],
                "advice": advice,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

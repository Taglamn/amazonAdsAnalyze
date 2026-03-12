from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.lingxing_context_package import build_lingxing_context_package


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Lingxing context package (date x ad_group) for Gemini input."
    )
    parser.add_argument("--store-id", required=True, help="Target store id, e.g. lingxing_123456")
    parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=365, help="Default range when no dates are given")
    parser.add_argument("--output", default=None, help="Output file path (.json)")
    args = parser.parse_args()

    package = build_lingxing_context_package(
        store_id=args.store_id,
        start_date=args.start_date,
        end_date=args.end_date,
        days=args.days,
    )

    if args.output:
        output_path = Path(args.output)
    else:
        window = package.get("window", {})
        start = window.get("start_date", "start")
        end = window.get("end_date", "end")
        output_path = Path(f"context_package_{args.store_id}_{start}_{end}.json")

    output_path.write_text(json.dumps(package, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(output_path.resolve()))


if __name__ == "__main__":
    main()

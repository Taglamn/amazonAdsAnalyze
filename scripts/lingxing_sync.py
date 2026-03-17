from __future__ import annotations

import argparse
import json

from app.lingxing_sync import sync_lingxing_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync and analyze Amazon ad data from Lingxing ERP")
    parser.add_argument(
        "--store-id",
        help="Sync one Lingxing store only, e.g. lingxing_123456",
        default=None,
    )
    parser.add_argument("--report-date", help="Single report date in YYYY-MM-DD", default=None)
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD", default=None)
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD", default=None)
    parser.add_argument(
        "--force-refetch-before-date",
        help="Force re-fetch dates <= this YYYY-MM-DD, ignoring local coverage cache",
        default=None,
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Do not write synced data to app/data/*.csv",
    )
    args = parser.parse_args()

    result = sync_lingxing_data(
        store_id=args.store_id,
        report_date=args.report_date,
        start_date=args.start_date,
        end_date=args.end_date,
        force_refetch_before_date=args.force_refetch_before_date,
        persist=not args.no_persist,
    )

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from app.ops_advisory import generate_periodic_advice
from app.ops_db import OpsDataStore
from app.ops_whitepaper import synthesize_operational_whitepaper


class OpsPipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.store_id = "lingxing_test_1"
        self.db = OpsDataStore(db_path=Path(self.temp_dir.name) / "ops_test.db")
        self._seed_data()

    def _seed_data(self) -> None:
        start = date(2026, 1, 1)
        performance_rows = []
        placement_rows = []
        query_rows = []
        for i in range(40):
            day = (start + timedelta(days=i)).isoformat()
            performance_rows.append(
                {
                    "store_id": self.store_id,
                    "date": day,
                    "sponsored_type": "sp",
                    "campaign_id": 1001,
                    "campaign_name": "cat_campaign",
                    "ad_group_id": 2001,
                    "ad_group_name": "cat_group",
                    "product_type": "cat",
                    "clicks": 60 + i,
                    "spend": 120 + i,
                    "sales": 320 + (i * 2),
                }
            )
            placement_rows.append(
                {
                    "store_id": self.store_id,
                    "date": day,
                    "sponsored_type": "sp",
                    "campaign_id": 1001,
                    "placement_type": "TOP_OF_SEARCH",
                    "top_of_search_is": 0.35,
                    "clicks": 20 + i,
                    "spend": 65 + i,
                    "sales": 180 + i,
                }
            )
            query_rows.append(
                {
                    "store_id": self.store_id,
                    "date": day,
                    "sponsored_type": "sp",
                    "campaign_id": 1001,
                    "ad_group_id": 2001,
                    "search_term": "bad term",
                    "clicks": 1,
                    "spend": 1.2,
                    "sales": 0.0,
                    "orders": 0,
                }
            )

        change_rows = [
            {
                "event_hash": "t1",
                "store_id": self.store_id,
                "date": "2026-01-10",
                "event_time": "2026-01-10T10:00:00",
                "sponsored_type": "sp",
                "target_level": "adgroup",
                "campaign_id": 1001,
                "ad_group_id": 2001,
                "ad_group_name": "cat_group",
                "change_type": "targeting_bid",
                "field_code": "bid",
                "old_value": "0.8",
                "new_value": "1.0",
                "keyword_text": "",
                "raw_json": "{}",
            },
            {
                "event_hash": "t2",
                "store_id": self.store_id,
                "date": "2026-01-15",
                "event_time": "2026-01-15T10:00:00",
                "sponsored_type": "sp",
                "target_level": "campaign",
                "campaign_id": 1001,
                "ad_group_id": 2001,
                "ad_group_name": "cat_group",
                "change_type": "placement_bid",
                "field_code": "top_bid",
                "old_value": "40",
                "new_value": "60",
                "keyword_text": "",
                "raw_json": "{}",
            },
            {
                "event_hash": "t3",
                "store_id": self.store_id,
                "date": "2026-01-20",
                "event_time": "2026-01-20T10:00:00",
                "sponsored_type": "sp",
                "target_level": "keywords",
                "campaign_id": 1001,
                "ad_group_id": 2001,
                "ad_group_name": "cat_group",
                "change_type": "negative_targeting",
                "field_code": "negative",
                "old_value": "",
                "new_value": "bad term",
                "keyword_text": "bad term",
                "raw_json": "{}",
            },
            {
                "event_hash": "t4",
                "store_id": self.store_id,
                "date": "2026-02-09",
                "event_time": "2026-02-09T00:00:00",
                "sponsored_type": "sp",
                "target_level": "adgroup",
                "campaign_id": 1001,
                "ad_group_id": 2001,
                "ad_group_name": "cat_group",
                "change_type": "targeting_bid_snapshot",
                "field_code": "snapshot_bid",
                "old_value": "1.1",
                "new_value": "1.1",
                "keyword_text": "",
                "raw_json": "{}",
            },
        ]

        inventory_rows = [
            {
                "store_id": self.store_id,
                "snapshot_date": "2026-02-09",
                "sponsored_type": "sp",
                "ad_group_id": 2001,
                "ad_group_name": "cat_group",
                "avg_price": 30.0,
                "avg_stock": 80.0,
                "asin_count": 3,
            }
        ]

        self.db.upsert_performance_rows(performance_rows)
        self.db.upsert_placement_rows(placement_rows)
        self.db.upsert_query_term_rows(query_rows)
        self.db.upsert_change_rows(change_rows)
        self.db.upsert_inventory_rows(inventory_rows)

    def test_whitepaper_and_advisory(self) -> None:
        with patch("app.ops_whitepaper.ops_data_store", self.db):
            with patch("app.ops_advisory.ops_data_store", self.db):
                whitepaper = synthesize_operational_whitepaper(self.store_id)
                advice = generate_periodic_advice(self.store_id)

        self.assertEqual(whitepaper["store_id"], self.store_id)
        self.assertIn("master_strategy", whitepaper["whitepaper"])
        self.assertEqual(advice["store_id"], self.store_id)
        self.assertTrue(advice["ad_group_advice"])
        self.assertIn("manual_review_required", advice)


if __name__ == "__main__":
    unittest.main()

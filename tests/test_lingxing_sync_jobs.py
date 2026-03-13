from __future__ import annotations

import time
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

from app.lingxing_sync_jobs import LingxingSyncJobManager


class LingxingSyncJobManagerTests(unittest.TestCase):
    def _wait_terminal(
        self,
        manager: LingxingSyncJobManager,
        job_id: str,
        timeout_seconds: float = 5.0,
    ) -> Dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_status: Dict[str, Any] = {}
        while time.time() < deadline:
            status = manager.get_job(job_id)
            if status is not None:
                last_status = status
                if status.get("status") in {"succeeded", "failed"}:
                    return status
            time.sleep(0.05)
        self.fail(f"job timeout, job_id={job_id}, last_status={last_status}")

    def test_job_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = LingxingSyncJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
            )

            def sync_func(**kwargs: Any) -> Dict[str, Any]:
                return {
                    "target_store_id": kwargs["store_id"],
                    "stores": [
                        {
                            "store_id": kwargs["store_id"],
                            "lingxing_output_rows": [],
                        }
                    ],
                }

            created = manager.create_job(
                store_id="lingxing_100",
                report_date="2026-03-09",
                start_date=None,
                end_date=None,
                persist=True,
                sync_func=sync_func,
            )

            status = self._wait_terminal(manager, created["job_id"])
            self.assertEqual(status["status"], "succeeded")
            self.assertTrue(status["result_ready"])
            self.assertIn("result", status)
            self.assertEqual(status["result"]["target_store_id"], "lingxing_100")

    def test_job_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = LingxingSyncJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
            )

            def sync_func(**_: Any) -> Dict[str, Any]:
                raise RuntimeError("mock lingxing sync failure")

            created = manager.create_job(
                store_id="lingxing_200",
                report_date=None,
                start_date="2026-03-01",
                end_date="2026-03-09",
                persist=True,
                sync_func=sync_func,
            )

            status = self._wait_terminal(manager, created["job_id"])
            self.assertEqual(status["status"], "failed")
            self.assertIn("mock lingxing sync failure", status["message"])
            self.assertFalse(status["result_ready"])

    def test_job_persists_across_manager_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            manager = LingxingSyncJobManager(
                output_dir=output_dir,
                max_workers=1,
                retention_seconds=3600,
            )

            def sync_func(**kwargs: Any) -> Dict[str, Any]:
                return {"target_store_id": kwargs["store_id"], "stores": []}

            created = manager.create_job(
                store_id="lingxing_300",
                report_date=None,
                start_date="2026-03-01",
                end_date="2026-03-09",
                persist=True,
                sync_func=sync_func,
            )
            status = self._wait_terminal(manager, created["job_id"])
            self.assertEqual(status["status"], "succeeded")

            reloaded_manager = LingxingSyncJobManager(
                output_dir=output_dir,
                max_workers=1,
                retention_seconds=3600,
            )
            reloaded_status = reloaded_manager.get_job(created["job_id"])
            self.assertIsNotNone(reloaded_status)
            assert reloaded_status is not None
            self.assertEqual(reloaded_status["status"], "succeeded")
            self.assertTrue(reloaded_status["result_ready"])

    def test_get_latest_job_for_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = LingxingSyncJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
            )

            def sync_func(**kwargs: Any) -> Dict[str, Any]:
                return {"target_store_id": kwargs["store_id"], "stores": []}

            first = manager.create_job(
                store_id="lingxing_500",
                report_date=None,
                start_date=None,
                end_date=None,
                persist=True,
                sync_func=sync_func,
            )
            self._wait_terminal(manager, first["job_id"])

            second = manager.create_job(
                store_id="lingxing_500",
                report_date=None,
                start_date=None,
                end_date=None,
                persist=True,
                sync_func=sync_func,
            )
            self._wait_terminal(manager, second["job_id"])

            latest = manager.get_latest_job_for_store("lingxing_500")
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest["job_id"], second["job_id"])

    def test_stale_running_job_marked_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = LingxingSyncJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
                stale_seconds=1,
            )

            def sync_func(**kwargs: Any) -> Dict[str, Any]:
                return {"target_store_id": kwargs["store_id"], "stores": []}

            created = manager.create_job(
                store_id="lingxing_600",
                report_date=None,
                start_date=None,
                end_date=None,
                persist=True,
                sync_func=sync_func,
            )
            status = self._wait_terminal(manager, created["job_id"])
            self.assertEqual(status["status"], "succeeded")

            with manager._lock:
                job = manager._jobs[created["job_id"]]
                job.status = "running"
                job.stage = "syncing"
                job.progress_pct = 25
                job.updated_at = job.updated_at - timedelta(minutes=10)
                manager._persist_job_locked(job)

            stale_status = manager.get_job(created["job_id"])
            self.assertIsNotNone(stale_status)
            assert stale_status is not None
            self.assertEqual(stale_status["status"], "failed")
            self.assertIn("stale", stale_status["message"].lower())

    def test_heartbeat_keeps_long_job_alive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = LingxingSyncJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
                stale_seconds=2,
                heartbeat_seconds=1,
            )

            def sync_func(**kwargs: Any) -> Dict[str, Any]:
                time.sleep(3)
                return {"target_store_id": kwargs["store_id"], "stores": []}

            created = manager.create_job(
                store_id="lingxing_700",
                report_date=None,
                start_date=None,
                end_date=None,
                persist=True,
                sync_func=sync_func,
            )

            time.sleep(2.2)
            mid_status = manager.get_job(created["job_id"])
            self.assertIsNotNone(mid_status)
            assert mid_status is not None
            self.assertEqual(mid_status["status"], "running")
            self.assertFalse(bool(mid_status.get("is_stale")))

            final_status = self._wait_terminal(manager, created["job_id"], timeout_seconds=8.0)
            self.assertEqual(final_status["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()

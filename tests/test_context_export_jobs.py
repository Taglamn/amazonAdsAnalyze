from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict

from app.context_export_jobs import ContextExportJobManager


class ContextExportJobManagerTests(unittest.TestCase):
    def _wait_for_terminal(
        self,
        manager: ContextExportJobManager,
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
        self.fail(f"Job {job_id} did not finish in time, last_status={last_status}")

    def test_job_succeeds_and_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = ContextExportJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
            )

            def build_func(**kwargs: Any) -> Dict[str, Any]:
                progress_cb = kwargs.get("progress_cb")
                if progress_cb:
                    progress_cb("daily_reports", 40, "Halfway")
                return {
                    "store_id": kwargs["store_id"],
                    "window": {
                        "start_date": "2026-01-01",
                        "end_date": "2026-01-31",
                    },
                    "ad_groups": [],
                }

            created = manager.create_job(
                store_id="lingxing_1",
                start_date=None,
                end_date=None,
                days=365,
                build_func=build_func,
            )

            status = self._wait_for_terminal(manager, created["job_id"])
            self.assertEqual(status["status"], "succeeded")
            self.assertTrue(status["download_ready"])
            self.assertIn("lingxing_1_context_package_2026-01-01_2026-01-31.json", status["filename"])

            download = manager.get_download_info(created["job_id"])
            self.assertIsNotNone(download)
            assert download is not None
            self.assertEqual(download["status"], "succeeded")
            self.assertTrue(Path(str(download["file_path"])).exists())

    def test_job_failure_reports_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = ContextExportJobManager(
                output_dir=Path(tmp_dir),
                max_workers=1,
                retention_seconds=3600,
            )

            def build_func(**_: Any) -> Dict[str, Any]:
                raise RuntimeError("mock build failed")

            created = manager.create_job(
                store_id="lingxing_2",
                start_date=None,
                end_date=None,
                days=365,
                build_func=build_func,
            )

            status = self._wait_for_terminal(manager, created["job_id"])
            self.assertEqual(status["status"], "failed")
            self.assertIn("mock build failed", status["message"])
            self.assertFalse(status["download_ready"])


if __name__ == "__main__":
    unittest.main()

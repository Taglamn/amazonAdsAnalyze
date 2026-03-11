from __future__ import annotations

import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .data_access import DATA_DIR


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


@dataclass
class ContextExportJob:
    job_id: str
    store_id: str
    start_date: Optional[str]
    end_date: Optional[str]
    days: int
    status: str
    progress_pct: int
    stage: str
    message: str
    created_at: datetime
    updated_at: datetime
    filename: Optional[str] = None
    file_path: Optional[str] = None


class ContextExportJobManager:
    def __init__(
        self,
        output_dir: Optional[Path] = None,
        max_workers: int = 1,
        retention_seconds: int = 86400,
    ) -> None:
        self._output_dir = output_dir or (DATA_DIR / "context_packages")
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ctx-export")
        self._retention_seconds = max(3600, retention_seconds)
        self._lock = threading.Lock()
        self._jobs: Dict[str, ContextExportJob] = {}

    def _to_public_payload(self, job: ContextExportJob) -> Dict[str, Any]:
        download_ready = (
            job.status == "succeeded"
            and bool(job.file_path)
            and Path(str(job.file_path)).exists()
        )
        return {
            "job_id": job.job_id,
            "store_id": job.store_id,
            "status": job.status,
            "progress_pct": int(job.progress_pct),
            "stage": job.stage,
            "message": job.message,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
            "download_ready": download_ready,
            "filename": job.filename,
        }

    def _update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        progress_pct: Optional[int] = None,
        stage: Optional[str] = None,
        message: Optional[str] = None,
        filename: Optional[str] = None,
        file_path: Optional[str] = None,
    ) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if status is not None:
                job.status = status
            if progress_pct is not None:
                job.progress_pct = max(0, min(100, int(progress_pct)))
            if stage is not None:
                job.stage = stage
            if message is not None:
                job.message = message
            if filename is not None:
                job.filename = filename
            if file_path is not None:
                job.file_path = file_path
            job.updated_at = _utc_now()

    def _cleanup_expired_locked(self) -> None:
        threshold = _utc_now() - timedelta(seconds=self._retention_seconds)

        stale_job_ids = [
            job_id
            for job_id, job in self._jobs.items()
            if job.updated_at < threshold
        ]
        for job_id in stale_job_ids:
            job = self._jobs.pop(job_id)
            if job.file_path:
                try:
                    Path(job.file_path).unlink(missing_ok=True)
                except OSError:
                    pass

        for path in self._output_dir.glob("*.json"):
            try:
                modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
            if modified_at < threshold:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass

    def create_job(
        self,
        *,
        store_id: str,
        start_date: Optional[str],
        end_date: Optional[str],
        days: int,
        build_func: Callable[..., Dict[str, Any]],
    ) -> Dict[str, Any]:
        now = _utc_now()
        job_id = uuid.uuid4().hex

        with self._lock:
            self._cleanup_expired_locked()
            self._jobs[job_id] = ContextExportJob(
                job_id=job_id,
                store_id=store_id,
                start_date=start_date,
                end_date=end_date,
                days=days,
                status="queued",
                progress_pct=0,
                stage="queued",
                message="Queued",
                created_at=now,
                updated_at=now,
            )

        self._executor.submit(self._run_job, job_id, build_func)
        return {"job_id": job_id, "status": "queued"}

    def _run_job(self, job_id: str, build_func: Callable[..., Dict[str, Any]]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            store_id = job.store_id
            start_date = job.start_date
            end_date = job.end_date
            days = job.days

        self._update_job(
            job_id,
            status="running",
            progress_pct=1,
            stage="starting",
            message="Starting context package build",
        )

        def progress_cb(stage: str, pct: int, msg: str) -> None:
            self._update_job(
                job_id,
                status="running",
                progress_pct=pct,
                stage=stage,
                message=msg,
            )

        try:
            package = build_func(
                store_id=store_id,
                start_date=start_date,
                end_date=end_date,
                days=days,
                progress_cb=progress_cb,
            )

            window = package.get("window", {})
            start = str(window.get("start_date") or "start")
            end = str(window.get("end_date") or "end")
            filename = f"{store_id}_context_package_{start}_{end}.json"
            file_path = self._output_dir / f"{job_id}.json"

            progress_cb("writing_file", 96, "Writing package file")
            file_path.write_text(json.dumps(package, ensure_ascii=False, indent=2), encoding="utf-8")

            self._update_job(
                job_id,
                status="succeeded",
                progress_pct=100,
                stage="completed",
                message="Context package is ready",
                filename=filename,
                file_path=str(file_path),
            )
        except Exception as exc:  # noqa: BLE001
            self._update_job(
                job_id,
                status="failed",
                progress_pct=100,
                stage="failed",
                message=str(exc),
            )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return self._to_public_payload(job)

    def get_download_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return {
                "store_id": job.store_id,
                "status": job.status,
                "file_path": job.file_path,
                "filename": job.filename,
            }


context_export_job_manager = ContextExportJobManager(
    max_workers=_read_env_int("CONTEXT_EXPORT_MAX_WORKERS", default=1, minimum=1, maximum=4),
    retention_seconds=_read_env_int(
        "CONTEXT_EXPORT_RETENTION_HOURS", default=24, minimum=1, maximum=168
    )
    * 3600,
)

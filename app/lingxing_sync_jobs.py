from __future__ import annotations

import json
import os
import threading
import time
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
class LingxingSyncJob:
    job_id: str
    store_id: str
    report_date: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    persist: bool
    status: str
    progress_pct: int
    stage: str
    message: str
    created_at: datetime
    updated_at: datetime
    result: Optional[Dict[str, Any]] = None


class LingxingSyncJobManager:
    def __init__(
        self,
        output_dir: Optional[Path] = None,
        max_workers: int = 1,
        retention_seconds: int = 86400,
        stale_seconds: int = 2700,
        heartbeat_seconds: int = 30,
    ) -> None:
        self._output_dir = output_dir or (DATA_DIR / "lingxing_sync_jobs")
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="lingxing-sync")
        self._retention_seconds = max(3600, retention_seconds)
        self._stale_seconds = max(300, int(stale_seconds))
        self._heartbeat_seconds = max(5, int(heartbeat_seconds))
        self._lock = threading.Lock()
        self._jobs: Dict[str, LingxingSyncJob] = {}
        self._load_jobs_from_disk()

    def _job_meta_path(self, job_id: str) -> Path:
        return self._output_dir / f"{job_id}.json"

    def _serialize_job(self, job: LingxingSyncJob) -> Dict[str, Any]:
        return {
            "job_id": job.job_id,
            "store_id": job.store_id,
            "report_date": job.report_date,
            "start_date": job.start_date,
            "end_date": job.end_date,
            "persist": bool(job.persist),
            "status": job.status,
            "progress_pct": int(job.progress_pct),
            "stage": job.stage,
            "message": job.message,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
            "result": job.result,
        }

    def _deserialize_job(self, payload: Dict[str, Any]) -> LingxingSyncJob:
        created_at = datetime.fromisoformat(str(payload.get("created_at")))
        updated_at = datetime.fromisoformat(str(payload.get("updated_at")))
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        job = LingxingSyncJob(
            job_id=str(payload.get("job_id") or ""),
            store_id=str(payload.get("store_id") or ""),
            report_date=payload.get("report_date"),
            start_date=payload.get("start_date"),
            end_date=payload.get("end_date"),
            persist=bool(payload.get("persist", True)),
            status=str(payload.get("status") or "failed"),
            progress_pct=int(payload.get("progress_pct") or 0),
            stage=str(payload.get("stage") or "unknown"),
            message=str(payload.get("message") or ""),
            created_at=created_at,
            updated_at=updated_at,
            result=payload.get("result") if isinstance(payload.get("result"), dict) else None,
        )
        if not job.job_id or not job.store_id:
            raise ValueError("invalid job payload")
        return job

    def _persist_job_locked(self, job: LingxingSyncJob) -> None:
        path = self._job_meta_path(job.job_id)
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(self._serialize_job(job), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(path)

    def _load_jobs_from_disk(self) -> None:
        now = _utc_now()
        threshold = now - timedelta(seconds=self._retention_seconds)
        loaded: Dict[str, LingxingSyncJob] = {}

        for path in self._output_dir.glob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    path.unlink(missing_ok=True)
                    continue
                job = self._deserialize_job(payload)
            except (OSError, json.JSONDecodeError, ValueError):
                path.unlink(missing_ok=True)
                continue

            if job.updated_at < threshold:
                path.unlink(missing_ok=True)
                continue

            if job.status in {"queued", "running"}:
                job.status = "failed"
                job.stage = "failed"
                job.progress_pct = 100
                job.message = "Task interrupted by service restart"
                job.updated_at = now
                try:
                    path.write_text(json.dumps(self._serialize_job(job), ensure_ascii=False, indent=2), encoding="utf-8")
                except OSError:
                    pass

            loaded[job.job_id] = job

        with self._lock:
            self._jobs = loaded

    def _to_public_payload(self, job: LingxingSyncJob) -> Dict[str, Any]:
        age_seconds = max(0, int((_utc_now() - job.updated_at).total_seconds()))
        is_stale = bool(job.status in {"queued", "running"} and age_seconds > self._stale_seconds)
        payload: Dict[str, Any] = {
            "job_id": job.job_id,
            "store_id": job.store_id,
            "status": job.status,
            "progress_pct": int(job.progress_pct),
            "stage": job.stage,
            "message": job.message,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
            "age_seconds": age_seconds,
            "is_stale": is_stale,
            "stale_after_seconds": self._stale_seconds,
            "result_ready": bool(job.status == "succeeded" and isinstance(job.result, dict)),
        }
        if job.status == "succeeded" and isinstance(job.result, dict):
            payload["result"] = job.result
        return payload

    def _mark_stale_job_locked(self, job: LingxingSyncJob) -> bool:
        if job.status not in {"queued", "running"}:
            return False
        age_seconds = int((_utc_now() - job.updated_at).total_seconds())
        if age_seconds <= self._stale_seconds:
            return False

        job.status = "failed"
        job.progress_pct = 100
        job.stage = "failed"
        job.message = (
            f"Task heartbeat stale for {age_seconds}s. "
            "Marked as failed; please retry sync."
        )
        job.updated_at = _utc_now()
        self._persist_job_locked(job)
        return True

    def _touch_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job.status != "running":
                return
            job.updated_at = _utc_now()
            self._persist_job_locked(job)

    def _update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        progress_pct: Optional[int] = None,
        stage: Optional[str] = None,
        message: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
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
            if result is not None:
                job.result = result
            job.updated_at = _utc_now()
            self._persist_job_locked(job)

    def _cleanup_expired_locked(self) -> None:
        threshold = _utc_now() - timedelta(seconds=self._retention_seconds)
        stale_ids = [
            job_id
            for job_id, job in self._jobs.items()
            if job.updated_at < threshold
        ]
        for job_id in stale_ids:
            self._jobs.pop(job_id, None)
            try:
                self._job_meta_path(job_id).unlink(missing_ok=True)
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
        report_date: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        persist: bool,
        sync_func: Callable[..., Dict[str, Any]],
    ) -> Dict[str, Any]:
        now = _utc_now()
        job_id = uuid.uuid4().hex
        with self._lock:
            self._cleanup_expired_locked()
            job = LingxingSyncJob(
                job_id=job_id,
                store_id=store_id,
                report_date=report_date,
                start_date=start_date,
                end_date=end_date,
                persist=persist,
                status="queued",
                progress_pct=0,
                stage="queued",
                message="Queued",
                created_at=now,
                updated_at=now,
            )
            self._jobs[job_id] = job
            self._persist_job_locked(job)

        self._executor.submit(self._run_job, job_id, sync_func)
        return {"job_id": job_id, "status": "queued"}

    def _run_job(self, job_id: str, sync_func: Callable[..., Dict[str, Any]]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            store_id = job.store_id
            report_date = job.report_date
            start_date = job.start_date
            end_date = job.end_date
            persist = job.persist

        self._update_job(
            job_id,
            status="running",
            progress_pct=5,
            stage="starting",
            message="Starting Lingxing sync",
        )

        heartbeat_stop = threading.Event()

        def heartbeat_loop() -> None:
            while not heartbeat_stop.wait(self._heartbeat_seconds):
                self._touch_job(job_id)

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            name=f"lingxing-sync-heartbeat-{job_id[:8]}",
            daemon=True,
        )
        heartbeat_thread.start()

        def progress_cb(stage: str, pct: int, msg: str) -> None:
            self._update_job(
                job_id,
                status="running",
                progress_pct=pct,
                stage=stage,
                message=msg,
            )

        try:
            progress_cb("syncing", 25, "Syncing Lingxing data")
            result = sync_func(
                store_id=store_id,
                report_date=report_date,
                start_date=start_date,
                end_date=end_date,
                persist=persist,
                progress_cb=progress_cb,
            )
            self._update_job(
                job_id,
                status="succeeded",
                progress_pct=100,
                stage="completed",
                message="Lingxing sync completed",
                result=result,
            )
        except Exception as exc:  # noqa: BLE001
            self._update_job(
                job_id,
                status="failed",
                progress_pct=100,
                stage="failed",
                message=str(exc),
            )
        finally:
            heartbeat_stop.set()

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            self._mark_stale_job_locked(job)
            return self._to_public_payload(job)

    def get_job_store_id(self, job_id: str) -> Optional[str]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return job.store_id

    def get_latest_job_for_store(self, store_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            matches = [job for job in self._jobs.values() if job.store_id == store_id]
            if not matches:
                return None
            latest = max(matches, key=lambda x: (x.created_at, x.updated_at))
            self._mark_stale_job_locked(latest)
            return self._to_public_payload(latest)


lingxing_sync_job_manager = LingxingSyncJobManager(
    max_workers=_read_env_int("LINGXING_SYNC_MAX_WORKERS", default=1, minimum=1, maximum=4),
    retention_seconds=_read_env_int(
        "LINGXING_SYNC_RETENTION_HOURS", default=24, minimum=1, maximum=168
    )
    * 3600,
    stale_seconds=_read_env_int(
        "LINGXING_SYNC_STALE_MINUTES", default=45, minimum=5, maximum=1440
    )
    * 60,
    heartbeat_seconds=_read_env_int(
        "LINGXING_SYNC_HEARTBEAT_SECONDS", default=30, minimum=5, maximum=600
    ),
)

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .analysis import build_bid_recommendations, build_optimization_cases
from .backend.routes import optimization_router
from .auth.api import router as auth_router
from .auth.bootstrap import init_auth_schema
from .auth.config import get_auth_settings
from .auth.crud import bulk_sync_stores, ensure_default_tenant, list_accessible_stores
from .auth.database import SessionLocal, get_db_session
from .auth.dependencies import enforce_store_access, get_current_user
from .auth.middleware import JWTAuthMiddleware
from .auth.models import User
from .customer_service_ai.api import router as customer_service_router
from .customer_service_ai.db import init_customer_service_schema
from .context_export_jobs import context_export_job_manager
from .data_access import HISTORY_DIR, PERFORMANCE_DIR, PLAYBOOK_DIR, Store, store_repo
from .gemini_bridge import (
    build_advice_prompt,
    build_whitepaper_prompt,
    call_gemini_with_meta,
    load_playbook,
    normalize_language,
    validate_metrics_store,
    yesterday_metrics_from_rows,
)
from .lingxing_sync import sync_lingxing_data
from .lingxing_sync_jobs import lingxing_sync_job_manager
from .lingxing_client import LingxingClient, LingxingCredentials
from .lingxing_context_package import build_lingxing_context_package
from .ops_advisory import generate_periodic_advice
from .ops_logger import get_ops_logger
from .ops_sync import incremental_sync_store
from .ops_whitepaper import read_operational_whitepaper, synthesize_operational_whitepaper
from .upload_analysis import build_upload_summary, parse_uploaded_workbook, serialize_performance_rows
from .whitepaper_store import load_whitepaper, save_whitepaper, whitepaper_info


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"

app = FastAPI(title="Amazon Ads Analyzer", version="1.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(JWTAuthMiddleware)
app.include_router(auth_router)
app.include_router(customer_service_router)
app.include_router(optimization_router)
logger = get_ops_logger()


@app.on_event("startup")
def on_startup() -> None:
    try:
        init_auth_schema()
        init_customer_service_schema()
        _bootstrap_default_tenant_stores()
    except Exception as exc:  # noqa: BLE001
        logger.warning("auth_or_customer_service_schema_init_failed error=%s", exc)


class AdviceRequest(BaseModel):
    metrics: Optional[Dict[str, Any]] = None
    model: Optional[str] = None
    lang: str = "zh"


class WhitepaperRequest(BaseModel):
    model: Optional[str] = None
    lang: str = "zh"


class AIContinueRequest(BaseModel):
    mode: Literal["whitepaper", "advice"] = "advice"
    current_text: str
    model: Optional[str] = None
    lang: str = "zh"


class PlaybookUpdateRequest(BaseModel):
    rules: Dict[str, Any]
    merge: bool = True
    store_name: Optional[str] = None


class LingxingSyncRequest(BaseModel):
    store_id: Optional[str] = None
    report_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    force_refetch_before_date: Optional[str] = None
    persist: bool = True


class LingxingSyncJobRequest(BaseModel):
    store_id: str
    report_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    force_refetch_before_date: Optional[str] = None
    persist: bool = True


class ContextPackageRequest(BaseModel):
    store_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: int = 365


class ContextPackageJobRequest(BaseModel):
    store_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    days: int = 365


class OpsIncrementalSyncRequest(BaseModel):
    store_id: str
    persist_csv: bool = True


class OpsWhitepaperSynthesisRequest(BaseModel):
    store_id: str


class OpsAdvisoryRequest(BaseModel):
    store_id: str
    refresh_whitepaper: bool = False


DEFAULT_UPLOAD_RULES = {
    "negative_search_terms": "Conservative Negative Search Terms",
    "acos_limit": 45,
    "bid_step_up_pct": 8,
    "bid_step_down_pct": 12,
    "focus": "Balance growth and efficiency with strict spend discipline",
}


def _playbook_path(store_id: str) -> Path:
    return PLAYBOOK_DIR / f"store_playbook_{store_id}.json"


def _load_or_init_playbook_payload(store_id: str, store_name: Optional[str] = None) -> Dict[str, Any]:
    PLAYBOOK_DIR.mkdir(parents=True, exist_ok=True)
    path = _playbook_path(store_id)
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            if str(payload.get("store_id") or "") != store_id:
                payload["store_id"] = store_id
            if store_name and not str(payload.get("store_name") or "").strip():
                payload["store_name"] = store_name
            if not isinstance(payload.get("rules"), dict):
                payload["rules"] = dict(DEFAULT_UPLOAD_RULES)
            return payload

    resolved_name = store_name or store_repo.get_store_name(store_id)
    return {
        "store_id": store_id,
        "store_name": resolved_name,
        "rules": dict(DEFAULT_UPLOAD_RULES),
    }


def _save_playbook_payload(store_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    PLAYBOOK_DIR.mkdir(parents=True, exist_ok=True)
    normalized = dict(payload or {})
    normalized["store_id"] = store_id
    normalized["store_name"] = str(normalized.get("store_name") or store_repo.get_store_name(store_id))
    rules = normalized.get("rules")
    if not isinstance(rules, dict):
        rules = dict(DEFAULT_UPLOAD_RULES)
    normalized["rules"] = rules

    path = _playbook_path(store_id)
    with path.open("w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)
    return normalized


def _parse_upload_rules(raw_rules: Optional[str]) -> Dict[str, Any]:
    if not raw_rules:
        return dict(DEFAULT_UPLOAD_RULES)

    try:
        parsed = json.loads(raw_rules)
        if isinstance(parsed, dict):
            merged = dict(DEFAULT_UPLOAD_RULES)
            merged.update(parsed)
            return merged
    except json.JSONDecodeError:
        pass

    merged = dict(DEFAULT_UPLOAD_RULES)
    merged["custom_rule_text"] = raw_rules.strip()
    return merged


def _decode_uploaded_text(file_bytes: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk", "latin-1"):
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("Unable to decode uploaded whitepaper file")


def _build_stored_text_meta(content: str) -> Dict[str, Any]:
    normalized = (content or "").replace("\r\n", "\n").replace("\r", "\n")
    return {
        "finish_reason": "STORED",
        "finish_reasons": ["STORED"],
        "char_count": len(normalized),
        "line_count": normalized.count("\n") + 1 if normalized else 0,
    }


def _append_without_overlap(current: str, extra: str) -> str:
    base = (current or "").rstrip()
    incoming = (extra or "").lstrip()
    if not base:
        return incoming
    if not incoming:
        return base

    max_overlap = min(len(base), len(incoming), 400)
    overlap = 0
    for size in range(max_overlap, 0, -1):
        if base[-size:] == incoming[:size]:
            overlap = size
            break
    merged = base + "\n" + incoming[overlap:].lstrip()
    return merged


def _build_continue_prompt(mode: str, current_text: str, language: str) -> str:
    lang = normalize_language(language)
    lang_line = (
        "Continue in Simplified Chinese with local Amazon ads operator wording."
        if lang == "zh"
        else "Continue in clear business English with Amazon ads wording."
    )
    doc_name = "Lingxing auto-rule blueprint" if mode == "whitepaper" else "auto-rule optimization advice"
    tail = current_text[-6000:] if len(current_text) > 6000 else current_text
    return (
        f"You are continuing a {doc_name} markdown document.\n"
        "Continue from exactly where it stopped.\n"
        "Do not restart, do not summarize previous content, do not repeat existing lines.\n"
        "Return continuation text only.\n"
        f"{lang_line}\n\n"
        "<<CURRENT_TEXT_TAIL_START>>\n"
        f"{tail}\n"
        "<<CURRENT_TEXT_TAIL_END>>\n"
    )


def _resolve_whitepaper_min_char_count() -> int:
    raw = os.getenv("WHITEPAPER_MIN_CHAR_COUNT", "").strip()
    if not raw:
        return 1200
    try:
        value = int(raw)
    except ValueError:
        return 1200
    return max(200, min(value, 20000))


def _list_lingxing_bound_stores() -> List[Dict[str, Any]]:
    credentials = LingxingCredentials.from_env()
    client = LingxingClient(credentials=credentials)
    access_token = client.generate_access_token()
    sellers = client.list_sellers(access_token=access_token)

    stores: List[Dict[str, Any]] = []
    for item in sellers:
        if int(item.get("status", 0) or 0) != 1:
            continue

        sid = int(item["sid"])
        store_id = f"lingxing_{sid}"
        stores.append(
            {
                "store_id": store_id,
                "store_name": str(item.get("name") or store_id),
                "sid": sid,
                "country": item.get("country"),
                "has_ads_setting": int(item.get("has_ads_setting", 0) or 0) == 1,
                "has_local_data": (
                    (PERFORMANCE_DIR / f"{store_id}.csv").exists()
                    and (HISTORY_DIR / f"{store_id}.csv").exists()
                ),
                "source": "lingxing_bound",
            }
        )

    stores.sort(key=lambda x: str(x.get("store_name") or x.get("store_id")))
    return stores


def _bootstrap_default_tenant_stores() -> None:
    """Ensure local store catalog exists in auth tables for default tenant."""

    db = SessionLocal()
    try:
        settings = get_auth_settings()
        tenant = ensure_default_tenant(db, settings.bootstrap_tenant_name)
        local_stores = store_repo.list_stores()
        bulk_sync_stores(
            db,
            tenant_id=tenant.tenant_id,
            stores=[(item["store_id"], item["store_name"]) for item in local_stores],
        )
    finally:
        db.close()


def _ensure_store_scope(db: Session, current_user: User, store_id: str) -> None:
    """Enforce store-level authorization for current user."""

    enforce_store_access(db, current_user=current_user, external_store_id=store_id)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/stores")
def list_stores(
    include_bound: bool = True,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    local_stores = store_repo.list_stores()
    local_store_map: Dict[str, Dict[str, Any]] = {item["store_id"]: item for item in local_stores}
    local_store_ids = set(local_store_map.keys())
    bulk_sync_stores(
        db,
        tenant_id=current_user.tenant_id,
        stores=[(item["store_id"], item["store_name"]) for item in local_stores],
    )
    bound_stores: List[Dict[str, Any]] = []
    bound_error: Optional[str] = None
    if include_bound:
        try:
            bound_stores = _list_lingxing_bound_stores()
            bulk_sync_stores(
                db,
                tenant_id=current_user.tenant_id,
                stores=[(item["store_id"], item["store_name"]) for item in bound_stores],
            )
        except Exception as exc:  # noqa: BLE001
            bound_error = str(exc)

    accessible_stores = list_accessible_stores(db, user=current_user)
    visible_store_ids = {item.external_store_id for item in accessible_stores}
    merged: Dict[str, Dict[str, Any]] = {
        item.external_store_id: {
            "store_id": item.external_store_id,
            "store_name": item.store_name,
            "has_local_data": item.external_store_id in local_store_ids,
            "source": "auth",
        }
        for item in accessible_stores
    }

    for store_id, item in local_store_map.items():
        if store_id not in visible_store_ids:
            continue
        existing = merged.get(store_id)
        if existing:
            existing["store_name"] = item["store_name"] or existing["store_name"]
            existing["has_local_data"] = True
            existing["source"] = "local+auth"
            continue
        merged[store_id] = {
            "store_id": store_id,
            "store_name": item["store_name"],
            "has_local_data": True,
            "source": "local",
        }

    if include_bound and bound_stores:
        # Bound stores may add newly discovered store rows for admin users.
        accessible_stores = list_accessible_stores(db, user=current_user)
        visible_store_ids = {item.external_store_id for item in accessible_stores}
        for item in bound_stores:
            if item["store_id"] not in visible_store_ids:
                continue
            existing = merged.get(item["store_id"])
            if existing:
                existing["store_name"] = item["store_name"] or existing["store_name"]
                existing["sid"] = item.get("sid")
                existing["country"] = item.get("country")
                existing["has_local_data"] = bool(existing.get("has_local_data"))
                existing["source"] = (
                    "local+lingxing_bound" if existing.get("has_local_data") else "auth+lingxing_bound"
                )
            else:
                merged[item["store_id"]] = item

    stores = sorted(
        merged.values(),
        key=lambda x: str(x.get("store_name") or x.get("store_id")),
    )

    return {
        "stores": stores,
        "store_ids": [item["store_id"] for item in stores],
        "include_bound": include_bound,
        "bound_error": bound_error,
    }


def _serialize_store_rows(store: Store) -> List[Dict[str, Any]]:
    rows = store.performance_data.sort_values("date").to_dict(orient="records")
    for row in rows:
        row["date"] = row["date"].isoformat()
    return rows


@app.get("/api/stores/{store_id}/performance")
def get_store_performance(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    try:
        store = store_repo.get_store(store_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "daily_performance": _serialize_store_rows(store),
    }


@app.get("/api/stores/{store_id}/optimization-cases")
def get_optimization_cases(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    try:
        store = store_repo.get_store(store_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    cases = build_optimization_cases(
        store_id=store_id,
        history_df=store.change_history,
        perf_df=store.performance_data,
    )
    return {"store_id": store_id, "cases": cases}


@app.get("/api/stores/{store_id}/ad-group-recommendations")
def get_ad_group_recommendations(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    try:
        store = store_repo.get_store(store_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    recommendations = build_bid_recommendations(
        store_id=store_id,
        history_df=store.change_history,
        perf_df=store.performance_data,
    )
    return {"store_id": store_id, "recommendations": recommendations}


@app.post("/api/stores/{store_id}/ai/advice")
def get_ai_advice(
    store_id: str,
    payload: AdviceRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    try:
        store = store_repo.get_store(store_id)
        playbook = load_playbook(store_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    performance_rows = _serialize_store_rows(store)
    metrics = payload.metrics or yesterday_metrics_from_rows(performance_rows)
    cases = build_optimization_cases(
        store_id=store_id,
        history_df=store.change_history,
        perf_df=store.performance_data,
    )

    try:
        lang = normalize_language(payload.lang)
        validate_metrics_store(metrics, store_id)
        whitepaper_context = load_whitepaper(store_id) or ""
        whitepaper_source = "stored" if whitepaper_context else "generated"
        whitepaper_meta: Dict[str, Any] = (
            _build_stored_text_meta(whitepaper_context) if whitepaper_context else {}
        )

        whitepaper_min_chars = _resolve_whitepaper_min_char_count()
        if not whitepaper_context or len(whitepaper_context.strip()) < whitepaper_min_chars:
            whitepaper_prompt = build_whitepaper_prompt(
                store_id=store_id,
                rules=playbook.get("rules", {}),
                performance_rows=performance_rows,
                cases=cases,
                language=lang,
            )
            whitepaper_context, whitepaper_meta = call_gemini_with_meta(
                prompt=whitepaper_prompt, model=payload.model
            )
            save_whitepaper(store_id, whitepaper_context)
            whitepaper_source = "generated"

        prompt = build_advice_prompt(
            store_id=store_id,
            rules=playbook.get("rules", {}),
            metrics=metrics,
            whitepaper_context=whitepaper_context,
            language=lang,
        )
        advice, advice_meta = call_gemini_with_meta(prompt=prompt, model=payload.model)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "language": lang,
        "metrics": metrics,
        "advice": advice,
        "advice_meta": advice_meta,
        "whitepaper_source": whitepaper_source,
        "whitepaper_meta": whitepaper_meta,
    }


@app.post("/api/stores/{store_id}/ai/whitepaper")
def get_ai_whitepaper(
    store_id: str,
    payload: WhitepaperRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    try:
        store = store_repo.get_store(store_id)
        playbook = load_playbook(store_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    performance_rows = _serialize_store_rows(store)
    cases = build_optimization_cases(
        store_id=store_id,
        history_df=store.change_history,
        perf_df=store.performance_data,
    )

    try:
        lang = normalize_language(payload.lang)
        prompt = build_whitepaper_prompt(
            store_id=store_id,
            rules=playbook.get("rules", {}),
            performance_rows=performance_rows,
            cases=cases,
            language=lang,
        )
        whitepaper, whitepaper_meta = call_gemini_with_meta(prompt=prompt, model=payload.model)
        save_whitepaper(store_id, whitepaper)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "language": lang,
        "whitepaper": whitepaper,
        "whitepaper_meta": whitepaper_meta,
    }


@app.post("/api/stores/{store_id}/ai/continue")
def continue_ai_output(
    store_id: str,
    payload: AIContinueRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    current_text = (payload.current_text or "").strip()
    if not current_text:
        raise HTTPException(status_code=400, detail="current_text is required")

    try:
        lang = normalize_language(payload.lang)
        prompt = _build_continue_prompt(
            mode=payload.mode,
            current_text=current_text,
            language=lang,
        )
        continuation_text, continuation_meta = call_gemini_with_meta(
            prompt=prompt,
            model=payload.model,
        )
        merged_text = _append_without_overlap(current_text, continuation_text)
        merged_meta = _build_stored_text_meta(merged_text)
        merged_meta.update(
            {
                "finish_reason": continuation_meta.get("finish_reason"),
                "finish_reasons": continuation_meta.get("finish_reasons"),
                "truncated": continuation_meta.get("truncated", False),
                "continuation_char_count": continuation_meta.get("char_count", 0),
                "continuation_line_count": continuation_meta.get("line_count", 0),
            }
        )
        if payload.mode == "whitepaper":
            save_whitepaper(store_id, merged_text)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "mode": payload.mode,
        "language": lang,
        "text": merged_text,
        "delta_text": continuation_text,
        "meta": merged_meta,
        "continuation_meta": continuation_meta,
    }


@app.get("/api/stores/{store_id}/playbook")
def get_store_playbook(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)
    try:
        payload = _load_or_init_playbook_payload(store_id=store_id)
        payload = _save_playbook_payload(store_id=store_id, payload=payload)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return payload


@app.put("/api/stores/{store_id}/playbook")
def update_store_playbook(
    store_id: str,
    payload: PlaybookUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)
    try:
        existing = _load_or_init_playbook_payload(
            store_id=store_id,
            store_name=payload.store_name,
        )
        incoming_rules = dict(payload.rules or {})
        if payload.merge:
            merged_rules = dict(existing.get("rules") or {})
            merged_rules.update(incoming_rules)
            existing["rules"] = merged_rules
        else:
            existing["rules"] = incoming_rules
        if payload.store_name:
            existing["store_name"] = payload.store_name
        saved = _save_playbook_payload(store_id=store_id, payload=existing)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return saved


@app.get("/api/stores/{store_id}/whitepaper")
def get_store_whitepaper(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)
    return whitepaper_info(store_id)


@app.post("/api/stores/{store_id}/whitepaper/import")
async def import_store_whitepaper(
    store_id: str,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    if not file.filename:
        raise HTTPException(status_code=400, detail="Please upload a whitepaper file")

    lower = file.filename.lower()
    if not (lower.endswith(".txt") or lower.endswith(".md")):
        raise HTTPException(status_code=400, detail="Only .txt/.md files are supported")

    try:
        file_bytes = await file.read()
        content = _decode_uploaded_text(file_bytes)
        save_whitepaper(store_id, content)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return whitepaper_info(store_id)


@app.get("/api/stores/{store_id}/whitepaper/export")
def export_store_whitepaper(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> PlainTextResponse:
    _ensure_store_scope(db, current_user, store_id)

    content = load_whitepaper(store_id)
    if not content:
        raise HTTPException(status_code=404, detail="Whitepaper not found for this store")

    filename = f"{store_id}_whitepaper.txt"
    headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    return PlainTextResponse(content=content, headers=headers)


@app.post("/api/lingxing/sync")
def sync_lingxing(
    payload: LingxingSyncRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    if not payload.store_id:
        raise HTTPException(status_code=422, detail="store_id is required")
    _ensure_store_scope(db, current_user, payload.store_id)

    try:
        result = sync_lingxing_data(
            store_id=payload.store_id,
            report_date=payload.report_date,
            start_date=payload.start_date,
            end_date=payload.end_date,
            force_refetch_before_date=payload.force_refetch_before_date,
            persist=payload.persist,
        )
        if payload.persist:
            store_repo.invalidate()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result


@app.post("/api/lingxing/sync/jobs", status_code=202)
def create_lingxing_sync_job(
    payload: LingxingSyncJobRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, payload.store_id)

    def _sync_runner(
        store_id: str,
        report_date: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        force_refetch_before_date: Optional[str],
        persist: bool,
        progress_cb: Any = None,
    ) -> Dict[str, Any]:
        result = sync_lingxing_data(
            store_id=store_id,
            report_date=report_date,
            start_date=start_date,
            end_date=end_date,
            force_refetch_before_date=force_refetch_before_date,
            persist=persist,
            progress_cb=progress_cb,
        )
        if persist:
            store_repo.invalidate()
        return result

    try:
        return lingxing_sync_job_manager.create_job(
            store_id=payload.store_id,
            report_date=payload.report_date,
            start_date=payload.start_date,
            end_date=payload.end_date,
            force_refetch_before_date=payload.force_refetch_before_date,
            persist=payload.persist,
            sync_func=_sync_runner,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/lingxing/sync/jobs/{job_id}")
def get_lingxing_sync_job(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    job_store_id = lingxing_sync_job_manager.get_job_store_id(job_id)
    if not job_store_id:
        raise HTTPException(status_code=404, detail="Lingxing sync job not found")
    _ensure_store_scope(db, current_user, job_store_id)

    job = lingxing_sync_job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Lingxing sync job not found")
    return job


@app.get("/api/lingxing/sync/jobs/latest/by-store")
def get_latest_lingxing_sync_job(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)
    job = lingxing_sync_job_manager.get_latest_job_for_store(store_id)
    return {"store_id": store_id, "job": job}


@app.post("/api/ops/sync/incremental")
def sync_ops_incremental(
    payload: OpsIncrementalSyncRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, payload.store_id)

    try:
        result = incremental_sync_store(
            store_id=payload.store_id,
            persist_csv=payload.persist_csv,
        )
        if payload.persist_csv:
            store_repo.invalidate()
    except Exception as exc:  # noqa: BLE001
        logger.exception("ops_incremental_sync_failed store_id=%s", payload.store_id)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result


@app.post("/api/ops/whitepaper/synthesize")
def synthesize_ops_whitepaper(
    payload: OpsWhitepaperSynthesisRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, payload.store_id)

    try:
        return synthesize_operational_whitepaper(store_id=payload.store_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ops_whitepaper_synthesis_failed store_id=%s", payload.store_id)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/ops/whitepaper/{store_id}")
def get_ops_whitepaper(
    store_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, store_id)

    try:
        return read_operational_whitepaper(store_id=store_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ops_whitepaper_read_failed store_id=%s", store_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/ops/advisory")
def get_ops_advisory(
    payload: OpsAdvisoryRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, payload.store_id)

    try:
        if payload.refresh_whitepaper:
            synthesize_operational_whitepaper(store_id=payload.store_id)
        return generate_periodic_advice(store_id=payload.store_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ops_advisory_failed store_id=%s", payload.store_id)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/lingxing/context-package/jobs", status_code=202)
def create_lingxing_context_package_job(
    payload: ContextPackageJobRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _ensure_store_scope(db, current_user, payload.store_id)

    try:
        result = context_export_job_manager.create_job(
            store_id=payload.store_id,
            start_date=payload.start_date,
            end_date=payload.end_date,
            days=payload.days,
            build_func=build_lingxing_context_package,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result


@app.get("/api/lingxing/context-package/jobs/{job_id}")
def get_lingxing_context_package_job(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    job = context_export_job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Context package job not found")
    _ensure_store_scope(db, current_user, str(job.get("store_id") or ""))
    return job


@app.get("/api/lingxing/context-package/jobs/{job_id}/download")
def download_lingxing_context_package_job(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> FileResponse:
    info = context_export_job_manager.get_download_info(job_id)
    if not info:
        raise HTTPException(status_code=404, detail="Context package job not found")
    _ensure_store_scope(db, current_user, str(info.get("store_id") or ""))

    status = str(info.get("status") or "")
    if status != "succeeded":
        raise HTTPException(
            status_code=409,
            detail=f"Context package job is {status}. Please wait until it succeeds.",
        )

    file_path_text = str(info.get("file_path") or "").strip()
    if not file_path_text:
        raise HTTPException(status_code=404, detail="Context package file is missing")

    file_path = Path(file_path_text)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Context package file not found on disk")

    filename = str(info.get("filename") or f"{job_id}_context_package.json")
    return FileResponse(
        path=file_path,
        media_type="application/json; charset=utf-8",
        filename=filename,
    )


@app.post("/api/lingxing/context-package/export")
def export_lingxing_context_package(
    payload: ContextPackageRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Response:
    _ensure_store_scope(db, current_user, payload.store_id)

    try:
        package = build_lingxing_context_package(
            store_id=payload.store_id,
            start_date=payload.start_date,
            end_date=payload.end_date,
            days=payload.days,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    window = package.get("window", {})
    start = str(window.get("start_date") or "start")
    end = str(window.get("end_date") or "end")
    filename = f"{payload.store_id}_context_package_{start}_{end}.json"
    headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    return Response(
        content=json.dumps(package, ensure_ascii=False, indent=2),
        media_type="application/json; charset=utf-8",
        headers=headers,
    )


@app.post("/api/ai/upload-analysis")
async def analyze_uploaded_excel(
    file: UploadFile = File(...),
    store_id: str = Form("uploaded_store"),
    lang: str = Form("zh"),
    model: Optional[str] = Form(None),
    rules: Optional[str] = Form(None),
    run_gemini: bool = Form(True),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Please upload a valid Excel file")

    lower_name = file.filename.lower()
    if not (lower_name.endswith(".xlsx") or lower_name.endswith(".xls")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xls files are supported")

    try:
        store_id = store_id.strip() or "uploaded_store"
        _ensure_store_scope(db, current_user, store_id)
        language = normalize_language(lang)
        file_bytes = await file.read()
        workbook = parse_uploaded_workbook(file_bytes=file_bytes, store_id=store_id)
        summary = build_upload_summary(workbook)

        performance_rows = serialize_performance_rows(workbook.performance_df)
        latest_metrics = yesterday_metrics_from_rows(performance_rows)
        validate_metrics_store(latest_metrics, store_id)

        cases = build_optimization_cases(
            store_id=store_id,
            history_df=workbook.history_df,
            perf_df=workbook.performance_df,
        )
        recommendations = build_bid_recommendations(
            store_id=store_id,
            history_df=workbook.history_df,
            perf_df=workbook.performance_df,
        )

        playbook_rules = _parse_upload_rules(rules)

        whitepaper = ""
        advice = ""
        whitepaper_meta: Dict[str, Any] = {}
        advice_meta: Dict[str, Any] = {}
        whitepaper_source = "none"
        if run_gemini:
            whitepaper_context = load_whitepaper(store_id) or ""
            whitepaper_source = "stored" if whitepaper_context else "generated"
            whitepaper_min_chars = _resolve_whitepaper_min_char_count()
            if whitepaper_context and len(whitepaper_context.strip()) >= whitepaper_min_chars:
                whitepaper = whitepaper_context
                whitepaper_meta = _build_stored_text_meta(whitepaper)
            else:
                whitepaper_prompt = build_whitepaper_prompt(
                    store_id=store_id,
                    rules=playbook_rules,
                    performance_rows=performance_rows,
                    cases=cases,
                    language=language,
                )
                whitepaper, whitepaper_meta = call_gemini_with_meta(
                    prompt=whitepaper_prompt, model=model
                )
                save_whitepaper(store_id, whitepaper)

            advice_prompt = build_advice_prompt(
                store_id=store_id,
                rules=playbook_rules,
                metrics=latest_metrics,
                whitepaper_context=whitepaper,
                language=language,
            )
            advice, advice_meta = call_gemini_with_meta(prompt=advice_prompt, model=model)

    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "language": language,
        "summary": summary,
        "cases": cases,
        "heuristic_recommendations": recommendations,
        "whitepaper": whitepaper,
        "advice": advice,
        "whitepaper_meta": whitepaper_meta,
        "advice_meta": advice_meta,
        "whitepaper_source": whitepaper_source,
    }

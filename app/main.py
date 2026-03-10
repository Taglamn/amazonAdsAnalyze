from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .analysis import build_bid_recommendations, build_optimization_cases
from .data_access import Store, store_repo
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


class AdviceRequest(BaseModel):
    metrics: Optional[Dict[str, Any]] = None
    model: Optional[str] = None
    lang: str = "zh"


class WhitepaperRequest(BaseModel):
    model: Optional[str] = None
    lang: str = "zh"


class LingxingSyncRequest(BaseModel):
    report_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    persist: bool = True


DEFAULT_UPLOAD_RULES = {
    "negative_search_terms": "Conservative Negative Search Terms",
    "acos_limit": 45,
    "bid_step_up_pct": 8,
    "bid_step_down_pct": 12,
    "focus": "Balance growth and efficiency with strict spend discipline",
}


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


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/stores")
def list_stores() -> Dict[str, List[str]]:
    return {"stores": store_repo.list_store_ids()}


def _serialize_store_rows(store: Store) -> List[Dict[str, Any]]:
    rows = store.performance_data.sort_values("date").to_dict(orient="records")
    for row in rows:
        row["date"] = row["date"].isoformat()
    return rows


@app.get("/api/stores/{store_id}/performance")
def get_store_performance(store_id: str) -> Dict[str, Any]:
    try:
        store = store_repo.get_store(store_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "daily_performance": _serialize_store_rows(store),
    }


@app.get("/api/stores/{store_id}/optimization-cases")
def get_optimization_cases(store_id: str) -> Dict[str, Any]:
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
def get_ad_group_recommendations(store_id: str) -> Dict[str, Any]:
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
def get_ai_advice(store_id: str, payload: AdviceRequest) -> Dict[str, Any]:
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

        if not whitepaper_context:
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
def get_ai_whitepaper(store_id: str, payload: WhitepaperRequest) -> Dict[str, Any]:
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


@app.get("/api/stores/{store_id}/whitepaper")
def get_store_whitepaper(store_id: str) -> Dict[str, Any]:
    return whitepaper_info(store_id)


@app.post("/api/stores/{store_id}/whitepaper/import")
async def import_store_whitepaper(
    store_id: str, file: UploadFile = File(...)
) -> Dict[str, Any]:
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
def export_store_whitepaper(store_id: str) -> PlainTextResponse:
    content = load_whitepaper(store_id)
    if not content:
        raise HTTPException(status_code=404, detail="Whitepaper not found for this store")

    filename = f"{store_id}_whitepaper.txt"
    headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    return PlainTextResponse(content=content, headers=headers)


@app.post("/api/lingxing/sync")
def sync_lingxing(payload: LingxingSyncRequest) -> Dict[str, Any]:
    try:
        result = sync_lingxing_data(
            report_date=payload.report_date,
            start_date=payload.start_date,
            end_date=payload.end_date,
            persist=payload.persist,
        )
        if payload.persist:
            store_repo.invalidate()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result


@app.post("/api/ai/upload-analysis")
async def analyze_uploaded_excel(
    file: UploadFile = File(...),
    store_id: str = Form("uploaded_store"),
    lang: str = Form("zh"),
    model: Optional[str] = Form(None),
    rules: Optional[str] = Form(None),
    run_gemini: bool = Form(True),
) -> Dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Please upload a valid Excel file")

    lower_name = file.filename.lower()
    if not (lower_name.endswith(".xlsx") or lower_name.endswith(".xls")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xls files are supported")

    try:
        store_id = store_id.strip() or "uploaded_store"
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
            if whitepaper_context:
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

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .analysis import build_bid_recommendations, build_optimization_cases
from .data_access import Store, store_repo
from .gemini_bridge import (
    build_advice_prompt,
    build_whitepaper_prompt,
    call_gemini,
    load_playbook,
    normalize_language,
    validate_metrics_store,
    yesterday_metrics_from_rows,
)


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
    model: str = "gemini-1.5-flash"
    lang: str = "zh"


class WhitepaperRequest(BaseModel):
    model: str = "gemini-1.5-flash"
    lang: str = "zh"


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

    try:
        lang = normalize_language(payload.lang)
        validate_metrics_store(metrics, store_id)
        prompt = build_advice_prompt(
            store_id=store_id,
            rules=playbook.get("rules", {}),
            metrics=metrics,
            language=lang,
        )
        advice = call_gemini(prompt=prompt, model=payload.model)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "language": lang,
        "metrics": metrics,
        "advice": advice,
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
        whitepaper = call_gemini(prompt=prompt, model=payload.model)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "store_id": store_id,
        "language": lang,
        "whitepaper": whitepaper,
    }

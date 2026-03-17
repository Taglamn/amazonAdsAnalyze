from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ...auth.database import get_db_session
from ...auth.dependencies import enforce_store_access, get_current_user
from ...auth.models import User
from ..models import (
    EvaluateRulesRequest,
    GenerateRulesRequest,
    LearnRulesRequest,
    ProcessDataRequest,
)
from ..services.rule_engine import evaluate_rules, generate_lingxing_rules, learn_rules, list_rules, process_data


router = APIRouter(tags=["Amazon Ads AI Optimization"])


def _enforce_scope(db: Session, current_user: User, store_id: str) -> None:
    enforce_store_access(db, current_user=current_user, external_store_id=store_id)


@router.post("/process-data")
def process_data_api(
    payload: ProcessDataRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _enforce_scope(db=db, current_user=current_user, store_id=payload.store_id)
    return process_data(payload.model_dump())


@router.post("/learn-rules")
def learn_rules_api(
    payload: LearnRulesRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _enforce_scope(db=db, current_user=current_user, store_id=payload.store_id)
    return learn_rules(payload.model_dump())


@router.post("/generate-rules")
def generate_rules_api(
    payload: GenerateRulesRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _enforce_scope(db=db, current_user=current_user, store_id=payload.store_id)
    return generate_lingxing_rules(payload.model_dump())


@router.post("/evaluate-rules")
def evaluate_rules_api(
    payload: EvaluateRulesRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _enforce_scope(db=db, current_user=current_user, store_id=payload.store_id)
    return evaluate_rules(payload.model_dump())


@router.get("/rules")
def list_rules_api(
    store_id: str = Query(..., description="Store ID"),
    active_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    _enforce_scope(db=db, current_user=current_user, store_id=store_id)
    return list_rules(store_id=store_id, active_only=active_only, limit=limit)

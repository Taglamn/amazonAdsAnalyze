from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class StrategyConfigItem(BaseModel):
    ad_group_id: int
    target_acos: float = Field(default=30.0, ge=0)
    upper_acos: float = Field(default=40.0, ge=0)
    lower_acos: float = Field(default=20.0, ge=0)


class ProcessDataRequest(BaseModel):
    store_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    window_days: int = Field(default=3, ge=1, le=14)
    target_acos: float = Field(default=30.0, ge=0)
    upper_acos: float = Field(default=40.0, ge=0)
    lower_acos: float = Field(default=20.0, ge=0)
    traffic_click_threshold: int = Field(default=20, ge=1)
    conversion_cvr_threshold: float = Field(default=0.1, ge=0)
    strategy_configs: List[StrategyConfigItem] = Field(default_factory=list)
    persist: bool = True


class LearnRulesRequest(BaseModel):
    store_id: str
    min_samples: int = Field(default=4, ge=1)
    min_win_rate: float = Field(default=0.55, ge=0.0, le=1.0)
    include_strategy_baseline: bool = True
    max_rules: int = Field(default=30, ge=1, le=200)


class GenerateRulesRequest(BaseModel):
    store_id: str
    active_only: bool = True
    max_rules: int = Field(default=50, ge=1, le=200)


class RuleObservation(BaseModel):
    rule_id: str
    before_acos: float = Field(ge=0)
    after_acos: float = Field(ge=0)
    before_clicks: int = Field(default=0, ge=0)
    after_clicks: int = Field(default=0, ge=0)
    before_cvr: float = Field(default=0.0, ge=0)
    after_cvr: float = Field(default=0.0, ge=0)


class EvaluateRulesRequest(BaseModel):
    store_id: str
    observations: List[RuleObservation] = Field(default_factory=list)
    update_rules: bool = True


class RuleResponse(BaseModel):
    rule_id: str
    store_id: str
    rule_name: str
    condition: Dict[str, Any]
    action: Dict[str, Any]
    lingxing_rule: Dict[str, Any]
    source: str
    status: str
    confidence: float
    win_rate: float
    sample_size: int
    updated_at: str

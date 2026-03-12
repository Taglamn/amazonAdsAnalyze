from __future__ import annotations

from dataclasses import dataclass

from .llm import CustomerServiceLLM
from .prompts import PRODUCT_ISSUE_PROMPT, RISK_DETECTION_PROMPT, RISK_OPTIONS


@dataclass(frozen=True)
class RiskResult:
    risk_level: str
    reason: str


@dataclass(frozen=True)
class ProductIssueResult:
    product_issue: str


class RiskDetectionService:
    def detect(self, llm: CustomerServiceLLM, buyer_message: str) -> RiskResult:
        prompt = RISK_DETECTION_PROMPT.replace("{buyer_message}", buyer_message)
        payload = llm.generate_json(prompt)
        raw_risk = str(payload.get("risk_level") or "").strip().lower()
        risk_level = raw_risk if raw_risk in RISK_OPTIONS else "medium"
        reason = str(payload.get("reason") or "").strip()
        return RiskResult(risk_level=risk_level, reason=reason)


class ProductIssueExtractionService:
    def extract(self, llm: CustomerServiceLLM, buyer_message: str) -> ProductIssueResult:
        prompt = PRODUCT_ISSUE_PROMPT.replace("{buyer_message}", buyer_message)
        payload = llm.generate_json(prompt)
        issue = str(payload.get("product_issue") or "").strip()
        return ProductIssueResult(product_issue=issue)

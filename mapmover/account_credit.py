"""Public runtime adapter for private hosted-Research credit authority."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin

import requests

from mapmover import logger
from mapmover.paths import SITE_URL


MICRO_USD_PER_DOLLAR = 1_000_000
RESEARCH_NEGATIVE_FLOOR_MICRO_USD = -1_000_000
RESEARCH_TOP_UP_CTA = "top_up"
RESEARCH_TOP_UP_URL = "/settings/account"
RESEARCH_CREDIT_CHECK_PATH = "/internal/research-credit/check"
RESEARCH_CREDIT_SETTLE_PATH = "/internal/research-credit/settle"
RESEARCH_CREDIT_TIMEOUT_SECONDS = 10.0
SUPPORTED_CALLER_KINDS = {"authenticated", "qa_suite", "qa_http_suite"}


@dataclass
class ResearchBudgetDecision:
    allowed: bool
    balance_micro_usd: int
    floor_micro_usd: int = RESEARCH_NEGATIVE_FLOOR_MICRO_USD
    error_code: Optional[str] = None
    message: Optional[str] = None
    cta: Optional[str] = None
    cta_url: Optional[str] = None


def research_credit_enabled() -> bool:
    configured = str(os.getenv("RESEARCH_CREDIT_SERVICE_ENABLED", "")).strip().lower()
    if configured:
        return configured in {"1", "true", "yes", "on"}
    return str(os.getenv("COMMERCIAL_ACCESS_ENABLED", "")).strip().lower() in {"1", "true", "yes", "on"}


def research_credit_base_url() -> str:
    configured = str(os.getenv("RESEARCH_CREDIT_SERVICE_BASE_URL", "")).strip().rstrip("/")
    if configured:
        return configured
    verifier = str(os.getenv("COMMERCIAL_ACCESS_VERIFIER_BASE_URL", "")).strip().rstrip("/")
    return verifier or SITE_URL.rstrip("/")


def research_credit_timeout_seconds() -> float:
    raw_value = str(os.getenv("RESEARCH_CREDIT_TIMEOUT_SECONDS", "")).strip()
    if not raw_value:
        return RESEARCH_CREDIT_TIMEOUT_SECONDS
    try:
        return max(1.0, float(raw_value))
    except ValueError:
        return RESEARCH_CREDIT_TIMEOUT_SECONDS


def research_credit_internal_token() -> str:
    return str(os.getenv("CLOUD_INTERNAL_API_TOKEN", "")).strip()


def post_research_credit(path: str, payload: dict) -> tuple[int, dict | None]:
    url = urljoin(f"{research_credit_base_url()}/", path.lstrip("/"))
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    token = research_credit_internal_token()
    if token:
        headers["x-internal-api-key"] = token
    response = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=research_credit_timeout_seconds(),
    )
    try:
        body = response.json()
    except Exception:
        body = None
    return response.status_code, body if isinstance(body, dict) else None


def _billable_identity(caller_ctx: dict) -> tuple[str, str]:
    caller_kind = str((caller_ctx or {}).get("caller_kind") or "").strip().lower()
    user_id = str((caller_ctx or {}).get("auth_user_id") or "").strip()
    return caller_kind, user_id


def check_research_budget(caller_ctx: dict, model: str | None = None) -> ResearchBudgetDecision:
    """Ask the private authority whether hosted Research may run this turn."""
    caller_kind, user_id = _billable_identity(caller_ctx)

    if caller_kind not in SUPPORTED_CALLER_KINDS or not user_id or not research_credit_enabled():
        return ResearchBudgetDecision(allowed=True, balance_micro_usd=0)

    try:
        status_code, payload = post_research_credit(
            RESEARCH_CREDIT_CHECK_PATH,
            {"caller_kind": caller_kind, "user_id": user_id, "model": model},
        )
    except Exception as exc:
        logger.warning("Research credit check unavailable user=%s: %s", user_id, exc)
        return ResearchBudgetDecision(allowed=True, balance_micro_usd=0)

    if status_code != 200 or not isinstance(payload, dict):
        logger.warning("Research credit check failed user=%s status=%s", user_id, status_code)
        return ResearchBudgetDecision(allowed=True, balance_micro_usd=0)

    try:
        balance_micro_usd = int(payload.get("balance_micro_usd") or 0)
    except (TypeError, ValueError):
        balance_micro_usd = 0

    if not bool(payload.get("allowed", True)):
        return ResearchBudgetDecision(
            allowed=False,
            balance_micro_usd=balance_micro_usd,
            floor_micro_usd=int(payload.get("floor_micro_usd") or RESEARCH_NEGATIVE_FLOOR_MICRO_USD),
            error_code=str(payload.get("error_code") or "research_top_up_required"),
            message=str(payload.get("message") or "Top up your account to continue using hosted Research."),
            cta=str(payload.get("cta") or RESEARCH_TOP_UP_CTA),
            cta_url=str(payload.get("cta_url") or RESEARCH_TOP_UP_URL),
        )

    return ResearchBudgetDecision(allowed=True, balance_micro_usd=balance_micro_usd)


def research_budget_rejection_payload(decision: ResearchBudgetDecision) -> dict:
    return {
        "type": "error",
        "error_code": decision.error_code or "research_top_up_required",
        "message": decision.message or "Top up your account to continue using hosted Research.",
        "cta": decision.cta or RESEARCH_TOP_UP_CTA,
        "cta_url": decision.cta_url or RESEARCH_TOP_UP_URL,
        "balance_micro_usd": decision.balance_micro_usd,
        "balance_usd": decision.balance_micro_usd / MICRO_USD_PER_DOLLAR,
        "floor_micro_usd": decision.floor_micro_usd,
    }


def settle_research_charge(
    *,
    request_id: str,
    caller_ctx: dict,
    request_fingerprint: Optional[str] = None,
    selected_model: Optional[str] = None,
) -> Optional[dict]:
    caller_kind, user_id = _billable_identity(caller_ctx)
    if (
        caller_kind not in SUPPORTED_CALLER_KINDS
        or not user_id
        or not request_id
        or not research_credit_enabled()
    ):
        return None

    try:
        status_code, payload = post_research_credit(
            RESEARCH_CREDIT_SETTLE_PATH,
            {
                "caller_kind": caller_kind,
                "user_id": user_id,
                "request_id": request_id,
                "request_fingerprint": request_fingerprint,
                "selected_model": selected_model,
            },
        )
    except Exception as exc:
        logger.warning("Research charge settlement unavailable request=%s: %s", request_id, exc)
        return None

    if status_code != 200 or not isinstance(payload, dict):
        logger.warning("Research charge settlement failed request=%s status=%s", request_id, status_code)
        return None
    return payload

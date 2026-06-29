"""Narrow public-runtime client for private hosted account operations."""

from __future__ import annotations

import os
from urllib.parse import urljoin

import requests

from mapmover.paths import SITE_URL


ACCOUNT_CONTEXT_PATH = "/internal/runtime-account/context"
SAVED_CORPUS_PATH = "/internal/runtime-account/corpus"
FEEDBACK_PATH = "/internal/runtime-account/feedback"
ANONYMOUS_USAGE_PATH = "/internal/runtime-account/anonymous-usage"
RUNTIME_EVENTS_PATH = "/internal/runtime-events"
DEFAULT_TIMEOUT_SECONDS = 10.0


class HostedControlPlaneUnavailable(RuntimeError):
    pass


def control_plane_enabled() -> bool:
    configured = str(os.getenv("HOSTED_CONTROL_PLANE_ENABLED", "")).strip().lower()
    if configured:
        return configured in {"1", "true", "yes", "on"}
    return bool(control_plane_internal_token())


def control_plane_base_url() -> str:
    configured = str(os.getenv("HOSTED_CONTROL_PLANE_BASE_URL", "")).strip().rstrip("/")
    if configured:
        return configured
    verifier = str(os.getenv("COMMERCIAL_ACCESS_VERIFIER_BASE_URL", "")).strip().rstrip("/")
    return verifier or SITE_URL.rstrip("/")


def control_plane_internal_token() -> str:
    return str(os.getenv("CLOUD_INTERNAL_API_TOKEN", "")).strip()


def control_plane_timeout_seconds() -> float:
    raw = str(os.getenv("HOSTED_CONTROL_PLANE_TIMEOUT_SECONDS", "")).strip()
    if not raw:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        return max(1.0, float(raw))
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS


def post_control_plane(path: str, payload: dict) -> tuple[int, dict | None]:
    if not control_plane_enabled():
        return 503, None
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    token = control_plane_internal_token()
    if token:
        headers["x-internal-api-key"] = token
    response = requests.post(
        urljoin(f"{control_plane_base_url()}/", path.lstrip("/")),
        json=payload,
        headers=headers,
        timeout=control_plane_timeout_seconds(),
    )
    try:
        body = response.json()
    except Exception:
        body = None
    return response.status_code, body if isinstance(body, dict) else None


def get_account_context(user_id: str) -> dict | None:
    if not user_id:
        return None
    try:
        status, payload = post_control_plane(ACCOUNT_CONTEXT_PATH, {"user_id": user_id})
    except Exception:
        return None
    return payload if status == 200 and isinstance(payload, dict) else None


def get_saved_corpus(user_id: str, corpus_id: str) -> dict | None:
    if not user_id or not corpus_id:
        return None
    try:
        status, payload = post_control_plane(
            SAVED_CORPUS_PATH,
            {"user_id": user_id, "corpus_id": corpus_id},
        )
    except Exception as exc:
        raise HostedControlPlaneUnavailable("Hosted saved-corpus service is unavailable") from exc
    if status == 404:
        return None
    if status != 200:
        raise HostedControlPlaneUnavailable(
            f"Hosted saved-corpus service returned HTTP {status}"
        )
    corpus = (payload or {}).get("corpus")
    return corpus if isinstance(corpus, dict) else None


def submit_feedback(message: str, source: str, user_id: str | None = None) -> bool:
    try:
        status, payload = post_control_plane(
            FEEDBACK_PATH,
            {"message": message, "source": source, "user_id": user_id},
        )
    except Exception:
        return False
    return bool(status == 200 and isinstance(payload, dict) and payload.get("ok"))


def get_anonymous_usage_cost(ip_hash: str, start_at: str) -> str | None:
    if not ip_hash or not start_at:
        return None
    try:
        status, payload = post_control_plane(
            ANONYMOUS_USAGE_PATH,
            {"ip_hash": ip_hash, "start_at": start_at},
        )
    except Exception:
        return None
    value = (payload or {}).get("cost_usd") if status == 200 else None
    return str(value) if value is not None else None


def emit_runtime_event(event_kind: str, payload: dict) -> bool:
    try:
        status, response = post_control_plane(
            RUNTIME_EVENTS_PATH,
            {"event_kind": event_kind, "payload": payload},
        )
    except Exception:
        return False
    return bool(status == 200 and isinstance(response, dict) and response.get("ok"))


class HostedEventSink:
    """Compatibility-shaped sink used by the existing logging call sites."""

    def log_api_usage_event(self, **payload):
        return emit_runtime_event("api_usage", payload)

    def log_security_event(self, **payload):
        return emit_runtime_event("security", payload)

    def log_session_message(self, **payload):
        return emit_runtime_event("conversation", payload)

    def log_llm_usage_event(self, **payload):
        return emit_runtime_event("llm_usage", payload)

    def log_error(self, **payload):
        return emit_runtime_event("error", payload)

    def log_data_quality_issue(self, **payload):
        return emit_runtime_event("data_quality", payload)

    def log_missing_geometry(self, *, country_names, **payload):
        logged = False
        for name in country_names or []:
            logged = emit_runtime_event(
                "data_quality",
                {"issue_type": "missing_geometry", "name": name, **payload},
            ) or logged
        return logged

    def log_missing_region(self, *, region_name, **payload):
        return emit_runtime_event(
            "data_quality",
            {"issue_type": "missing_region", "name": region_name, **payload},
        )


_event_sink = HostedEventSink()


def get_hosted_event_sink() -> HostedEventSink | None:
    return _event_sink if control_plane_enabled() else None

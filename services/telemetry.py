"""
Telemetry Client
================
Queries Azure Monitor alerts, metrics, and Application Insights logs/traces.
Uses service-principal auth via direct REST (avoids requiring full Azure SDK).
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from azure.identity import DefaultAzureCredential

from services.config import Settings

logger = logging.getLogger(__name__)


# ── Auth ─────────────────────────────────────────────────────────────────

def _get_management_token() -> str | None:
    """Obtain an Azure Management bearer token using DefaultAzureCredential."""
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://management.azure.com/.default")
        logger.debug("Management token acquired — expires_on=%s",
                     token.expires_on)
        return token.token
    except Exception as exc:
        logger.exception("Failed to obtain Azure management token | error_type=%s | detail=%s",
                         type(exc).__name__, exc)
        return None


def _mgmt_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── Azure Monitor Alerts ─────────────────────────────────────────────────

def query_alerts(settings: Settings, resource_id: str) -> list[dict]:
    """Fetch recently fired alerts from Azure Monitor for *resource_id*."""
    token = _get_management_token()
    if not token:
        logger.warning("No management token — cannot query alerts")
        return []
    url = (
        f"https://management.azure.com/subscriptions/{settings.azure_subscription_id}"
        f"/providers/Microsoft.AlertsManagement/alerts"
    )
    # Pass targetResource as a structured param so requests URL-encodes it,
    # preventing query-parameter injection if resource_id contains special chars.
    params = {
        "api-version": "2023-07-12-preview",
        "targetResource": resource_id,
    }
    logger.debug("Querying alerts — url=%s resource_id=%s", url, resource_id)
    try:
        resp = requests.get(url, headers=_mgmt_headers(
            token), params=params, timeout=30)
        logger.debug("Alert query response — status=%d url=%s",
                     resp.status_code, resp.url)
        if resp.status_code == 404:
            logger.info(
                "No alerts found for resource (404) — returning empty list")
            return []
        if not resp.ok:
            logger.error("Alert query failed — status=%d url=%s body=%.500s",
                         resp.status_code, resp.url, resp.text)
        resp.raise_for_status()
        alerts = resp.json().get("value", [])
        logger.info("Alert query returned %d alert(s)", len(alerts))
        return alerts
    except requests.RequestException as exc:
        logger.exception(
            "Alert query failed | error_type=%s | url=%s", type(exc).__name__, url)
        return []


# ── Azure Monitor Metrics ────────────────────────────────────────────────

def query_metrics(settings: Settings, resource_id: str, metric_names: str = "") -> dict:
    """
    Fetch metrics from Azure Monitor.  *metric_names* is a comma-separated
    list (e.g. ``"Percentage CPU,Available Memory Bytes"``).  If empty the API
    returns default metrics.
    """
    token = _get_management_token()
    if not token:
        return {}
    url = f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"
    params: dict = {
        "api-version": "2018-01-01",
        "timespan": "PT1H",
        "interval": "PT5M",
    }
    if metric_names:
        params["metricnames"] = metric_names
    logger.debug("Querying metrics — url=%s", url)
    try:
        resp = requests.get(url, headers=_mgmt_headers(
            token), params=params, timeout=30)
        logger.debug("Metrics response — status=%d url=%s",
                     resp.status_code, resp.url)
        if not resp.ok:
            logger.error("Metrics query failed — status=%d url=%s body=%.500s",
                         resp.status_code, resp.url, resp.text)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.exception(
            "Metrics query failed | error_type=%s | url=%s", type(exc).__name__, url)
        return {}


# ── Application Insights Logs ────────────────────────────────────────────

def _ai_get(settings: Settings, query: str) -> dict:
    """Execute a KQL query against App Insights and return the raw response."""
    if not settings.appinsights_app_id:
        return {}
    url = f"https://api.applicationinsights.io/v1/apps/{settings.appinsights_app_id}/query"
    logger.debug("App Insights query — app_id=%s url=%s query=%.200s",
                 settings.appinsights_app_id, url, query)
    try:
        credential = DefaultAzureCredential()
        ai_token = credential.get_token(
            "https://api.applicationinsights.io/.default")
        logger.debug("App Insights token acquired — expires_on=%s",
                     ai_token.expires_on)
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {ai_token.token}"},
            params={"query": query},
            timeout=30,
        )
        logger.debug("App Insights response — status=%d url=%s",
                     resp.status_code, url)
        if not resp.ok:
            logger.error("App Insights query failed — status=%d app_id=%s body=%.500s",
                         resp.status_code, settings.appinsights_app_id, resp.text)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.exception("App Insights query failed | error_type=%s | app_id=%s | url=%s",
                         type(exc).__name__, settings.appinsights_app_id, url)
        return {}


def query_exceptions(settings: Settings) -> dict:
    """
    Fetch recent exceptions with full details: error message, type,
    stack trace, and the operation/request that caused them.
    """
    if not settings.appinsights_app_id:
        logger.warning(
            "App Insights not configured — skipping exceptions query")
        return {}
    query = (
        "exceptions"
        "| where timestamp > ago(1h)"
        "| project timestamp, type, outerMessage, innermostMessage,"
        "  details, problemId, operation_Id, operation_Name,"
        "  cloud_RoleName, appName"
        "| order by timestamp desc"
        "| take 20"
    )
    logger.info("Querying App Insights exceptions")
    return _ai_get(settings, query)


def query_failed_requests(settings: Settings) -> dict:
    """
    Fetch failed HTTP requests with full context: URL, method, response code,
    duration, user agent, and custom dimensions (input parameters).
    Distinguishes 4xx client errors (bad API calls) from 5xx server errors.
    """
    if not settings.appinsights_app_id:
        logger.warning(
            "App Insights not configured — skipping failed requests query")
        return {}
    query = (
        "requests"
        "| where timestamp > ago(1h)"
        "| where success == false"
        "| project timestamp, name, url, resultCode, duration,"
        "  performanceBucket, operation_Id, operation_Name,"
        "  client_Type, client_Browser, client_OS,"
        "  customDimensions, customMeasurements,"
        "  cloud_RoleName, appName"
        "| order by timestamp desc"
        "| take 20"
    )
    logger.info("Querying App Insights failed requests")
    return _ai_get(settings, query)


def query_traces(settings: Settings) -> dict:
    """
    Fetch recent warning/error traces to provide surrounding log context
    around the time the alert fired.
    """
    if not settings.appinsights_app_id:
        logger.warning("App Insights not configured — skipping traces query")
        return {}
    query = (
        "traces"
        "| where timestamp > ago(1h)"
        "| where severityLevel >= 2"  # 2=Warning, 3=Error, 4=Critical
        "| project timestamp, message, severityLevel, operation_Id,"
        "  operation_Name, cloud_RoleName, customDimensions"
        "| order by timestamp desc"
        "| take 30"
    )
    logger.info("Querying App Insights warning/error traces")
    return _ai_get(settings, query)


def query_dependencies(settings: Settings) -> dict:
    """
    Fetch failed OUTBOUND dependency calls logged by App Insights.

    Captures failures when the function app calls external services
    (Azure OpenAI, Azure AI Search, Azure SQL, DevOps REST APIs, etc.).
    This is the key signal for distinguishing an outbound dependency
    failure (→ incident / DependencyFailure) from an inbound caller
    error (→ invalid_api_call).
    """
    if not settings.appinsights_app_id:
        logger.warning(
            "App Insights not configured — skipping dependencies query")
        return {}
    query = (
        "dependencies"
        "| where timestamp > ago(1h)"
        "| where success == false"
        "| project timestamp, name, target, type, resultCode, duration,"
        "  data, operation_Id, operation_Name, cloud_RoleName"
        "| order by timestamp desc"
        "| take 20"
    )
    logger.info("Querying App Insights failed outbound dependencies")
    return _ai_get(settings, query)


def query_app_insights(settings: Settings, kusto_query: str | None = None) -> dict:
    """
    Execute a custom KQL query against Application Insights.
    Used when a specific query is needed beyond the standard collectors.
    """
    if not settings.appinsights_app_id:
        logger.warning("App Insights not configured — skipping query")
        return {}
    default_query = (
        "union exceptions, requests"
        "| where timestamp > ago(1h)"
        "| where success == false or itemType == 'exception'"
        "| summarize count() by type, resultCode, bin(timestamp, 5m)"
        "| order by timestamp desc"
        "| take 50"
    )
    return _ai_get(settings, kusto_query or default_query)


# ── Convenience: collect all telemetry ───────────────────────────────────

def collect_telemetry(settings: Settings, resource_id: str) -> dict[str, Any]:
    """
    Gather full telemetry for the given resource:
    - Azure Monitor alerts and metrics
    - App Insights exceptions (with stack traces and error messages)
    - App Insights failed requests (with HTTP status codes and input context)
    - App Insights warning/error traces (surrounding log context)
    """
    return {
        "alerts": query_alerts(settings, resource_id),
        "metrics": query_metrics(settings, resource_id),
        "exceptions": query_exceptions(settings),
        "failed_requests": query_failed_requests(settings),
        "traces": query_traces(settings),
        "dependencies": query_dependencies(settings),
    }

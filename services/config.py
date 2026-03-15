"""
Configuration
=============
Centralised configuration loading and validation.
All runtime values come from environment variables / App Settings.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_REQUIRED_VARS = [
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_CHAT_DEPLOYMENT",
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
    "AZURE_SEARCH_ENDPOINT",
    "AZURE_SEARCH_INDEX_NAME",
    "SQL_CONNECTION_STRING",
]


@dataclass(frozen=True)
class Settings:
    """Immutable application settings populated from environment."""

    # Azure OpenAI
    azure_openai_endpoint: str = ""
    azure_openai_chat_deployment: str = ""
    azure_openai_embedding_deployment: str = ""
    azure_openai_api_version: str = "2024-12-01-preview"

    # Azure AI Search
    azure_search_endpoint: str = ""
    azure_search_index_name: str = "incidents"

    # Azure identity (used by DefaultAzureCredential & REST calls)
    azure_subscription_id: str = ""

    # Azure Monitor / App Insights
    monitored_resource_id: str = ""
    appinsights_app_id: str = ""

    # Azure DevOps
    azdo_org: str = ""
    azdo_project: str = ""
    azdo_repo: str = ""
    azdo_pat: str = ""
    azdo_branch: str = "main"

    # Azure SQL
    sql_connection_string: str = ""

    # Orchestration tunables
    max_agent_iterations: int = 10
    session_timeout_seconds: int = 300

    # Dedup window
    dedup_window_minutes: int = 60

    missing_vars: tuple[str, ...] = field(default_factory=tuple)


def load_settings() -> Settings:
    """Load settings from environment variables and validate required ones."""

    def _env(name: str, default: str = "") -> str:
        return os.environ.get(name, default)

    missing = [v for v in _REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        logger.warning(
            "Missing required environment variables: %s — some features will be unavailable",
            ", ".join(missing),
        )

    settings = Settings(
        azure_openai_endpoint=_env("AZURE_OPENAI_ENDPOINT"),
        azure_openai_chat_deployment=_env("AZURE_OPENAI_CHAT_DEPLOYMENT"),
        azure_openai_embedding_deployment=_env(
            "AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
        azure_openai_api_version=_env(
            "AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
        azure_search_endpoint=_env("AZURE_SEARCH_ENDPOINT"),
        azure_search_index_name=_env("AZURE_SEARCH_INDEX_NAME", "incidents"),
        azure_subscription_id=_env("AZURE_SUBSCRIPTION_ID"),
        monitored_resource_id=_env("MONITORED_RESOURCE_ID"),
        appinsights_app_id=_env("APPINSIGHTS_APP_ID"),
        azdo_org=_env("AZDO_ORG"),
        azdo_project=_env("AZDO_PROJECT"),
        azdo_repo=_env("AZDO_REPO"),
        azdo_pat=_env("AZDO_PAT"),
        azdo_branch=_env("AZDO_BRANCH", "main"),
        sql_connection_string=_env("SQL_CONNECTION_STRING"),
        max_agent_iterations=int(_env("MAX_AGENT_ITERATIONS", "10")),
        session_timeout_seconds=int(_env("SESSION_TIMEOUT_SECONDS", "300")),
        dedup_window_minutes=int(_env("DEDUP_WINDOW_MINUTES", "60")),
        missing_vars=tuple(missing),
    )

    masked_sql = re.sub(r'(Pwd\s*=)[^;]+', r'\1***',
                        settings.sql_connection_string, flags=re.IGNORECASE)
    logger.info(
        "Settings loaded — openai_endpoint=%s chat_deployment=%s embedding_deployment=%s "
        "search_endpoint=%s search_index=%s appinsights_app_id=%s "
        "subscription_id=%s sql=%s",
        settings.azure_openai_endpoint,
        settings.azure_openai_chat_deployment,
        settings.azure_openai_embedding_deployment,
        settings.azure_search_endpoint,
        settings.azure_search_index_name,
        settings.appinsights_app_id,
        settings.azure_subscription_id,
        masked_sql if masked_sql else "<not set>",
    )
    return settings

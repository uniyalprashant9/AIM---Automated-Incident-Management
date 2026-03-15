"""
Azure AI Search Client
======================
Hybrid (keyword + vector) search against the incidents search index.
"""

from __future__ import annotations

import logging

from azure.identity import DefaultAzureCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery

from services.config import Settings
from services import azure_openai

logger = logging.getLogger(__name__)

# Shared credential — instantiated once to avoid repeated SDK probe messages
# (e.g. "No environment configuration found.") on every call.
_credential = DefaultAzureCredential()


def _build_client(settings: Settings) -> SearchClient:
    logger.debug("Building SearchClient — endpoint=%s index=%s",
                 settings.azure_search_endpoint, settings.azure_search_index_name)
    return SearchClient(
        endpoint=settings.azure_search_endpoint,
        index_name=settings.azure_search_index_name,
        credential=_credential,
    )


def search_similar_incidents(
    settings: Settings,
    query_text: str,
    *,
    top: int = 5,
) -> list[dict]:
    """
    Hybrid search: keyword + vector against the Azure AI Search index.

    Returns a list of result dicts with score, incident_id, summary, root_cause,
    remediation, severity, etc.
    """
    if not settings.azure_search_endpoint:
        logger.warning(
            "Azure AI Search not configured (AZURE_SEARCH_ENDPOINT is empty) — skipping retrieval")
        return []

    try:
        embedding = azure_openai.get_embedding(settings, query_text)
    except Exception as exc:
        logger.exception(
            "Failed to generate embedding — falling back to keyword-only search | "
            "endpoint=%s deployment=%s error_type=%s",
            settings.azure_openai_endpoint, settings.azure_openai_embedding_deployment,
            type(exc).__name__)
        embedding = None

    client = _build_client(settings)

    # Select only fields that exist in the index schema.
    # The index is a chunked document store: chunk_id, parent_id, chunk,
    # text_vector, id, incident_id.
    search_kwargs: dict = {
        "search_text": query_text,
        "top": top,
        "select": [
            "id",
            "incident_id",
            "chunk_id",
            "parent_id",
            "chunk",
        ],
    }

    if embedding:
        search_kwargs["vector_queries"] = [
            VectorizedQuery(
                vector=embedding,
                k_nearest_neighbors=top,
                fields="text_vector",
            )
        ]

    logger.debug("Azure AI Search query — endpoint=%s index=%s text=%.100s vector=%s top=%d",
                 settings.azure_search_endpoint, settings.azure_search_index_name,
                 query_text, "yes" if embedding else "no", top)
    try:
        results = client.search(**search_kwargs)
        hits: list[dict] = []
        for result in results:
            hit = {k: result.get(k) for k in search_kwargs["select"]}
            hit["search_score"] = result.get("@search.score", 0)
            # Expose chunk text under a stable key for downstream agents
            hit["content"] = hit.get("chunk", "")
            hits.append(hit)
        logger.info("Azure AI Search returned %d hits | endpoint=%s index=%s",
                    len(hits), settings.azure_search_endpoint, settings.azure_search_index_name)
        return hits
    except Exception as exc:
        logger.exception("Azure AI Search query failed | endpoint=%s index=%s error_type=%s | %s",
                         settings.azure_search_endpoint, settings.azure_search_index_name,
                         type(exc).__name__, exc)
        return []

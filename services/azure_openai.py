"""
Azure OpenAI Client
===================
Wraps the Azure OpenAI SDK for chat completions and embeddings.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from openai import AzureOpenAI

from services.config import Settings

logger = logging.getLogger(__name__)

# Shared credential — instantiated once to avoid repeated SDK probe messages
# (e.g. "No environment configuration found.") on every call.
_credential = DefaultAzureCredential()


def _build_client(settings: Settings) -> AzureOpenAI:
    logger.debug("Building AzureOpenAI client — endpoint=%s api_version=%s",
                 settings.azure_openai_endpoint, settings.azure_openai_api_version)
    token_provider = get_bearer_token_provider(
        _credential, "https://cognitiveservices.azure.com/.default"
    )
    return AzureOpenAI(
        azure_endpoint=settings.azure_openai_endpoint,
        azure_ad_token_provider=token_provider,
        api_version=settings.azure_openai_api_version,
    )


def chat_completion(
    settings: Settings,
    messages: list[dict[str, str]],
    *,
    temperature: float = 0.2,
    max_tokens: int = 4096,
    response_format: dict | None = None,
) -> str:
    """Send a chat completion request and return the assistant message content."""
    client = _build_client(settings)
    kwargs: dict[str, Any] = {
        "model": settings.azure_openai_chat_deployment,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_format:
        kwargs["response_format"] = response_format

    logger.debug("chat_completion — endpoint=%s deployment=%s messages=%d max_tokens=%d",
                 settings.azure_openai_endpoint, settings.azure_openai_chat_deployment,
                 len(messages), max_tokens)
    try:
        response = client.chat.completions.create(**kwargs)
        content = response.choices[0].message.content or ""
        logger.debug("chat_completion succeeded — deployment=%s finish_reason=%s tokens_used=%s",
                     settings.azure_openai_chat_deployment,
                     response.choices[0].finish_reason,
                     response.usage.total_tokens if response.usage else "n/a")
        return content
    except Exception as exc:
        logger.error("chat_completion failed — endpoint=%s deployment=%s error_type=%s | %s",
                     settings.azure_openai_endpoint, settings.azure_openai_chat_deployment,
                     type(exc).__name__, exc)
        raise


def chat_completion_json(
    settings: Settings,
    messages: list[dict[str, str]],
    **kwargs: Any,
) -> dict:
    """Chat completion that parses the response as JSON."""
    raw = chat_completion(
        settings,
        messages,
        response_format={"type": "json_object"},
        **kwargs,
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.error("Failed to parse chat response as JSON: %.500s", raw)
        return {"error": "invalid_json", "raw": raw[:2000]}


def get_embedding(settings: Settings, text: str) -> list[float]:
    """Generate a vector embedding for the given text."""
    client = _build_client(settings)
    logger.debug("get_embedding — endpoint=%s deployment=%s text_len=%d",
                 settings.azure_openai_endpoint,
                 settings.azure_openai_embedding_deployment, len(text))
    try:
        response = client.embeddings.create(
            model=settings.azure_openai_embedding_deployment,
            input=text,
        )
        logger.debug("get_embedding succeeded — deployment=%s vector_dims=%d",
                     settings.azure_openai_embedding_deployment,
                     len(response.data[0].embedding))
        return response.data[0].embedding
    except Exception as exc:
        logger.error("get_embedding failed — endpoint=%s deployment=%s error_type=%s | %s",
                     settings.azure_openai_endpoint,
                     settings.azure_openai_embedding_deployment,
                     type(exc).__name__, exc)
        raise

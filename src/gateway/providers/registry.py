"""
Provider registry — constructs provider instances from config.

Only import concrete providers here. Core/pipeline code imports Provider (ABC)
and calls build_provider(); it never imports a concrete class directly.
"""
from __future__ import annotations

from typing import Optional

import httpx

from gateway.config import Settings
from gateway.providers.base import Provider
from gateway.providers.openai_compat import OpenAICompatProvider


def build_provider(
    settings: Settings,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> Provider:
    """
    Instantiate and return the provider named in settings.provider_name.

    Args:
        settings: Application settings.
        client:   Optional pre-built httpx.AsyncClient (for testing or reuse mode).

    Raises:
        ValueError: If the provider name is not registered.
    """
    name = settings.provider_name.lower()

    # Both "mock" and "openai" resolve to the same HTTP implementation;
    # the difference is the base_url / api_key in settings.
    openai_compat_names = {"mock", "openai", "openai_compat", "azure"}

    if name in openai_compat_names:
        return OpenAICompatProvider(settings, client=client)

    raise ValueError(
        f"Unknown provider {name!r}. "
        f"Registered names: {sorted(openai_compat_names)}"
    )

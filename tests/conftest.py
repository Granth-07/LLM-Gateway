"""
Shared pytest fixtures.
"""
from __future__ import annotations

import pytest
from gateway.config import Settings


@pytest.fixture()
def base_settings() -> Settings:
    """Default settings — no env file, mock-compatible."""
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
    )

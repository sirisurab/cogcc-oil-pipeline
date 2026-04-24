"""Pytest configuration — register custom marks."""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require network access or data files on disk",
    )
    config.addinivalue_line(
        "markers",
        "unit: marks tests that require neither network access nor data files on disk",
    )

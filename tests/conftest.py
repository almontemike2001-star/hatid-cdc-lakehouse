from __future__ import annotations

import socket

import pytest

from hatid.config import Settings

STACK_PORTS = {"postgres": 5435, "clickhouse": 8123, "debezium": 8083}


def _stack_up() -> bool:
    for port in STACK_PORTS.values():
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        except OSError:
            return False
    return True


def pytest_collection_modifyitems(config, items):
    if _stack_up():
        return
    skip = pytest.mark.skip(reason="docker compose stack not running (make up)")
    for item in items:
        if "stack" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()

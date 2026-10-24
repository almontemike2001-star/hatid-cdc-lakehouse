"""Connection settings, overridable with environment variables (defaults match docker-compose)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"
CONNECTOR_CONFIG = PROJECT_ROOT / "infra" / "connect" / "hatid-postgres.json"


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    connect_url: str
    kafka_broker_internal: str  # as seen from inside ClickHouse's container

    @classmethod
    def from_env(cls) -> Settings:
        e = os.environ.get
        return cls(
            pg_dsn=e("HATID_PG_DSN", "postgresql://hatid:hatid@localhost:5435/hatid"),
            ch_host=e("HATID_CH_HOST", "localhost"),
            ch_port=int(e("HATID_CH_PORT", "8123")),
            ch_user=e("HATID_CH_USER", "hatid"),
            ch_password=e("HATID_CH_PASSWORD", "hatid"),
            connect_url=e("HATID_CONNECT_URL", "http://localhost:8083"),
            kafka_broker_internal=e("HATID_KAFKA_INTERNAL", "redpanda:9092"),
        )

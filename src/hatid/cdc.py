"""Control plane for the CDC pipeline: connector registration, ClickHouse bronze bootstrap,
schema evolution and Postgres <-> ClickHouse reconciliation."""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

import clickhouse_connect
import psycopg

from hatid import schema
from hatid.config import CONNECTOR_CONFIG, Settings

log = logging.getLogger(__name__)


def ch_client(s: Settings):
    return clickhouse_connect.get_client(
        host=s.ch_host, port=s.ch_port, username=s.ch_user, password=s.ch_password
    )


def _http(method: str, url: str, body: dict | None = None):
    req = urllib.request.Request(
        url, method=method, data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )  # fmt: skip
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


# --------------------------------------------------------------------------- connector


def register_connector(s: Settings) -> str:
    """Create or update the Debezium connector (idempotent) and wait until it is RUNNING."""
    spec = json.loads(CONNECTOR_CONFIG.read_text())
    name = spec["name"]
    status, body = _http("PUT", f"{s.connect_url}/connectors/{name}/config", spec["config"])
    if status not in (200, 201):
        raise RuntimeError(f"connector config rejected ({status}): {body}")
    for _ in range(60):
        status, body = _http("GET", f"{s.connect_url}/connectors/{name}/status")
        tasks = (body or {}).get("tasks", [])
        if status == 200 and tasks and all(t["state"] == "RUNNING" for t in tasks):
            return "RUNNING"
        if any(t.get("state") == "FAILED" for t in tasks):
            raise RuntimeError(f"connector task failed: {tasks[0].get('trace', '')[:800]}")
        time.sleep(2)
    raise TimeoutError("connector did not reach RUNNING")


# --------------------------------------------------------------------------- clickhouse


def _source_has_tip(pg_dsn: str) -> bool:
    with psycopg.connect(pg_dsn) as conn:
        return conn.execute(
            "select 1 from information_schema.columns where table_schema='app'"
            " and table_name='orders' and column_name='tip_amount'"
        ).fetchone() is not None  # fmt: skip


def bootstrap_clickhouse(s: Settings) -> int:
    client = ch_client(s)
    statements = schema.bootstrap_sql(s.kafka_broker_internal)
    for sql in statements:
        client.command(sql)
    if _source_has_tip(s.pg_dsn):  # re-bootstrapping after the schema change
        for sql in schema.add_column_sql(schema.table("orders"), schema.TIP_COLUMN):
            client.command(sql)
    return len(statements)


def migrate_add_tips(s: Settings) -> None:
    """Additive schema change, applied while data is flowing: ClickHouse first (so the new
    field is captured from its first appearance), then the source table."""
    from hatid.simulator import enable_tips

    client = ch_client(s)
    for sql in schema.add_column_sql(schema.table("orders"), schema.TIP_COLUMN):
        client.command(sql)
    with psycopg.connect(s.pg_dsn) as conn:
        enable_tips(conn)


# --------------------------------------------------------------------------- reconciliation


@dataclass(frozen=True)
class TableCheck:
    table: str
    postgres_rows: int
    postgres_checksum: int
    clickhouse_rows: int
    clickhouse_checksum: int

    @property
    def ok(self) -> bool:
        return (self.postgres_rows, self.postgres_checksum) == (
            self.clickhouse_rows, self.clickhouse_checksum,
        )  # fmt: skip


def _pg_stats(conn: psycopg.Connection, t: schema.Table) -> tuple[int, int]:
    # Row count plus a checksum over key and last update time catches missed updates and
    # deletes, not just missing rows.
    row = conn.execute(
        f"select count(*), coalesce(sum({t.key} * 31 + floor(extract(epoch from updated_at))::bigint), 0)"
        f" from app.{t.name}"
    ).fetchone()  # fmt: skip
    return int(row[0]), int(row[1])


def _ch_stats(client, t: schema.Table) -> tuple[int, int]:
    row = client.query(
        f"select count(), sum({t.key} * 31 + toUnixTimestamp(updated_at))"
        f" from {schema.BRONZE}.{t.name} final where _deleted = 0"
    ).result_rows[0]  # fmt: skip
    return int(row[0]), int(row[1] or 0)


def reconcile(s: Settings) -> list[TableCheck]:
    client = ch_client(s)
    with psycopg.connect(s.pg_dsn) as conn:
        return [TableCheck(t.name, *_pg_stats(conn, t), *_ch_stats(client, t)) for t in schema.TABLES]


def wait_until_replicated(s: Settings, timeout_s: float = 180, poll_s: float = 2) -> list[TableCheck]:
    """Poll until every table matches between Postgres and ClickHouse (or time out)."""
    deadline = time.monotonic() + timeout_s
    while True:
        checks = reconcile(s)
        if all(c.ok for c in checks) or time.monotonic() > deadline:
            return checks
        time.sleep(poll_s)


def replication_lag_seconds(s: Settings) -> dict[str, float]:
    """p95 of (ClickHouse ingest time - Postgres commit time) over the last hour, per table."""
    client = ch_client(s)
    lag = {}
    for t in schema.TABLES:
        row = client.query(
            f"select quantile(0.95)(dateDiff('millisecond', _source_ts, _ingested_at)) / 1000"
            f" from {schema.BRONZE}.{t.name}_changelog"
            f" where _op != 'r' and _ingested_at > now() - interval 1 hour"
        ).result_rows[0]  # fmt: skip
        lag[t.name] = float(row[0]) if row[0] == row[0] else 0.0  # NaN when no recent changes
    return lag

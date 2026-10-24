"""Dagster definitions: app activity -> CDC bronze tables -> dbt models -> KPI report.

    uv run dagster dev        # UI on http://localhost:3003 (see Makefile)

The bronze tables are filled continuously by Debezium and ClickHouse, not by Dagster. The
``bronze_cdc`` asset therefore does not copy data: it waits until ClickHouse has caught up with
Postgres and records the result as asset checks, so dbt only runs on a consistent snapshot.
"""

# No `from __future__ import annotations`: Dagster resolves Config type hints at runtime.
import subprocess
import sys
from pathlib import Path

import psycopg
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Config,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

from hatid import cdc, schema, simulator
from hatid.config import DBT_DIR, PROJECT_ROOT, Settings

LAG_WARN_SECONDS = 30

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()
if not Path(dbt_project.manifest_path).exists():  # e.g. tests or `dagster definitions validate`
    dbt = Path(sys.executable).with_name("dbt")
    subprocess.run([str(dbt), "parse", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
                   check=True, capture_output=True)  # fmt: skip


class ActivityConfig(Config):
    hours: float = 6
    orders_per_day: int = 1500


@asset(group_name="source_app", compute_kind="postgres")
def app_activity(context: AssetExecutionContext, config: ActivityConfig) -> MaterializeResult:
    """Advance the simulated delivery app: orders, status updates, sign-ups, deletions."""
    s = Settings.from_env()
    with psycopg.connect(s.pg_dsn) as conn:
        simulator.seed(conn)
        st = simulator.simulate(conn, hours=config.hours, orders_per_day=config.orders_per_day)
    return MaterializeResult(metadata={
        "window": f"{st.start:%Y-%m-%d %H:%M} -> {st.end:%Y-%m-%d %H:%M} UTC",
        "changes_applied": st.applied,
        "orders_placed": st.orders_placed,
        "events_pending": st.pending,
    })  # fmt: skip


BRONZE_KEYS = [AssetKey(["bronze", name]) for t in schema.TABLES
               for name in (t.name, f"{t.name}_changelog")]  # fmt: skip
CURRENT_KEYS = {t.name: AssetKey(["bronze", t.name]) for t in schema.TABLES}


@multi_asset(
    specs=[
        AssetSpec(k, deps=[app_activity], group_name="bronze_cdc", kinds={"clickhouse"}) for k in BRONZE_KEYS
    ],  # fmt: skip
    check_specs=[
        spec
        for key in CURRENT_KEYS.values()
        for spec in (
            AssetCheckSpec(
                "matches_postgres",
                asset=key,
                description="Row count and update checksum equal Postgres",
            ),
            AssetCheckSpec(
                "replication_lag",
                asset=key,
                description=f"p95 commit-to-ingest lag under {LAG_WARN_SECONDS}s",
            ),
        )
    ],  # fmt: skip
)
def bronze_cdc(context: AssetExecutionContext):
    """Wait for Debezium + ClickHouse to catch up with Postgres, then verify every table."""
    s = Settings.from_env()
    checks = {c.table: c for c in cdc.wait_until_replicated(s, timeout_s=300)}
    lag = cdc.replication_lag_seconds(s)
    for key in BRONZE_KEYS:
        table = key.path[-1].removesuffix("_changelog")
        c = checks[table]
        yield MaterializeResult(asset_key=key, metadata={
            "postgres_rows": c.postgres_rows, "clickhouse_rows": c.clickhouse_rows,
        })  # fmt: skip
    for table, key in CURRENT_KEYS.items():
        c = checks[table]
        yield AssetCheckResult(
            asset_key=key, check_name="matches_postgres", passed=c.ok,
            metadata={"postgres_rows": c.postgres_rows, "clickhouse_rows": c.clickhouse_rows},
        )  # fmt: skip
        yield AssetCheckResult(
            asset_key=key, check_name="replication_lag", passed=lag[table] <= LAG_WARN_SECONDS,
            severity=AssetCheckSeverity.WARN,
            metadata={"p95_lag_seconds": MetadataValue.float(lag[table])},
        )  # fmt: skip


class LayerTranslator(DagsterDbtTranslator):
    """Group dbt models by medallion layer (their target database: silver / gold)."""

    def get_group_name(self, dbt_resource_props) -> str | None:
        if dbt_resource_props["resource_type"] == "model":
            return dbt_resource_props["config"].get("schema") or "dbt"
        return super().get_group_name(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=LayerTranslator())
def hatid_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    deps=[get_asset_key_for_model([hatid_dbt], m) for m in
          ("agg_daily_city", "agg_rider_daily", "agg_restaurant_performance", "cdc_table_health")],
    group_name="reporting", compute_kind="html",
)  # fmt: skip
def kpi_report(context: AssetExecutionContext) -> MaterializeResult:
    """Static KPI report published to GitHub Pages (docs/kpi_report.html)."""
    from hatid.report import render

    out = render(Settings.from_env(), PROJECT_ROOT / "reports" / "kpi_report.html")
    return MaterializeResult(metadata={"path": str(out)})


pipeline_job = define_asset_job("hatid_pipeline", selection=AssetSelection.all())

defs = Definitions(
    assets=[app_activity, bronze_cdc, hatid_dbt, kpi_report],
    jobs=[pipeline_job],
    schedules=[
        ScheduleDefinition(
            job=pipeline_job,
            cron_schedule="*/30 * * * *",
            default_status=DefaultScheduleStatus.STOPPED,
        )
    ],  # fmt: skip
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)

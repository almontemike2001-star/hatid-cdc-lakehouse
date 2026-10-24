"""Integration tests against the running docker compose stack (marked `stack`)."""

import psycopg
import pytest

from hatid import cdc, schema, simulator

pytestmark = pytest.mark.stack


def _wait(settings):
    checks = cdc.wait_until_replicated(settings, timeout_s=120)
    assert all(c.ok for c in checks), [c for c in checks if not c.ok]
    return checks


def test_new_activity_replicates_exactly(settings):
    with psycopg.connect(settings.pg_dsn) as conn:
        simulator.seed(conn)
        stats = simulator.simulate(conn, hours=3)
    assert stats.applied > 0
    _wait(settings)


def test_a_delete_in_postgres_removes_the_row_from_current_state(settings):
    with psycopg.connect(settings.pg_dsn) as conn:
        cid = conn.execute("select max(customer_id) from app.customers").fetchone()[0]
        conn.execute("delete from app.customers where customer_id = %s", (cid,))
        conn.commit()
    _wait(settings)
    ch = cdc.ch_client(settings)
    assert (
        ch.query(
            f"select count() from bronze.customers final where customer_id = {cid} and _deleted = 0"
        ).result_rows[0][0]
        == 0
    )
    ops = [
        r[0]
        for r in ch.query(
            f"select _op from bronze.customers_changelog where customer_id = {cid} order by _lsn"
        ).result_rows
    ]
    assert ops[-1] == "d"


def test_an_update_creates_a_new_version_and_wins_in_current_state(settings):
    with psycopg.connect(settings.pg_dsn) as conn:
        conn.execute("update app.restaurants set rating = 3.1, updated_at = now() where restaurant_id = 1")
        conn.commit()
    _wait(settings)
    ch = cdc.ch_client(settings)
    current = ch.query("select rating from bronze.restaurants final where restaurant_id = 1").result_rows[0][
        0
    ]
    assert float(current) == 3.1
    versions = ch.query(
        "select count() from bronze.restaurants_changelog where restaurant_id = 1"
    ).result_rows[0][0]
    assert versions >= 2


def test_bronze_tables_exist_for_every_source_table(settings):
    ch = cdc.ch_client(settings)
    existing = {
        r[0] for r in ch.query("select name from system.tables where database = 'bronze'").result_rows
    }
    for t in schema.TABLES:
        assert {t.name, f"{t.name}_changelog", f"kafka_{t.name}"} <= existing


def test_dagster_runs_the_whole_asset_graph(settings):
    from dagster import materialize
    from hatid_dagster import definitions as d

    result = materialize(
        [d.app_activity, d.bronze_cdc, d.hatid_dbt, d.kpi_report],
        resources={"dbt": d.defs.resources["dbt"]},
        run_config={"ops": {"app_activity": {"config": {"hours": 2}}}},
    )
    assert result.success
    checks = result.get_asset_check_evaluations()
    matches = [c for c in checks if c.check_name == "matches_postgres"]
    assert len(matches) == len(schema.TABLES) and all(c.passed for c in matches)

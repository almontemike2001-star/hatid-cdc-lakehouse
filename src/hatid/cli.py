"""Command line entry point: ``hatid <command>``."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import psycopg

from hatid import cdc, simulator
from hatid.config import DBT_DIR, PROJECT_ROOT, Settings

log = logging.getLogger("hatid")


def run_dbt(args: list[str]) -> None:
    dbt = Path(sys.executable).with_name("dbt")
    cmd = [str(dbt), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, check=False).returncode != 0:
        raise SystemExit(f"dbt {' '.join(args)} failed")


def print_checks(checks: list[cdc.TableCheck]) -> bool:
    for c in checks:
        mark = "OK " if c.ok else "DIFF"
        print(f"  {mark} {c.table:<12} postgres={c.postgres_rows:>7}  clickhouse={c.clickhouse_rows:>7}")
    return all(c.ok for c in checks)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="hatid", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="seed Postgres, create ClickHouse bronze, register Debezium")
    sim = sub.add_parser("simulate", help="advance the simulated app by N hours")
    sim.add_argument("--hours", type=float, default=6)
    sim.add_argument("--orders-per-day", type=int, default=1500)
    sub.add_parser("migrate-tips", help="live schema change: add orders.tip_amount")
    sub.add_parser("reconcile", help="compare Postgres and ClickHouse table by table")
    tr = sub.add_parser("transform", help="run dbt (default: build)")
    tr.add_argument("dbt_args", nargs="*", default=["build"])
    rep = sub.add_parser("report", help="render the static KPI report")
    rep.add_argument("--out", default=str(PROJECT_ROOT / "reports" / "kpi_report.html"))
    demo = sub.add_parser("demo", help="setup + 3 days of activity with a mid-run schema change")
    demo.add_argument("--days", type=int, default=3)
    demo.add_argument("--skip-dbt", action="store_true", help="stop after replication (CI slim builds)")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))
    s = Settings.from_env()

    if args.command in {"setup", "demo"}:
        with psycopg.connect(s.pg_dsn) as conn:
            simulator.seed(conn)
        log.info("bronze: %d ClickHouse statements", cdc.bootstrap_clickhouse(s))
        log.info("debezium connector: %s", cdc.register_connector(s))
    if args.command == "simulate":
        with psycopg.connect(s.pg_dsn) as conn:
            st = simulator.simulate(conn, hours=args.hours, orders_per_day=args.orders_per_day)
        log.info("simulated %s -> %s: %d changes applied, %d orders placed, %d pending",
                 st.start, st.end, st.applied, st.orders_placed, st.pending)  # fmt: skip
    if args.command == "migrate-tips":
        cdc.migrate_add_tips(s)
        log.info("orders.tip_amount added in ClickHouse and Postgres")
    if args.command == "demo":
        with psycopg.connect(s.pg_dsn) as conn:
            for day in range(args.days):
                if day == 1:
                    cdc.migrate_add_tips(s)  # schema change while data is flowing
                    log.info("schema change applied: orders.tip_amount")
                for _ in range(4):
                    st = simulator.simulate(conn, hours=6)
                    log.info("day %d: %d changes, %d orders", day + 1, st.applied, st.orders_placed)
    if args.command in {"reconcile", "demo"}:
        ok = print_checks(cdc.wait_until_replicated(s))
        if not ok:
            raise SystemExit("Postgres and ClickHouse disagree")
    skip_dbt = getattr(args, "skip_dbt", False)
    if args.command == "transform" or (args.command == "demo" and not skip_dbt):
        run_dbt(getattr(args, "dbt_args", ["build"]))
    if args.command == "report" or (args.command == "demo" and not skip_dbt):
        from hatid.report import render

        out = render(s, Path(getattr(args, "out", PROJECT_ROOT / "reports" / "kpi_report.html")))
        log.info("report written to %s", out)


if __name__ == "__main__":
    main()

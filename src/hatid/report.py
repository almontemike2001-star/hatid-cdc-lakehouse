"""Render the static KPI report from the ClickHouse gold marts (plus a live reconciliation)."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from hatid.cdc import ch_client, reconcile
from hatid.config import Settings

QUERIES = {
    "kpis": """
        select count() as orders, countIf(status = 'delivered') as delivered,
               countIf(status = 'cancelled') / count() as cancellation_rate,
               sum(gmv) as gmv, sumIf(tip_amount, status = 'delivered') as tips,
               countIf(tip_amount is not null) as tipped_orders,
               avgIf(total_minutes, status = 'delivered') as avg_total_minutes,
               avgIf(toFloat64(is_on_time), status = 'delivered') as on_time_rate,
               countIf(is_anonymised) as anonymised_orders,
               min(order_date) as first_day, max(order_date) as last_day
        from gold.fct_orders
    """,
    "people": """
        select (select count() from gold.dim_customers) as customers,
               (select countIf(is_active) from gold.dim_riders) as active_riders,
               (select count() from gold.dim_restaurants) as restaurants
    """,
    "cdc": """
        select sum(change_events) as change_events, sum(inserts) as inserts,
               sum(updates) as updates, sum(deletes) as deletes,
               sum(snapshot_events) as snapshot_events, max(p95_lag_seconds) as worst_p95_lag
        from gold.cdc_table_health
    """,
    "cdc_tables": """
        select source_table, snapshot_events, inserts, updates, deletes, current_rows,
               p50_lag_seconds, p95_lag_seconds
        from gold.cdc_table_health order by change_events desc
    """,
    "hourly": """
        select order_hour as hour, count() as orders,
               avgIf(toFloat64(is_on_time), status = 'delivered') as on_time_rate,
               avgIf(prep_minutes, prep_minutes is not null) as prep_minutes
        from gold.fct_orders group by 1 order by 1
    """,
    "cities": """
        select city, sum(orders) as orders, sum(gmv) as gmv,
               sum(cancelled_orders) / sum(orders) as cancellation_rate,
               sumIf(on_time_rate * delivered_orders, on_time_rate is not null)
                 / sum(delivered_orders) as on_time_rate
        from gold.agg_daily_city group by 1 order by 3 desc
    """,
    "delivery_time": """
        select intDiv(toUInt32(total_minutes), 5) * 5 as bucket, count() as orders
        from gold.fct_orders where status = 'delivered' group by 1 order by 1
    """,
    "riders": """
        select rider_name, any(rider_city) as city, sum(deliveries) as deliveries,
               sumIf(on_time_rate * deliveries, on_time_rate is not null) / sum(deliveries) as on_time_rate,
               sum(km_delivered) as km
        from gold.agg_rider_daily group by rider_id, rider_name
        order by 3 desc limit 10
    """,
    "cancellations": """
        select cancel_reason as reason, count() as orders
        from gold.fct_orders where status = 'cancelled' group by 1 order by 2 desc
    """,
}


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, float) and value != value:  # NaN
        return None
    return value


def collect(s: Settings) -> dict:
    client = ch_client(s)
    data = {}
    for name, sql in QUERIES.items():
        # Aliases reuse column names (sum(orders) AS orders); let columns win when resolving.
        result = client.query(sql, settings={"prefer_column_name_to_alias": 1})
        data[name] = [
            {c: _jsonable(v) for c, v in zip(result.column_names, row, strict=True)}
            for row in result.result_rows
        ]
    data["reconciliation"] = [asdict(c) | {"ok": c.ok} for c in reconcile(s)]
    return data


def render(s: Settings, out: Path) -> Path:
    template = (Path(__file__).parent / "report_template.html").read_text()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(template.replace("__DATA__", json.dumps(collect(s))))
    return out

"""Build the Hatid Metabase dashboards through the Metabase API (dashboards as code).

Idempotent: on every run it (1) completes first-time setup or logs in, (2) registers the
ClickHouse gold database, (3) archives the previous Hatid questions/dashboards and recreates
them, and (4) enables public links so the dashboards can be screenshotted.

    make dashboards        # admin login from bi/metabase/.env, local defaults otherwise
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
MB_URL = os.environ.get("MB_URL", "http://localhost:3002")
COLLECTION = "Hatid"
DB_NAME = "Hatid ClickHouse (gold)"

# Palette from the report (validated for colour-vision deficiency, light mode).
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
NO_OTHER = {"graph.max_categories_enabled": True, "graph.max_categories": 50}

# --------------------------------------------------------------------------- http


def _env() -> dict[str, str]:
    env = {}
    env_file = HERE / ".env"
    for line in env_file.read_text().splitlines() if env_file.exists() else []:
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return {**env, **os.environ}


class Metabase:
    def __init__(self, url: str):
        self.url = url
        self.session: str | None = None

    def call(self, method: str, path: str, body: dict | None = None):
        req = urllib.request.Request(
            f"{self.url}/api{path}",
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"}
            | ({"X-Metabase-Session": self.session} if self.session else {}),
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> {exc.code}: {exc.read()[:500]!r}") from exc
        return json.loads(raw) if raw else None

    get = lambda self, p: self.call("GET", p)  # noqa: E731
    post = lambda self, p, b=None: self.call("POST", p, b or {})  # noqa: E731
    put = lambda self, p, b: self.call("PUT", p, b)  # noqa: E731


def connect(env: dict[str, str]) -> Metabase:
    mb = Metabase(MB_URL)
    for _ in range(60):
        try:
            if mb.get("/health")["status"] == "ok":
                break
        except Exception:
            pass
        time.sleep(5)
    props = mb.get("/session/properties")
    email = env.get("MB_ADMIN_EMAIL", "admin@hatid.local")
    password = env.get("MB_ADMIN_PASSWORD", "Hatid-local-2026!")
    if props.get("setup-token") and not props.get("has-user-setup"):
        result = mb.post(
            "/setup",
            {
                "token": props["setup-token"],
                "user": {
                    "email": email,
                    "password": password,
                    "first_name": "Hatid",
                    "last_name": "Admin",
                    "site_name": "Hatid",
                },
                "prefs": {"site_name": "Hatid", "allow_tracking": False},
            },
        )
        mb.session = result["id"]
    else:
        mb.session = mb.post("/session", {"username": email, "password": password})["id"]
    return mb


# --------------------------------------------------------------------------- setup


def ensure_database(mb: Metabase, env: dict[str, str]) -> int:
    existing = mb.get("/database")
    existing = existing.get("data", existing)
    for db in existing:
        if db["name"] == DB_NAME:
            mb.post(f"/database/{db['id']}/sync_schema")
            return db["id"]
    db = mb.post(
        "/database",
        {
            "engine": "clickhouse",
            "name": DB_NAME,
            "details": {
                "host": "clickhouse",
                "port": 8123,
                "dbname": "gold",
                "user": "hatid",
                "password": "hatid",
                "ssl": False,
            },
        },
    )
    return db["id"]


def field_ids(mb: Metabase, db_id: int, expected_tables: set[str]) -> dict[tuple[str, str], int]:
    for _ in range(60):
        meta = mb.get(f"/database/{db_id}/metadata")
        fields = {
            (t["name"], f["name"]): f["id"]
            for t in meta["tables"]
            if t.get("schema") in ("gold", None)
            for f in t["fields"]
        }
        if expected_tables <= {table for table, _ in fields}:
            return fields
        time.sleep(3)
    raise RuntimeError("Metabase did not finish syncing the gold schema")


def reset_collection(mb: Metabase) -> int:
    for coll in mb.get("/collection"):
        if coll.get("name") == COLLECTION and not coll.get("archived"):
            for item in mb.get(f"/collection/{coll['id']}/items")["data"]:
                kind = {"card": "card", "dataset": "card", "dashboard": "dashboard"}.get(item["model"])
                if kind:
                    mb.put(f"/{kind}/{item['id']}", {"archived": True})
            return coll["id"]
    return mb.post("/collection", {"name": COLLECTION, "color": BLUE})["id"]


# --------------------------------------------------------------------------- cards

PARAMS = {
    "date": {"name": "Order date", "slug": "date", "type": "date/all-options", "sectionId": "date"},
    "city": {"name": "City", "slug": "city", "type": "string/=", "sectionId": "string"},
    "cuisine": {"name": "Cuisine", "slug": "cuisine", "type": "string/=", "sectionId": "string"},
}


class Builder:
    def __init__(self, mb: Metabase, db_id: int, fields: dict, collection_id: int):
        self.mb, self.db_id, self.fields, self.collection_id = mb, db_id, fields, collection_id

    def card(self, name: str, sql: str, display: str, filters: dict[str, tuple], viz=None) -> dict:
        """filters maps a filter key (date/city/cuisine) to its (table, column)."""
        if display == "scalar":  # full numbers, not "₱2M"
            viz = {"scalar.compact_primary_number": False, **(viz or {})}
        tags = {
            key: {
                "id": f"{key}-{abs(hash((name, key))) % 10**8}",
                "name": key,
                "display-name": PARAMS[key]["name"],
                "type": "dimension",
                "dimension": ["field", self.fields[target], None],
                "widget-type": PARAMS[key]["type"],
            }
            for key, target in filters.items()
        }
        card = self.mb.post(
            "/card",
            {
                "name": name,
                "collection_id": self.collection_id,
                "display": display,
                "visualization_settings": viz or {},
                "dataset_query": {
                    "type": "native",
                    "database": self.db_id,
                    "native": {"query": sql.strip(), "template-tags": tags},
                },
            },
        )
        card["_filters"] = list(filters)
        return card

    def dashboard(self, name: str, description: str, params: list[str], layout: list) -> dict:
        dash = self.mb.post(
            "/dashboard",
            {"name": name, "description": description, "collection_id": self.collection_id},
        )
        parameters = [{"id": key, **PARAMS[key]} for key in params]
        dashcards = []
        for i, (card, row, col, sx, sy) in enumerate(layout):
            dashcards.append(
                {
                    "id": -(i + 1),
                    "card_id": card["id"],
                    "row": row,
                    "col": col,
                    "size_x": sx,
                    "size_y": sy,
                    "parameter_mappings": [
                        {
                            "parameter_id": key,
                            "card_id": card["id"],
                            "target": ["dimension", ["template-tag", key]],
                        }
                        for key in card["_filters"]
                        if key in params
                    ],
                }
            )
        self.mb.put(f"/dashboard/{dash['id']}", {"parameters": parameters, "dashcards": dashcards})
        uuid = self.mb.post(f"/dashboard/{dash['id']}/public_link")["uuid"]
        return {"id": dash["id"], "name": name, "public_url": f"{MB_URL}/public/dashboard/{uuid}"}


def money(col: str) -> dict:
    return {f'["name","{col}"]': {"prefix": "₱", "decimals": 0}}


def pct(col: str, decimals: int = 1) -> dict:
    return {f'["name","{col}"]': {"number_style": "percent", "decimals": decimals}}


def chart(x: str, ys: list[str], colors: list[str], x_title: str = "", y_title: str = "", **extra):
    return {
        "graph.dimensions": [x],
        "graph.metrics": ys,
        "graph.x_axis.title_text": x_title,
        "graph.y_axis.title_text": y_title,
        "series_settings": {y: {"color": c} for y, c in zip(ys, colors, strict=True)},
        **extra,
    }


def dec(col: str, decimals: int = 1) -> dict:
    return {f'["name","{col}"]': {"decimals": decimals}}


def build(b: Builder) -> list[dict]:
    fo = {"date": ("fct_orders", "order_date"), "city": ("fct_orders", "city"),
          "cuisine": ("fct_orders", "cuisine")}  # fmt: skip
    w = "where {{date}} and {{city}} and {{cuisine}}"
    src = f"from gold.fct_orders {w}"
    dlv = f"{src} and status = 'delivered'"

    # ---------------------------------------------------------------- operations
    ops = [
        b.card("Orders", f"select count() as order_count {src}", "scalar", fo),
        b.card(
            "GMV (delivered)",
            f"select sum(gmv) as gmv_total {src}",
            "scalar",
            fo,
            {
                "column_settings": {
                    '["name","gmv_total"]': {"prefix": "₱", "suffix": "M", "scale": 0.000001, "decimals": 2}
                }
            },
        ),
        b.card("Minutes to door", f"select round(avg(total_minutes), 1) as minutes {dlv}", "scalar", fo),
        b.card(
            "On-time rate",
            f"select avg(toFloat64(is_on_time)) as on_time {dlv}",
            "scalar",
            fo,
            {"column_settings": pct("on_time")},
        ),
        b.card(
            "Cancellation rate",
            f"select countIf(status = 'cancelled') / count() as cancel_rate {src}",
            "scalar",
            fo,
            {"column_settings": pct("cancel_rate")},
        ),
        b.card(
            "Tips",
            f"select sum(tip_amount) as tips_total {dlv}",
            "scalar",
            fo,
            {
                "column_settings": {
                    '["name","tips_total"]': {"prefix": "₱", "suffix": "k", "scale": 0.001, "decimals": 1}
                }
            },
        ),
        b.card(
            "Orders by local hour",
            f"select order_hour as hour, count() as order_count {src} group by hour order by hour",
            "bar",
            fo,
            chart("hour", ["order_count"], [BLUE], "Hour of day (Manila)", "Orders"),
        ),
        b.card(
            "On-time rate by local hour",
            f"select order_hour as hour, avg(toFloat64(is_on_time)) as on_time {dlv} group by hour order by hour",
            "line",
            fo,
            chart("hour", ["on_time"], [BLUE], "Hour of day (Manila)", "On time")
            | {
                "column_settings": pct("on_time"),
                "graph.y_axis.min": 0,
                "graph.y_axis.max": 1,
                "graph.y_axis.auto_range": False,
            },
        ),
        b.card(
            "Order-to-door time (minutes)",
            f"""select toString(intDiv(toUInt32(total_minutes), 5) * 5) as minutes_bucket,
                       intDiv(toUInt32(total_minutes), 5) as bucket_order, count() as order_count
                {dlv} group by minutes_bucket, bucket_order order by bucket_order""",
            "bar",
            fo,
            chart(
                "minutes_bucket",
                ["order_count"],
                [BLUE],
                "Minutes from order to door (5-min buckets)",
                "Orders",
            ),
        ),
        b.card(
            "GMV by city",
            f"select city as order_city, sum(gmv) as gmv_total {src} group by order_city order by gmv_total desc",
            "row",
            fo,
            chart("order_city", ["gmv_total"], [BLUE], **NO_OTHER) | {"column_settings": money("gmv_total")},
        ),
        b.card(
            "Cancellations by reason",
            f"""select initcap(replace(cancel_reason, '_', ' ')) as reason, count() as order_count
                {src} and status = 'cancelled' group by reason order by order_count desc""",
            "row",
            fo,
            chart("reason", ["order_count"], [BLUE], **NO_OTHER),
        ),
        b.card(
            "Orders by cuisine",
            f"select cuisine as cuisine_name, count() as order_count {src} group by cuisine_name order by order_count desc",
            "row",
            fo,
            chart("cuisine_name", ["order_count"], [BLUE], **NO_OTHER),
        ),
        b.card(
            "City scorecard",
            f"""select city as `City`, count() as `Orders`, countIf(status = 'delivered') as `Delivered`,
                       sum(gmv) as `GMV`, round(avgIf(total_minutes, status = 'delivered'), 1) as `Order-to-door (min)`,
                       avgIf(toFloat64(is_on_time), status = 'delivered') as `On time`,
                       countIf(status = 'cancelled') / count() as `Cancelled`,
                       sum(tip_amount) as `Tips`
                {src} group by `City` order by `GMV` desc""",
            "table",
            fo,
            {"column_settings": money("GMV") | money("Tips") | pct("On time") | pct("Cancelled")},
        ),
    ]
    operations = b.dashboard(
        "Delivery Operations",
        "Orders, GMV, delivery speed and on-time rate from the CDC-fed ClickHouse gold layer. Synthetic data.",
        ["date", "city", "cuisine"],
        [
            (ops[0], 0, 0, 4, 3), (ops[1], 0, 4, 4, 3), (ops[2], 0, 8, 4, 3),
            (ops[3], 0, 12, 4, 3), (ops[4], 0, 16, 4, 3), (ops[5], 0, 20, 4, 3),
            (ops[6], 3, 0, 12, 6), (ops[7], 3, 12, 12, 6),
            (ops[8], 9, 0, 12, 6), (ops[9], 9, 12, 12, 6),
            (ops[10], 15, 0, 12, 8), (ops[11], 15, 12, 12, 8),
            (ops[12], 23, 0, 24, 8),
        ],
    )  # fmt: skip

    # ---------------------------------------------------------------- riders & restaurants
    fr = {"date": ("agg_rider_daily", "order_date"), "city": ("agg_rider_daily", "rider_city")}
    rsrc = "from gold.agg_rider_daily where {{date}} and {{city}}"
    frs = {
        "city": ("agg_restaurant_performance", "city"),
        "cuisine": ("agg_restaurant_performance", "cuisine"),
    }
    ssrc = "from gold.agg_restaurant_performance where {{city}} and {{cuisine}}"
    people = [
        b.card(
            "Top 10 riders by deliveries",
            f"""select rider_name as rider, sum(deliveries) as delivery_count {rsrc}
                group by rider_id, rider order by delivery_count desc limit 10""",
            "row",
            fr,
            chart("rider", ["delivery_count"], [BLUE], **NO_OTHER),
        ),
        b.card(
            "Rider on-time rate by vehicle",
            f"""select initcap(vehicle) as vehicle_type,
                       sumIf(on_time_rate * deliveries, on_time_rate is not null) / sum(deliveries) as on_time
                {rsrc} group by vehicle_type order by on_time desc""",
            "bar",
            fr,
            chart("vehicle_type", ["on_time"], [BLUE], "Vehicle", "On time")
            | {"column_settings": pct("on_time")},
        ),
        b.card(
            "Slowest kitchens (avg prep minutes)",
            f"""select restaurant_name as restaurant, avg_prep_minutes as prep_minutes {ssrc}
                order by prep_minutes desc limit 10""",
            "row",
            frs,
            chart("restaurant", ["prep_minutes"], [BLUE], **NO_OTHER)
            | {"column_settings": dec("prep_minutes")},
        ),
        b.card(
            "Rejection rate by cuisine",
            f"""select cuisine as cuisine_name, sum(rejected_orders) / sum(orders) as rejection
                {ssrc} group by cuisine_name order by rejection desc""",
            "row",
            frs,
            chart("cuisine_name", ["rejection"], [BLUE], **NO_OTHER) | {"column_settings": pct("rejection")},
        ),
        b.card(
            "Rider scorecard",
            f"""select rider_name as `Rider`, any(rider_city) as `City`, initcap(any(vehicle)) as `Vehicle`,
                       sum(deliveries) as `Deliveries`, sum(failed_deliveries) as `Failed`,
                       round(sumIf(avg_delivery_minutes * deliveries, avg_delivery_minutes is not null)
                             / sum(deliveries), 1) as `Avg delivery (min)`,
                       sumIf(on_time_rate * deliveries, on_time_rate is not null) / sum(deliveries) as `On time`,
                       sum(km_delivered) as `km`
                {rsrc} group by rider_id, `Rider` order by `Deliveries` desc""",
            "table",
            fr,
            {"column_settings": pct("On time") | dec("km")},
        ),
        b.card(
            "Restaurant scorecard",
            f"""select restaurant_name as `Restaurant`, cuisine as `Cuisine`, city as `City`,
                       orders as `Orders`, gmv as `GMV`, avg_prep_minutes as `Avg prep (min)`,
                       rejection_rate as `Rejected`, rating_at_first_order as `Rating then`,
                       current_rating as `Rating now`
                {ssrc} order by `GMV` desc""",
            "table",
            frs,
            {"column_settings": money("GMV") | pct("Rejected")},
        ),
    ]
    riders = b.dashboard(
        "Riders & Restaurants",
        "Rider throughput and punctuality, kitchen speed and restaurant ratings over time. Synthetic data.",
        ["date", "city", "cuisine"],
        [
            (people[0], 0, 0, 12, 7), (people[1], 0, 12, 12, 7),
            (people[2], 7, 0, 12, 8), (people[3], 7, 12, 12, 8),
            (people[4], 15, 0, 24, 8), (people[5], 23, 0, 24, 8),
        ],
    )  # fmt: skip

    # ---------------------------------------------------------------- cdc health
    h = "from gold.cdc_table_health"
    cdc_cards = [
        b.card("Change events", f"select sum(change_events) as events {h}", "scalar", {}),
        b.card("Updates", f"select sum(updates) as update_count {h}", "scalar", {}),
        b.card("Deletes", f"select sum(deletes) as delete_count {h}", "scalar", {}),
        b.card(
            "Worst p95 lag (s)",
            f"select max(p95_lag_seconds) as lag_s {h}",
            "scalar",
            {},
            {"column_settings": dec("lag_s")},
        ),
        b.card(
            "Anonymised",
            "select countIf(is_anonymised) as anonymised from gold.fct_orders",
            "scalar",
            {},
        ),
        b.card(
            "With history",
            """select count() as with_history from (select customer_id from gold.dim_customers_history
               group by customer_id having count() > 1)""",
            "scalar",
            {},
        ),
        b.card(
            "Change events by table",
            f"""select source_table as tbl, snapshot_events as `Snapshot`, inserts as `Inserts`,
                       updates as `Updates`, deletes as `Deletes` {h} order by change_events desc""",
            "row",
            {},
            chart(
                "tbl",
                ["Snapshot", "Inserts", "Updates", "Deletes"],
                [BLUE, ORANGE, AQUA, "#eda100"],
                **{"stackable.stack_type": "stacked"},
                **NO_OTHER,
            ),
        ),
        b.card(
            "Replication lag by table (seconds)",
            f"select source_table as tbl, p50_lag_seconds as `p50`, p95_lag_seconds as `p95` {h} order by tbl",
            "bar",
            {},
            chart("tbl", ["p50", "p95"], [BLUE, ORANGE], "Table", "Seconds"),
        ),
        b.card(
            "CDC table health",
            f"""select source_table as `Table`, current_rows as `Current rows`, snapshot_events as `Snapshot`,
                       inserts as `Inserts`, updates as `Updates`, deletes as `Deletes`,
                       p50_lag_seconds as `p50 lag (s)`, p95_lag_seconds as `p95 lag (s)`,
                       last_change_at as `Last change` {h} order by change_events desc""",
            "table",
            {},
        ),
    ]
    health = b.dashboard(
        "CDC Health",
        "What Debezium captured from Postgres and how fast it reached ClickHouse.",
        [],
        [
            (cdc_cards[0], 0, 0, 4, 3), (cdc_cards[1], 0, 4, 4, 3), (cdc_cards[2], 0, 8, 4, 3),
            (cdc_cards[4], 0, 12, 4, 3), (cdc_cards[5], 0, 16, 4, 3), (cdc_cards[3], 0, 20, 4, 3),
            (cdc_cards[6], 3, 0, 12, 6), (cdc_cards[7], 3, 12, 12, 6),
            (cdc_cards[8], 9, 0, 24, 6),
        ],
    )  # fmt: skip
    return [operations, riders, health]


def main() -> None:
    env = _env()
    mb = connect(env)
    mb.put("/setting/enable-public-sharing", {"value": True})
    db_id = ensure_database(mb, env)
    fields = field_ids(mb, db_id, {"fct_orders", "agg_rider_daily", "agg_restaurant_performance"})
    builder = Builder(mb, db_id, fields, reset_collection(mb))
    dashboards = build(builder)
    (HERE / "dashboards.json").write_text(json.dumps(dashboards, indent=2) + "\n")
    for dash in dashboards:
        print(f"{dash['name']:<24} {dash['public_url']}")


if __name__ == "__main__":
    main()

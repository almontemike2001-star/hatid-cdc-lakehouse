"""Single source of truth for the captured tables and their ClickHouse bronze layer.

For every source table the bronze layer has:

* ``kafka_<t>``    Kafka engine table reading the Debezium topic as raw JSON strings
* ``<t>_changelog`` append-only MergeTree: every change (insert/update/delete/snapshot), typed
                    columns plus the raw JSON, so history can be replayed or re-parsed
* ``<t>``           ReplacingMergeTree(_lsn, _deleted): current state; with FINAL, the latest
                    version per key wins and deleted keys disappear
* two materialized views wiring them: kafka -> changelog -> current
"""

from __future__ import annotations

from dataclasses import dataclass

BRONZE = "bronze"
TOPIC_PREFIX = "hatid.app"


@dataclass(frozen=True)
class Column:
    name: str
    ch_type: str  # ClickHouse type in the bronze tables

    @property
    def extract(self) -> str:
        """Expression that pulls this column out of the Debezium JSON in ``raw``."""
        base = self.ch_type.removeprefix("Nullable(").removesuffix(")")
        js = f"JSONExtract(raw, '{self.name}', 'Nullable(String)')"
        if base.startswith("Decimal"):
            scale = base.split(",")[1].strip(" )")
            expr = f"toDecimal64OrNull({js}, {scale})"
        elif base.startswith("DateTime64"):
            expr = f"parseDateTime64BestEffortOrNull({js}, 3, 'UTC')"
        else:
            expr = f"JSONExtract(raw, '{self.name}', 'Nullable({base})')"
        if not self.ch_type.startswith("Nullable("):
            expr = f"assumeNotNull({expr})"
        return expr


@dataclass(frozen=True)
class Table:
    name: str
    key: str
    columns: tuple[Column, ...]

    @property
    def topic(self) -> str:
        return f"{TOPIC_PREFIX}.{self.name}"


def C(name: str, ch_type: str) -> Column:  # noqa: N802 - terse table definitions below
    return Column(name, ch_type)


TS = "DateTime64(3, 'UTC')"
NTS = f"Nullable({TS})"

TABLES: tuple[Table, ...] = (
    Table("customers", "customer_id", (
        C("customer_id", "Int64"), C("full_name", "String"), C("email", "String"),
        C("city", "String"), C("created_at", TS), C("updated_at", TS),
    )),
    Table("restaurants", "restaurant_id", (
        C("restaurant_id", "Int64"), C("name", "String"), C("cuisine", "String"),
        C("city", "String"), C("rating", "Decimal(2, 1)"), C("is_active", "Bool"),
        C("created_at", TS), C("updated_at", TS),
    )),
    Table("riders", "rider_id", (
        C("rider_id", "Int64"), C("full_name", "String"), C("city", "String"),
        C("vehicle", "String"), C("is_active", "Bool"), C("created_at", TS), C("updated_at", TS),
    )),
    Table("orders", "order_id", (
        C("order_id", "Int64"), C("customer_id", "Nullable(Int64)"), C("restaurant_id", "Int64"),
        C("status", "String"), C("subtotal", "Decimal(10, 2)"), C("delivery_fee", "Decimal(10, 2)"),
        C("placed_at", TS), C("accepted_at", NTS), C("ready_at", NTS), C("cancelled_at", NTS),
        C("cancel_reason", "Nullable(String)"), C("updated_at", TS),
    )),
    Table("deliveries", "delivery_id", (
        C("delivery_id", "Int64"), C("order_id", "Int64"), C("rider_id", "Int64"),
        C("status", "String"), C("distance_km", "Decimal(5, 2)"), C("promised_at", TS),
        C("assigned_at", TS), C("picked_up_at", NTS), C("delivered_at", NTS), C("updated_at", TS),
    )),
)  # fmt: skip

# Additive schema change applied while the pipeline is running (see `hatid migrate`).
TIP_COLUMN = C("tip_amount", "Nullable(Decimal(10, 2))")

META_COLUMNS = (
    ("_op", "LowCardinality(String)", "JSONExtractString(raw, '__op')"),
    ("_lsn", "UInt64", "JSONExtractUInt(raw, '__lsn')"),
    ("_source_ts", TS, "fromUnixTimestamp64Milli(JSONExtractInt(raw, '__source_ts_ms'), 'UTC')"),
    ("_deleted", "UInt8", "JSONExtractString(raw, '__deleted') = 'true'"),
)


def table(name: str) -> Table:
    return next(t for t in TABLES if t.name == name)


def with_column(t: Table, column: Column) -> Table:
    return Table(t.name, t.key, (*t.columns, column))


def _changelog_select(t: Table) -> str:
    cols = [f"{c.extract} AS {c.name}" for c in t.columns]
    cols += [f"{expr} AS {name}" for name, _, expr in META_COLUMNS]
    cols += ["now64(3, 'UTC') AS _ingested_at", "raw AS _raw"]
    return "SELECT\n    " + ",\n    ".join(cols) + f"\nFROM {BRONZE}.kafka_{t.name}"


def changelog_view_sql(t: Table) -> str:
    return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {BRONZE}.mv_{t.name}_changelog\n" \
           f"TO {BRONZE}.{t.name}_changelog AS\n{_changelog_select(t)}"  # fmt: skip


def ddl(t: Table, broker: str) -> list[str]:
    typed = ",\n    ".join(f"{c.name} {c.ch_type}" for c in t.columns)
    meta = ",\n    ".join(f"{name} {typ}" for name, typ, _ in META_COLUMNS)
    all_cols = ", ".join([c.name for c in t.columns] + [m[0] for m in META_COLUMNS])
    return [
        f"""CREATE TABLE IF NOT EXISTS {BRONZE}.kafka_{t.name} (raw String)
ENGINE = Kafka SETTINGS
    kafka_broker_list = '{broker}',
    kafka_topic_list = '{t.topic}',
    kafka_group_name = 'clickhouse_{t.name}',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0,
    kafka_flush_interval_ms = 1000""",
        f"""CREATE TABLE IF NOT EXISTS {BRONZE}.{t.name}_changelog (
    {typed},
    {meta},
    _ingested_at {TS},
    _raw String CODEC(ZSTD(3))
) ENGINE = MergeTree
ORDER BY ({t.key}, _lsn)""",
        f"""CREATE TABLE IF NOT EXISTS {BRONZE}.{t.name} (
    {typed},
    {meta},
    _ingested_at {TS}
) ENGINE = ReplacingMergeTree(_lsn, _deleted)
ORDER BY {t.key}""",
        changelog_view_sql(t),
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {BRONZE}.mv_{t.name}_current
TO {BRONZE}.{t.name} AS
SELECT {all_cols}, _ingested_at FROM {BRONZE}.{t.name}_changelog""",
    ]


def bootstrap_sql(broker: str) -> list[str]:
    statements = [f"CREATE DATABASE IF NOT EXISTS {BRONZE}"]
    for t in TABLES:
        statements += ddl(t, broker)
    return statements


def add_column_sql(t: Table, column: Column) -> list[str]:
    """ClickHouse side of an additive source change. The Kafka consumer pauses while its view
    is being replaced (offsets are only committed by an attached view), so nothing is lost;
    rows captured before the change keep NULL, and the raw JSON allows a later re-parse."""
    evolved = with_column(t, column)
    return [
        f"ALTER TABLE {BRONZE}.{t.name}_changelog ADD COLUMN IF NOT EXISTS "
        f"{column.name} {column.ch_type} AFTER updated_at",
        f"ALTER TABLE {BRONZE}.{t.name} ADD COLUMN IF NOT EXISTS "
        f"{column.name} {column.ch_type} AFTER updated_at",
        # Stop consuming first, rewire changelog -> current, resume consuming last.
        f"DROP VIEW IF EXISTS {BRONZE}.mv_{t.name}_changelog",
        f"DROP VIEW IF EXISTS {BRONZE}.mv_{t.name}_current",
        f"""CREATE MATERIALIZED VIEW {BRONZE}.mv_{t.name}_current TO {BRONZE}.{t.name} AS
SELECT {", ".join([c.name for c in evolved.columns] + [m[0] for m in META_COLUMNS])}, _ingested_at
FROM {BRONZE}.{t.name}_changelog""",
        changelog_view_sql(evolved),
    ]

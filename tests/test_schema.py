from hatid import schema


def test_every_table_gets_kafka_changelog_current_and_two_views():
    sql = schema.bootstrap_sql("redpanda:9092")
    for t in schema.TABLES:
        assert any(f"kafka_{t.name} (raw String)" in s and t.topic in s for s in sql)
        assert any(f"{t.name}_changelog (" in s and "ENGINE = MergeTree" in s for s in sql)
        assert any(f"bronze.{t.name} (" in s and "ReplacingMergeTree(_lsn, _deleted)" in s for s in sql)
        assert any(f"mv_{t.name}_changelog" in s for s in sql)
        assert any(f"mv_{t.name}_current" in s for s in sql)


def test_extract_expressions_follow_the_column_type():
    assert schema.C("x", "Int64").extract.startswith("assumeNotNull(JSONExtract(raw, 'x', 'Nullable(Int64)')")
    assert "toDecimal64OrNull" in schema.C("x", "Decimal(10, 2)").extract
    assert ", 2)" in schema.C("x", "Decimal(10, 2)").extract
    ts = schema.C("x", "Nullable(DateTime64(3, 'UTC'))").extract
    assert ts.startswith("parseDateTime64BestEffortOrNull") and "assumeNotNull" not in ts


def test_schema_change_stops_consumer_first_and_resumes_it_last():
    steps = schema.add_column_sql(schema.table("orders"), schema.TIP_COLUMN)
    assert steps[0].startswith("ALTER TABLE bronze.orders_changelog ADD COLUMN")
    drop_consumer = steps.index("DROP VIEW IF EXISTS bronze.mv_orders_changelog")
    recreate_consumer = next(i for i, s in enumerate(steps) if "mv_orders_changelog" in s and "CREATE" in s)
    recreate_current = next(i for i, s in enumerate(steps) if "mv_orders_current" in s and "CREATE" in s)
    assert drop_consumer < recreate_current < recreate_consumer == len(steps) - 1
    assert "tip_amount" in steps[-1]

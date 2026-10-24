-- Per captured table: current rows, change events by operation and replication lag
-- (Postgres commit -> ClickHouse ingest; snapshot rows excluded from lag).
{% set tables = ['customers', 'restaurants', 'riders', 'orders', 'deliveries'] %}
{% for t in tables %}
select
    '{{ t }}' as source_table,
    (select count() from {{ source('bronze', t) }} final where _deleted = 0) as current_rows,
    count() as change_events,
    countIf(_op = 'r') as snapshot_events,
    countIf(_op = 'c') as inserts,
    countIf(_op = 'u') as updates,
    countIf(_op = 'd') as deletes,
    max(_source_ts) as last_change_at,
    round(quantileIf(0.5)(dateDiff('millisecond', _source_ts, _ingested_at), _op != 'r') / 1000, 2) as p50_lag_seconds,
    round(quantileIf(0.95)(dateDiff('millisecond', _source_ts, _ingested_at), _op != 'r') / 1000, 2) as p95_lag_seconds
from {{ source('bronze', t ~ '_changelog') }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
